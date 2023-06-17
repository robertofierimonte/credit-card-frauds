CREATE TEMP TABLE merged
CLUSTER BY mcc, datetime_unix_seconds
AS (
    WITH transactions AS (
        /* Features related to transactions */
        SELECT
            ROW_NUMBER() OVER() AS transaction_id,
            DATETIME(t.year, t.month, t.day, CAST(SUBSTR(t.time, 1, 2) AS INT), CAST(SUBSTR(t.time, 4, 2) AS INT), 0) AS datetime,
            t.year,
            SIN((CAST(SUBSTR(t.time, 1, 2) AS INT) / 24) * 2 * ACOS(-1)) AS hour_sin,
            COS((CAST(SUBSTR(t.time, 1, 2) AS INT) / 24) * 2 * ACOS(-1)) AS hour_cos,
            SIN(((t.month - 1) / 12) * 2 * ACOS(-1)) AS month_sin,
            COS(((t.month - 1) / 12) * 2 * ACOS(-1)) AS month_cos,
            IF(t.year >= 2015, 1, 0) AS is_2015_or_later,
            t.amount,
            IF(t.use_chip = "Chip Transaction", 1, 0) AS chip_transaction,
            IF(t.use_chip = "Swipe Transaction", 1, 0) AS swipe_transaction,
            IF(t.use_chip = "Online Transaction", 1, 0) AS online_transaction,
            IF(t.use_chip = "Chip Transaction" OR t.use_chip = "Swipe Transaction", 1, 0) AS card_present_transaction,
            t.card,
            t.user,
            t.mcc,
            CAST(t.Is_Fraud_ AS INT) AS is_fraud

        FROM `{{ transactions_table }}` t
    )

    , users AS (
        /* Features related to users */
        SELECT
            u.user,
            IF(u.gender = "Male", 0, 1) AS gender,
            CAST(REPLACE(u.`Per Capita Income - Zipcode`, "$", "") AS NUMERIC) AS per_capita_income_zipcode,
            CAST(REPLACE(u.`Yearly Income - Person`, "$", "") AS NUMERIC) AS yearly_income_person,
            CAST(REPLACE(u.`Total Debt`, "$", "") AS NUMERIC) AS total_debt,

        FROM `{{ users_table }}` u
    )

    , cards AS (
        /* Features related to cards */
        SELECT
            user,
            card_index,
            IF(card_brand = "Amex", 1, 0) AS amex,
            IF(card_brand = "Discover", 1, 0) AS discover,
            IF(card_brand = "Mastercard", 1, 0) AS mastercard,
            IF(card_brand = "Visa", 1, 0) AS visa,
            IF(card_type = "Credit", 1, 0) AS credit,
            IF(card_type = "Debit", 1, 0) AS debit,
            IF(card_type = "Debit (Prepaid)", 1, 0) AS debit_prepaid,
            credit_limit,
            IF(has_chip IS TRUE, 1, 0) AS has_chip,
            IF(card_on_dark_web IS TRUE, 1, 0) AS card_on_dark_web,

        FROM `{{ cards_table }}`
    )

    /* Merging all features together */
    SELECT
        t.transaction_id,
        t.datetime,
        t.year,
        UNIX_SECONDS(CAST(t.datetime AS TIMESTAMP)) AS datetime_unix_seconds,
        t.hour_sin,
        t.hour_cos,
        t.month_sin,
        t.month_cos,
        EXTRACT(DAYOFWEEK FROM t.datetime) AS day_of_week,
        IF(EXTRACT(DAYOFWEEK FROM t.datetime) IN (1, 7), 1, 0) AS weekend,
        IF(EXTRACT(DATE FROM t.datetime) IN (SELECT date FROM `{{ holidays_table }}`), 1, 0) AS is_holiday,
        SIN((IF(EXTRACT(DAYOFWEEK FROM t.datetime) = 1, 6, EXTRACT(DAYOFWEEK FROM t.datetime) - 2) / 7) * 2 * ACOS(-1)) AS day_of_week_sin,
        COS((IF(EXTRACT(DAYOFWEEK FROM t.datetime) = 1, 6, EXTRACT(DAYOFWEEK FROM t.datetime) - 2) / 7) * 2 * ACOS(-1)) AS day_of_week_cos,
        t.is_2015_or_later,
        t.amount,
        t.swipe_transaction,
        t.chip_transaction,
        t.online_transaction,
        t.card_present_transaction,
        IF(t.is_fraud = 1 AND t.chip_transaction = 1, 1, 0) AS fraud_chip,
        IF(t.is_fraud = 1 AND t.swipe_transaction = 1, 1, 0) AS fraud_swipe,
        IF(t.is_fraud = 1 AND t.online_transaction = 1, 1, 0) AS fraud_online,
        IF(t.is_fraud = 1 AND t.card_present_transaction = 1, 1, 0) AS fraud_card_present,
        t.user,
        t.mcc,
        u.gender,
        c.amex,
        c.discover,
        c.mastercard,
        c.visa,
        c.credit,
        c.debit,
        c.debit_prepaid,
        c.credit_limit,
        c.has_chip,
        c.card_on_dark_web,
        u.per_capita_income_zipcode,
        u.yearly_income_person,
        u.total_debt,
        t.is_fraud,

    FROM transactions t

    INNER JOIN cards c
        ON t.card = c.card_index
        AND t.user = c.user

    INNER JOIN users u
        ON t.user = u.user
)
;

CREATE TEMP TABLE mcc_aux AS (
    /* Rolling features related to MCC */
    SELECT
        m.transaction_id,
        COALESCE(SUM(m.is_fraud) OVER(mcc_window) / COUNT(m.is_fraud) OVER(mcc_window), 0) AS mcc_mean_encoding

    FROM merged m
    WINDOW mcc_window AS (PARTITION BY m.mcc ORDER BY m.datetime_unix_seconds RANGE BETWEEN UNBOUNDED PRECEDING AND {{ fraud_delay_seconds }} PRECEDING)
)
;

CREATE TEMP TABLE user_aux
CLUSTER BY user, datetime_unix_seconds
/* Rolling features related to users */
AS (
    SELECT
        m.*,
        COALESCE(AVG(m.amount) OVER(user_window), 0) AS mean_amount,
        COUNT(m.transaction_id) OVER(user_window) AS transaction_count,
        DATE_DIFF(m.datetime, MIN(m.datetime) OVER(PARTITION BY m.user), DAY) AS days_since_first_transaction,

    FROM merged m
    WINDOW user_window AS (PARTITION BY m.user ORDER BY m.datetime ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
)
;

CREATE TEMP TABLE rolling_aux_amount_frequency AS (
    /* Rolling features related to transaction amount and frquency */
    WITH tmp AS (
        SELECT
            r.*,
            COALESCE(AVG(r.amount) OVER(PARTITION BY r.user ORDER BY r.datetime_unix_seconds RANGE BETWEEN 86400 PRECEDING AND 1 PRECEDING), 0) AS mean_amount_last_1_days,
            COALESCE(AVG(r.amount) OVER(PARTITION BY r.user ORDER BY r.datetime_unix_seconds RANGE BETWEEN 172800 PRECEDING AND 1 PRECEDING), 0) AS mean_amount_last_2_days,
            COALESCE(AVG(r.amount) OVER(PARTITION BY r.user ORDER BY r.datetime_unix_seconds RANGE BETWEEN 604800 PRECEDING AND 1 PRECEDING), 0) AS mean_amount_last_7_days,
            COALESCE(AVG(r.amount) OVER(PARTITION BY r.user ORDER BY r.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING), 0) AS mean_amount_last_30_days,
            COALESCE(AVG(r.amount) OVER(PARTITION BY r.user ORDER BY r.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND 1 PRECEDING), 0) AS mean_amount_last_year,
            IF(
                r.days_since_first_transaction > 0,
                (r.transaction_count - 1) / r.days_since_first_transaction,
                0
            ) AS transaction_frequency_all,
            IF(
                r.days_since_first_transaction > 0,
                COUNT(r.transaction_id) OVER(PARTITION BY r.user ORDER BY r.datetime_unix_seconds RANGE BETWEEN 86400 PRECEDING AND 1 PRECEDING)
                    / LEAST(1, r.days_since_first_transaction),
                0
            ) AS transaction_frequency_last_1_days,
            IF(
                r.days_since_first_transaction > 0,
                COUNT(r.transaction_id) OVER(PARTITION BY r.user ORDER BY r.datetime_unix_seconds RANGE BETWEEN 172800 PRECEDING AND 1 PRECEDING)
                    / LEAST(2, r.days_since_first_transaction),
                0
            ) AS transaction_frequency_last_2_days,
            IF(
                r.days_since_first_transaction > 0,
                COUNT(r.transaction_id) OVER(PARTITION BY r.user ORDER BY r.datetime_unix_seconds RANGE BETWEEN 604800 PRECEDING AND 1 PRECEDING)
                    / LEAST(7, r.days_since_first_transaction),
                0
            ) AS transaction_frequency_last_7_days,
            IF(
                r.days_since_first_transaction > 0,
                COUNT(r.transaction_id) OVER(PARTITION BY r.user ORDER BY r.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING)
                    / LEAST(30, r.days_since_first_transaction),
                0
            ) AS transaction_frequency_last_30_days,
            IF(
                r.days_since_first_transaction > 0,
                COUNT(r.transaction_id) OVER(PARTITION BY r.user ORDER BY r.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND 1 PRECEDING)
                    / LEAST(365, r.days_since_first_transaction),
                0
            ) AS transaction_frequency_last_year

        FROM user_aux r
    )
    SELECT
        t.transaction_id,
        t.mean_amount,
        t.transaction_count,
        days_since_first_transaction,
        mean_amount_last_year,
        mean_amount_last_30_days,
        mean_amount_last_7_days,
        mean_amount_last_2_days,
        mean_amount_last_1_days,
        transaction_frequency_all,
        transaction_frequency_last_year,
        transaction_frequency_last_30_days,
        transaction_frequency_last_7_days,
        transaction_frequency_last_2_days,
        transaction_frequency_last_1_days,

        COALESCE(SAFE_DIVIDE(mean_amount_last_7_days, mean_amount_last_year), 0) AS mean_amount_last_7_days_relative_to_last_year,
        COALESCE(SAFE_DIVIDE(mean_amount_last_2_days, mean_amount_last_year), 0) AS mean_amount_last_2_days_relative_to_last_year,
        COALESCE(SAFE_DIVIDE(mean_amount_last_1_days, mean_amount_last_year), 0) AS mean_amount_last_1_days_relative_to_last_year,

        COALESCE(SAFE_DIVIDE(mean_amount_last_7_days, mean_amount_last_30_days), 0) AS mean_amount_last_7_days_relative_to_last_30_days,
        COALESCE(SAFE_DIVIDE(mean_amount_last_2_days, mean_amount_last_30_days), 0) AS mean_amount_last_2_days_relative_to_last_30_days,
        COALESCE(SAFE_DIVIDE(mean_amount_last_1_days, mean_amount_last_30_days), 0) AS mean_amount_last_1_days_relative_to_last_30_days,

        COALESCE(SAFE_DIVIDE(transaction_frequency_last_7_days, transaction_frequency_last_year), 0) AS `7_days_transaction_frequency_relative_to_last_year`,
        COALESCE(SAFE_DIVIDE(transaction_frequency_last_2_days, transaction_frequency_last_year), 0) AS `2_days_transaction_frequency_relative_to_last_year`,
        COALESCE(SAFE_DIVIDE(transaction_frequency_last_1_days, transaction_frequency_last_year), 0) AS `1_days_transaction_frequency_relative_to_last_year`,

        COALESCE(SAFE_DIVIDE(transaction_frequency_last_7_days, transaction_frequency_last_30_days), 0) AS `7_days_transaction_frequency_relative_to_last_30_days`,
        COALESCE(SAFE_DIVIDE(transaction_frequency_last_2_days, transaction_frequency_last_30_days), 0) AS `2_days_transaction_frequency_relative_to_last_30_days`,
        COALESCE(SAFE_DIVIDE(transaction_frequency_last_1_days, transaction_frequency_last_30_days), 0) AS `1_days_transaction_frequency_relative_to_last_30_days`,

    FROM tmp t
)
;

CREATE TEMP TABLE rolling_aux_frauds
/* Rolling features related to amount of frauds */
AS (
    WITH tmp1 AS (
        SELECT
            m.transaction_id,
            m.year,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_2_years,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_365_days,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_60_days,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_30_days,

        FROM merged m

        WHERE m.year <= 2000
    )

    , tmp2 AS (
        SELECT
            m.transaction_id,
            m.year,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_2_years,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_365_days,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_60_days,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_30_days,

        FROM merged m

        WHERE m.year BETWEEN 1998 AND 2010
    )

    , tmp3 AS (
        SELECT
            m.transaction_id,
            m.year,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_2_years,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_365_days,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_60_days,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_30_days,

        FROM merged m

        WHERE m.year BETWEEN 2008 AND 2015
    )

    , tmp4 AS (
        SELECT
            m.transaction_id,
            m.year,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_2_years,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_365_days,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_60_days,
            COALESCE(AVG(m.is_fraud) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_swipe) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_swipe_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_chip) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_chip_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_online) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_online_rolling_mean_30_days,

            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 63072000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_2_years,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 31536000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_365_days,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 5184000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_60_days,
            COALESCE(AVG(m.fraud_card_present) OVER (ORDER BY m.datetime_unix_seconds RANGE BETWEEN 2592000 PRECEDING AND {{ fraud_delay_seconds }} PRECEDING), 0) AS fraud_card_present_rolling_mean_30_days,

        FROM merged m

        WHERE m.year >= 2013
    )

    , all_years AS (
        SELECT * EXCEPT(year) FROM tmp1

        UNION ALL

        SELECT * EXCEPT(year) FROM tmp2 WHERE year BETWEEN 2001 AND 2010

        UNION ALL

        SELECT * EXCEPT(year) FROM tmp3 WHERE year BETWEEN 2011 AND 2015

        UNION ALL

        SELECT * EXCEPT(year) FROM tmp4 WHERE year >= 2016
    )

    SELECT
        a.*,
        COALESCE(SAFE_DIVIDE(fraud_rolling_mean_30_days, fraud_rolling_mean_365_days), 0) AS fraud_rolling_30_days_relative_to_365_days,
        COALESCE(SAFE_DIVIDE(fraud_rolling_mean_30_days, fraud_rolling_mean_2_years), 0) AS fraud_rolling_30_days_relative_to_2_years,
        COALESCE(SAFE_DIVIDE(fraud_rolling_mean_60_days, fraud_rolling_mean_365_days), 0) AS fraud_rolling_60_days_relative_to_365_days,
        COALESCE(SAFE_DIVIDE(fraud_rolling_mean_60_days, fraud_rolling_mean_2_years), 0) AS fraud_rolling_60_days_relative_to_2_years,

        COALESCE(SAFE_DIVIDE(fraud_swipe_rolling_mean_30_days, fraud_rolling_mean_365_days), 0) AS fraud_swipe_rolling_30_days_relative_to_365_days,
        COALESCE(SAFE_DIVIDE(fraud_swipe_rolling_mean_30_days, fraud_rolling_mean_2_years), 0) AS fraud_swipe_rolling_30_days_relative_to_2_years,
        COALESCE(SAFE_DIVIDE(fraud_swipe_rolling_mean_60_days, fraud_rolling_mean_365_days), 0) AS fraud_swipe_rolling_60_days_relative_to_365_days,
        COALESCE(SAFE_DIVIDE(fraud_swipe_rolling_mean_60_days, fraud_rolling_mean_2_years), 0) AS fraud_swipe_rolling_60_days_relative_to_2_years,

        COALESCE(SAFE_DIVIDE(fraud_chip_rolling_mean_30_days, fraud_rolling_mean_365_days), 0) AS fraud_chip_rolling_30_days_relative_to_365_days,
        COALESCE(SAFE_DIVIDE(fraud_chip_rolling_mean_30_days, fraud_rolling_mean_2_years), 0) AS fraud_chip_rolling_30_days_relative_to_2_years,
        COALESCE(SAFE_DIVIDE(fraud_chip_rolling_mean_60_days, fraud_rolling_mean_365_days), 0) AS fraud_chip_rolling_60_days_relative_to_365_days,
        COALESCE(SAFE_DIVIDE(fraud_chip_rolling_mean_60_days, fraud_rolling_mean_2_years), 0) AS fraud_chip_rolling_60_days_relative_to_2_years,

        COALESCE(SAFE_DIVIDE(fraud_online_rolling_mean_30_days, fraud_rolling_mean_365_days), 0) AS fraud_online_rolling_30_days_relative_to_365_days,
        COALESCE(SAFE_DIVIDE(fraud_online_rolling_mean_30_days, fraud_rolling_mean_2_years), 0) AS fraud_online_rolling_30_days_relative_to_2_years,
        COALESCE(SAFE_DIVIDE(fraud_online_rolling_mean_60_days, fraud_rolling_mean_365_days), 0) AS fraud_online_rolling_60_days_relative_to_365_days,
        COALESCE(SAFE_DIVIDE(fraud_online_rolling_mean_60_days, fraud_rolling_mean_2_years), 0) AS fraud_online_rolling_60_days_relative_to_2_years,

        COALESCE(SAFE_DIVIDE(fraud_card_present_rolling_mean_30_days, fraud_rolling_mean_365_days), 0) AS fraud_card_present_rolling_30_days_relative_to_365_days,
        COALESCE(SAFE_DIVIDE(fraud_card_present_rolling_mean_30_days, fraud_rolling_mean_2_years), 0) AS fraud_card_present_rolling_30_days_relative_to_2_years,
        COALESCE(SAFE_DIVIDE(fraud_card_present_rolling_mean_60_days, fraud_rolling_mean_365_days), 0) AS fraud_card_present_rolling_60_days_relative_to_365_days,
        COALESCE(SAFE_DIVIDE(fraud_card_present_rolling_mean_60_days, fraud_rolling_mean_2_years), 0) AS fraud_card_present_rolling_60_days_relative_to_2_years,

        COALESCE(SAFE_DIVIDE(fraud_swipe_rolling_mean_30_days, fraud_rolling_mean_30_days), 0) AS fraud_swipe_rolling_30_days_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_chip_rolling_mean_30_days, fraud_rolling_mean_30_days), 0) AS fraud_chip_rolling_30_days_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_online_rolling_mean_30_days, fraud_rolling_mean_30_days), 0) AS fraud_online_rolling_30_days_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_card_present_rolling_mean_30_days, fraud_rolling_mean_30_days), 0) AS fraud_card_present_rolling_30_days_relative_to_all_frauds,

        COALESCE(SAFE_DIVIDE(fraud_swipe_rolling_mean_60_days, fraud_rolling_mean_60_days), 0) AS fraud_swipe_rolling_60_days_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_chip_rolling_mean_60_days, fraud_rolling_mean_60_days), 0) AS fraud_chip_rolling_60_days_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_online_rolling_mean_60_days, fraud_rolling_mean_60_days), 0) AS fraud_online_rolling_60_days_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_card_present_rolling_mean_60_days, fraud_rolling_mean_60_days), 0) AS fraud_card_present_rolling_60_days_relative_to_all_frauds,

        COALESCE(SAFE_DIVIDE(fraud_swipe_rolling_mean_365_days, fraud_rolling_mean_365_days), 0) AS fraud_swipe_rolling_365_days_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_chip_rolling_mean_365_days, fraud_rolling_mean_365_days), 0) AS fraud_chip_rolling_365_days_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_online_rolling_mean_365_days, fraud_rolling_mean_365_days), 0) AS fraud_online_rolling_365_days_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_card_present_rolling_mean_365_days, fraud_rolling_mean_365_days), 0) AS fraud_card_present_rolling_365_days_relative_to_all_frauds,

        COALESCE(SAFE_DIVIDE(fraud_swipe_rolling_mean_2_years, fraud_rolling_mean_2_years), 0) AS fraud_swipe_rolling_2_years_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_chip_rolling_mean_2_years, fraud_rolling_mean_2_years), 0) AS fraud_chip_rolling_2_years_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_online_rolling_mean_2_years, fraud_rolling_mean_2_years), 0) AS fraud_online_rolling_2_years_relative_to_all_frauds,
        COALESCE(SAFE_DIVIDE(fraud_card_present_rolling_mean_2_years, fraud_rolling_mean_2_years), 0) AS fraud_card_present_rolling_2_years_relative_to_all_frauds,

    FROM all_years a
)
;

CREATE OR REPLACE TABLE `{{ preprocessed_table }}`
CLUSTER BY datetime_unix_seconds
AS (
    SELECT
        m.transaction_id,
        m.datetime_unix_seconds,
        {{ features }},
        is_fraud

    FROM merged m

    INNER JOIN mcc_aux mc
        ON m.transaction_id = mc.transaction_id

    INNER JOIN rolling_aux_amount_frequency raf
        ON m.transaction_id = raf.transaction_id

    INNER JOIN rolling_aux_frauds rf
        ON m.transaction_id = rf.transaction_id
)
;
