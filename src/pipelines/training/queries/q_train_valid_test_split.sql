/* Use older data for training. Use APPROX_QUANTILES otherwise BQ runs out of memory.*/
DECLARE train_limit INT64 DEFAULT (
    SELECT APPROX_QUANTILES(datetime_unix_seconds, 100)[OFFSET(CAST(100 * (1 - {{ valid_size }} - {{ test_size }}) AS INT))] train_limit
    FROM `{{ source_table }}`
)
;

{% if create_replace_table is sameas true %}
CREATE OR REPLACE TABLE
{% else %}
CREATE TABLE IF NOT EXISTS
{% endif %}
`{{ training_table }}` AS (
    SELECT t.* EXCEPT(datetime_unix_seconds)

    FROM `{{ source_table }}` t

    WHERE t.datetime_unix_seconds < train_limit
)
;

/* Randomly split newer data into test and validation sets. */
CREATE TEMP TABLE validation_testing AS (
    SELECT t.* EXCEPT(datetime_unix_seconds)

    FROM `{{ source_table }}` t

    WHERE t.datetime_unix_seconds >= train_limit
)
;

{% if create_replace_table is sameas true %}
CREATE OR REPLACE TABLE
{% else %}
CREATE TABLE IF NOT EXISTS
{% endif %}
`{{ validation_table }}` AS (
    SELECT t.*

    FROM validation_testing t

    WHERE ABS(MOD(t.transaction_id, 100)) < CAST({{ valid_size }} / ({{ valid_size }} + {{ test_size }}) * 100 AS INT)
)
;

{% if create_replace_table is sameas true %}
CREATE OR REPLACE TABLE
{% else %}
CREATE TABLE IF NOT EXISTS
{% endif %}
`{{ testing_table }}` AS (
    SELECT t.*

    FROM validation_testing t

    WHERE ABS(MOD(t.transaction_id, 100)) >= CAST({{ valid_size }} / ({{ valid_size }} + {{ test_size }}) * 100 AS INT)
)
;
