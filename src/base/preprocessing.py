import pandas as pd
from loguru import logger


def rolling_feature_engineering(
    data: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate rolling features over the dataset.

    Args:
        data (pd.DataFrame): Dataframe over which to compute the rolling features.

    Returns:
        pd.DataFrame: Dataframe with rolling features.
    """
    data = data.sort_values("datetime_offset")
    data = data.set_index("datetime_offset")

    # Calculate rolling averages for different fraud types
    data["fraud_rolling_mean_30_days"] = (
        data["is_fraud"].rolling("30d", closed="left").mean()
    )
    data["fraud_rolling_mean_60_days"] = (
        data["is_fraud"].rolling("60d", closed="left").mean()
    )
    data["fraud_rolling_mean_365_days"] = (
        data["is_fraud"].rolling("365d", closed="left").mean()
    )
    data["fraud_rolling_mean_2_years"] = (
        data["is_fraud"].rolling("730d", closed="left").mean()
    )

    data["fraud_swipe_rolling_mean_30_days"] = (
        data["fraud_swipe"].rolling("30d", closed="left").mean()
    )
    data["fraud_swipe_rolling_mean_60_days"] = (
        data["fraud_swipe"].rolling("60d", closed="left").mean()
    )
    data["fraud_swipe_rolling_mean_365_days"] = (
        data["fraud_swipe"].rolling("365d", closed="left").mean()
    )
    data["fraud_swipe_rolling_mean_2_years"] = (
        data["fraud_swipe"].rolling("730d", closed="left").mean()
    )

    data["fraud_chip_rolling_mean_30_days"] = (
        data["fraud_chip"].rolling("30d", closed="left").mean()
    )
    data["fraud_chip_rolling_mean_60_days"] = (
        data["fraud_chip"].rolling("60d", closed="left").mean()
    )
    data["fraud_chip_rolling_mean_365_days"] = (
        data["fraud_chip"].rolling("365d", closed="left").mean()
    )
    data["fraud_chip_rolling_mean_2_years"] = (
        data["fraud_chip"].rolling("730d", closed="left").mean()
    )

    data["fraud_online_rolling_mean_30_days"] = (
        data["fraud_online"].rolling("30d", closed="left").mean()
    )
    data["fraud_online_rolling_mean_60_days"] = (
        data["fraud_online"].rolling("60d", closed="left").mean()
    )
    data["fraud_online_rolling_mean_365_days"] = (
        data["fraud_online"].rolling("365d", closed="left").mean()
    )
    data["fraud_online_rolling_mean_2_years"] = (
        data["fraud_online"].rolling("730d", closed="left").mean()
    )

    data["fraud_card_present_rolling_mean_30_days"] = (
        data["fraud_card_present"].rolling("30d", closed="left").mean()
    )
    data["fraud_card_present_rolling_mean_60_days"] = (
        data["fraud_card_present"].rolling("60d", closed="left").mean()
    )
    data["fraud_card_present_rolling_mean_365_days"] = (
        data["fraud_card_present"].rolling("365d", closed="left").mean()
    )
    data["fraud_card_present_rolling_mean_2_years"] = (
        data["fraud_card_present"].rolling("730d", closed="left").mean()
    )
    logger.info("Computed fraud rolling statistics.")

    # Reset index back to original
    data = data.reset_index(drop=False)

    # Compare different fraud types' recent averages to
    # longer term averages (find spikes and lows)
    data["fraud_rolling_30_days_relative_to_365_days"] = (
        data["fraud_rolling_mean_30_days"] / data["fraud_rolling_mean_365_days"]
    )
    data["fraud_rolling_30_days_relative_to_2_years"] = (
        data["fraud_rolling_mean_30_days"] / data["fraud_rolling_mean_2_years"]
    )
    data["fraud_rolling_60_days_relative_to_365_days"] = (
        data["fraud_rolling_mean_60_days"] / data["fraud_rolling_mean_365_days"]
    )
    data["fraud_rolling_60_days_relative_to_2_years"] = (
        data["fraud_rolling_mean_60_days"] / data["fraud_rolling_mean_2_years"]
    )

    data["fraud_swipe_rolling_30_days_relative_to_365_days"] = (
        data["fraud_swipe_rolling_mean_30_days"]
        / data["fraud_swipe_rolling_mean_365_days"]
    )
    data["fraud_swipe_rolling_30_days_relative_to_2_years"] = (
        data["fraud_swipe_rolling_mean_30_days"]
        / data["fraud_swipe_rolling_mean_2_years"]
    )
    data["fraud_swipe_rolling_60_days_relative_to_365_days"] = (
        data["fraud_swipe_rolling_mean_60_days"]
        / data["fraud_swipe_rolling_mean_365_days"]
    )
    data["fraud_swipe_rolling_60_days_relative_to_2_years"] = (
        data["fraud_swipe_rolling_mean_60_days"]
        / data["fraud_swipe_rolling_mean_2_years"]
    )

    data["fraud_chip_rolling_30_days_relative_to_365_days"] = (
        data["fraud_chip_rolling_mean_30_days"]
        / data["fraud_chip_rolling_mean_365_days"]
    )
    data["fraud_chip_rolling_30_days_relative_to_2_years"] = (
        data["fraud_chip_rolling_mean_30_days"]
        / data["fraud_chip_rolling_mean_2_years"]
    )
    data["fraud_chip_rolling_60_days_relative_to_365_days"] = (
        data["fraud_chip_rolling_mean_60_days"]
        / data["fraud_chip_rolling_mean_365_days"]
    )
    data["fraud_chip_rolling_60_days_relative_to_2_years"] = (
        data["fraud_chip_rolling_mean_60_days"]
        / data["fraud_chip_rolling_mean_2_years"]
    )

    data["fraud_online_rolling_30_days_relative_to_365_days"] = (
        data["fraud_online_rolling_mean_30_days"]
        / data["fraud_online_rolling_mean_365_days"]
    )
    data["fraud_online_rolling_30_days_relative_to_2_years"] = (
        data["fraud_online_rolling_mean_30_days"]
        / data["fraud_online_rolling_mean_2_years"]
    )
    data["fraud_online_rolling_60_days_relative_to_365_days"] = (
        data["fraud_online_rolling_mean_60_days"]
        / data["fraud_online_rolling_mean_365_days"]
    )
    data["fraud_online_rolling_60_days_relative_to_2_years"] = (
        data["fraud_online_rolling_mean_60_days"]
        / data["fraud_online_rolling_mean_2_years"]
    )

    data["fraud_card_present_rolling_30_days_relative_to_365_days"] = (
        data["fraud_card_present_rolling_mean_30_days"]
        / data["fraud_card_present_rolling_mean_365_days"]
    )
    data["fraud_card_present_rolling_30_days_relative_to_2_years"] = (
        data["fraud_card_present_rolling_mean_30_days"]
        / data["fraud_card_present_rolling_mean_2_years"]
    )
    data["fraud_card_present_rolling_60_days_relative_to_365_days"] = (
        data["fraud_card_present_rolling_mean_60_days"]
        / data["fraud_card_present_rolling_mean_365_days"]
    )
    data["fraud_card_present_rolling_60_days_relative_to_2_years"] = (
        data["fraud_card_present_rolling_mean_60_days"]
        / data["fraud_card_present_rolling_mean_2_years"]
    )

    # Rolling proportions relative all frauds
    data["fraud_swipe_rolling_30_days_relative_to_all_frauds"] = (
        data["fraud_swipe_rolling_mean_30_days"] / data["fraud_rolling_mean_30_days"]
    )
    data["fraud_chip_rolling_30_days_relative_to_all_frauds"] = (
        data["fraud_chip_rolling_mean_30_days"] / data["fraud_rolling_mean_30_days"]
    )
    data["fraud_online_rolling_30_days_relative_to_all_frauds"] = (
        data["fraud_online_rolling_mean_30_days"] / data["fraud_rolling_mean_30_days"]
    )
    data["fraud_card_present_rolling_30_days_relative_to_all_frauds"] = (
        data["fraud_card_present_rolling_mean_30_days"]
        / data["fraud_rolling_mean_30_days"]
    )

    data["fraud_swipe_rolling_60_days_relative_to_all_frauds"] = (
        data["fraud_swipe_rolling_mean_60_days"] / data["fraud_rolling_mean_60_days"]
    )
    data["fraud_chip_rolling_60_days_relative_to_all_frauds"] = (
        data["fraud_chip_rolling_mean_60_days"] / data["fraud_rolling_mean_60_days"]
    )
    data["fraud_online_rolling_60_days_relative_to_all_frauds"] = (
        data["fraud_online_rolling_mean_60_days"] / data["fraud_rolling_mean_60_days"]
    )
    data["fraud_card_present_rolling_60_days_relative_to_all_frauds"] = (
        data["fraud_card_present_rolling_mean_60_days"]
        / data["fraud_rolling_mean_60_days"]
    )

    data["fraud_swipe_rolling_365_days_relative_to_all_frauds"] = (
        data["fraud_swipe_rolling_mean_365_days"] / data["fraud_rolling_mean_365_days"]
    )
    data["fraud_chip_rolling_365_days_relative_to_all_frauds"] = (
        data["fraud_chip_rolling_mean_365_days"] / data["fraud_rolling_mean_365_days"]
    )
    data["fraud_online_rolling_365_days_relative_to_all_frauds"] = (
        data["fraud_online_rolling_mean_365_days"] / data["fraud_rolling_mean_365_days"]
    )
    data["fraud_card_present_rolling_365_days_relative_to_all_frauds"] = (
        data["fraud_card_present_rolling_mean_365_days"]
        / data["fraud_rolling_mean_365_days"]
    )

    data["fraud_swipe_rolling_2_years_relative_to_all_frauds"] = (
        data["fraud_swipe_rolling_mean_2_years"] / data["fraud_rolling_mean_2_years"]
    )
    data["fraud_chip_rolling_2_years_relative_to_all_frauds"] = (
        data["fraud_chip_rolling_mean_2_years"] / data["fraud_rolling_mean_2_years"]
    )
    data["fraud_online_rolling_2_years_relative_to_all_frauds"] = (
        data["fraud_online_rolling_mean_2_years"] / data["fraud_rolling_mean_2_years"]
    )
    data["fraud_card_present_rolling_2_years_relative_to_all_frauds"] = (
        data["fraud_card_present_rolling_mean_2_years"]
        / data["fraud_rolling_mean_2_years"]
    )
    logger.info("Computed proportions of fraud rolling statistics.")

    # Mean amount spent over short time periods relative to longer time periods
    data["mean_amount_last_7_days_relative_to_last_year"] = (
        data["mean_amount_last_7_days"] / data["mean_amount_last_year"]
    )
    data["mean_amount_last_2_days_relative_to_last_year"] = (
        data["mean_amount_last_2_days"] / data["mean_amount_last_year"]
    )
    data["mean_amount_last_1_days_relative_to_last_year"] = (
        data["mean_amount_last_1_days"] / data["mean_amount_last_year"]
    )
    data["mean_amount_last_7_days_relative_to_last_30_days"] = (
        data["mean_amount_last_7_days"] / data["mean_amount_last_30_days"]
    )
    data["mean_amount_last_2_days_relative_to_last_30_days"] = (
        data["mean_amount_last_2_days"] / data["mean_amount_last_30_days"]
    )
    data["mean_amount_last_1_days_relative_to_last_30_days"] = (
        data["mean_amount_last_1_days"] / data["mean_amount_last_30_days"]
    )
    logger.info("Computed proportions of rolling mean amount.")

    # Calculate statistic related to transaction frequencies
    data["1_days_transaction_frequency_relative_to_last_30_days"] = (
        data["transaction_frequency_last_1_days"]
        / data["transaction_frequency_last_30_days"]
    )
    data["1_days_transaction_frequency_relative_to_last_year"] = (
        data["transaction_frequency_last_1_days"]
        / data["transaction_frequency_last_year"]
    )
    data["2_days_transaction_frequency_relative_to_last_30_days"] = (
        data["transaction_frequency_last_2_days"]
        / data["transaction_frequency_last_30_days"]
    )
    data["2_days_transaction_frequency_relative_to_last_year"] = (
        data["transaction_frequency_last_2_days"]
        / data["transaction_frequency_last_year"]
    )
    data["7_days_transaction_frequency_relative_to_last_30_days"] = (
        data["transaction_frequency_last_7_days"]
        / data["transaction_frequency_last_30_days"]
    )
    data["7_days_transaction_frequency_relative_to_last_year"] = (
        data["transaction_frequency_last_7_days"]
        / data["transaction_frequency_last_year"]
    )
    logger.info("Computed proportions of rolling transaction frequency.")

    return data
