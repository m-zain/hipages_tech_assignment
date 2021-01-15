import pandas as pd
import os
import datetime


def parse_time_stamp(consumed_events_df):
    """
    Description: Function i) to convert time_stamp of the events from string into timestamp.
                          ii) converting time_stamp to hourly format. e.g 'yyyyMMddHH'
    :parameter: consumed_events_df (all the consumed events)
    :return: consumed_events_df after converting time_stamp.
    """

    consumed_events_df["time_stamp"] = pd.to_datetime(consumed_events_df['time_stamp'], format="%d/%m/%Y %H:%M:%S")
    consumed_events_df["time_stamp"] = consumed_events_df["time_stamp"].dt.strftime("%Y%m%d%H")


def aggregate_event_stats(consumed_events_df):
    """
    Description: Function to get aggregated stats (activity_count and user_count)
    at hourly granularity, url_level1 and activity level.
    :parameter: consumed_events_df (all the consumed events)
    :return: aggregated_web_stats (aggregated event stats)
    """

    aggregated_web_stats = consumed_events_df.groupby(
        ["time_stamp", "url_level1", "activity"], as_index=False
    ).agg(

            activity_count=('activity', 'count'),  # count of activities.
            user_count=('user_id', 'nunique')  # get the count of unique users

    )

    aggregated_web_stats.sort_values('time_stamp')

    return aggregated_web_stats


if __name__ == '__main__':

    if os.path.isfile('user_web_stats.csv'):
        consumed_events_df = pd.read_csv('user_web_stats.csv')

        # converting and rounding the time_stamp till hour for the events.
        parse_time_stamp(consumed_events_df)

        # performing aggregation on the stored events
        aggregated_web_stats = aggregate_event_stats(consumed_events_df)
        aggregated_web_stats.columns = aggregated_web_stats.columns.get_level_values(0)

        # Exporting aggregated events stats into CSV (tabular form)
        aggregated_web_stats.to_csv("aggregated_web_stats.csv", index=False, header=True, mode="w")

    else:
        print("user_web_stats.csv file not found to perform aggregations")
        print("Please execute consume_events.py to consume events first")