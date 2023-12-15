import argparse
from datetime import datetime, timezone
from typing import NamedTuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import trigger, window
from apache_beam.utils.timestamp import Timestamp


class Transaction(NamedTuple):
    user: str
    amount: float
    timestamp: datetime


def parse_row(row):
    timestamp = datetime(
        int(row["Year"]),
        int(row["Month"]),
        int(row["Day"]),
        int(row["Time"][:2]),
        int(row["Time"][3:]),
        tzinfo=timezone.utc,
    )
    transaction = Transaction(
        user=row["User"], amount=row["Amount"], timestamp=timestamp
    )
    yield window.TimestampedValue(transaction, Timestamp.from_utc_datetime(timestamp))


class BuildTimestampedValueFn(beam.DoFn):
    def process(self, element: Transaction, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()
        return [
            element._asdict() | {"window_start": window_start, "window_end": window_end}
        ]


def run(argv=None):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        pass


if __name__ == "__main__":
    output = []

    def collect(row):
        output.append(row)
        return True

    def map_func(row):
        return (row["User"], 1)

    p = beam.Pipeline()
    users = (
        p
        | "read"
        >> beam.io.ReadFromBigQuery(
            table="robertofierimonte-ml-pipe.credit_card_frauds_20230614T145952.test",
            method=beam.io.ReadFromBigQuery.Method.DIRECT_READ,
        )
        | "map" >> beam.FlatMap(parse_row).with_output_types(Transaction)
        | "window"
        >> beam.WindowInto(
            windowfn=window.SlidingWindows(30 * 24 * 60 * 60, 60 * 60 * 24, 662774400),
            accumulation_model=trigger.AccumulationMode.DISCARDING,
        )
        | "groupby"
        >> beam.GroupBy("user")
        .aggregate_field(
            field="amount",
            combine_fn=beam.combiners.MeanCombineFn(),
            dest="avg_Amount",
        )
        .aggregate_field(
            field="amount",
            combine_fn=beam.combiners.CountCombineFn(),
            dest="cnt_transactions",
        )
        | "add_timestamp" >> beam.ParDo(BuildTimestampedValueFn())
        # | "print" >> beam.Map(collect)
        | "write"
        >> beam.io.WriteToBigQuery(
            table="robertofierimonte-ml-pipe.credit_card_frauds_20230614T145952.beam_output",
            schema="user:STRING,avg_amount:FLOAT64,cnt_transactions:INT64,window_start:DATETIME,window_end:DATETIME",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
    )
    result = p.run()
    # result.wait_until_finish()

    # print(output)
