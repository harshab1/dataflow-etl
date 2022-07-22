
from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os

PROJECT_ID = 'arc-insights-20220721'
SCHEMA = 'Date:DATE,Open:FLOAT,High:FLOAT,Low:FLOAT,Close:FLOAT,Volume:INTEGER'

class DataIngestion:
    def parse_method(self, string_input):
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('Date', '', 'Open', 'High', 'Low', 'Close', 'Volume'),
                values))
        return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.',
        default='gs://stocks-project-2/full_history/AADR.csv')

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='stocks_data.historical_prices')


    known_args, pipeline_args = parser.parse_known_args(argv)

    data_ingestion = DataIngestion()


    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (
     p | 'Read from a File' >> beam.io.ReadFromText(known_args.input,
                                                  skip_header_lines=1)
    

     | 'String To BigQuery Row' >>
     beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
         beam.io.BigQuerySink(

             known_args.output,


             schema=SCHEMA,


             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,

             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()