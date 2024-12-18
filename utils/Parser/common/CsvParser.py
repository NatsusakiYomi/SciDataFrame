from ParserInterface import ParserInterface
import pyarrow as pa
import pandas as pd


class CsvParser(ParserInterface):

    def name(self):
        return "csv_parser"

    def description(self):
        return "to parse standard csv files."

    def parse(self, file_url) -> pa.Table:
        df = pd.read_csv(file_url)
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default csv_parser.',
            'origin_format': 'csv',
            'shape': ','.join(map(str, t.shape))
        })
        return t
