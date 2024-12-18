from ParserInterface import ParserInterface
import pyarrow as pa
import pandas as pd


class JsonParser(ParserInterface):

    def name(self):
        return "json_parser"

    def description(self):
        return "to parse standard json files."

    def parse(self, file_url) -> pa.Table:
        df = pd.read_json(file_url)
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default json_parser.',
            'origin_format': 'json',
            'shape': ','.join(map(str, t.shape))
        })
        return t
