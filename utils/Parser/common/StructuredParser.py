from ParserInterface import ParserInterface
import pyarrow as pa
import pandas as pd


class StructuredParser(ParserInterface):

    def name(self):
        return "structured_parser"

    def description(self):
        return "to parse structured data."

    def parse(self, file_url) -> pa.Table:
        df = pd.read_json(file_url)
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the structured_parser.',
            'origin_format': 'structured',
            'shape': ','.join(map(str, t.shape))
        })
        return t
