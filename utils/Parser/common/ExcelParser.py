from ParserInterface import ParserInterface
import pyarrow as pa
import pandas as pd


class ExcelParser(ParserInterface):

    def name(self):
        return "excel_parser"

    def description(self):
        return "to parse standard excel files."

    def parse(self, file_url) -> pa.Table:
        df = pd.read_excel(file_url)
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default excel_parser.',
            'origin_format': 'excel',
            'shape': ','.join(map(str, t.shape))
        })
        return t
