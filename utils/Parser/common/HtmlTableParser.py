import os
import tempfile
from io import BytesIO
import requests
from ParserInterface import ParserInterface
import pyarrow as pa
import pandas as pd
import numpy as np


class HtmlTableParser(ParserInterface):

    def name(self):
        return "html_table_parser"

    def description(self):
        return "to parse html tables."

    def parse(self, file_url) -> pa.Table:
        fd, temp_file_path = tempfile.mkstemp(suffix='.html')
        with os.fdopen(fd, 'wb') as temp_file:
            response = requests.get(file_url)
            if response.status_code == 200:
                temp_file.write(response.content)
        tables = pd.read_html(temp_file_path)
        df_dict = {}
        for i in range(len(tables)):
            df_dict[i] = array_to_bytes(tables[i].to_numpy())
        df = pd.DataFrame.from_dict(df_dict, orient='index')
        df.columns = ['bytes']
        df = df.reset_index()
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default html_table_parser. Each row is a table in html.',
            'origin_format': 'html',
            'columns': "index, bytes",
            'columns_meta': "index of tables in html, data bytes of a pandas dataframe(html table)",
            'shape': ','.join(map(str, t.shape))
        })
        os.remove(temp_file_path)
        return t


def array_to_bytes(x: np.ndarray) -> bytes:
    np_bytes = BytesIO()
    np.save(np_bytes, x, allow_pickle=True)
    return np_bytes.getvalue()
