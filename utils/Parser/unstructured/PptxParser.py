from ParserInterface import ParserInterface
import pyarrow
import pandas as pd
from unstructured.UnstructuredUtils import UnstructuredUtils


class PptxParser(ParserInterface):

    def name(self):
        return "pptx_parser"

    def description(self):
        return "to parse ppt,pptx files."

    def parse(self, file_url) -> pyarrow.Table:
        file_path = UnstructuredUtils.download_file_from_url(file_url, ".pptx")
        response_json = UnstructuredUtils.parse_unstructured_json(file_path)
        if response_json is not None:
            df = pd.DataFrame.from_records(response_json)
            return pyarrow.Table.from_pandas(df)
