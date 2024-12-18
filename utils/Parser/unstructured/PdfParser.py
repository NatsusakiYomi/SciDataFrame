from ParserInterface import ParserInterface
import pyarrow
import pandas as pd
from unstructured.UnstructuredUtils import UnstructuredUtils


class PdfParser(ParserInterface):

    def name(self):
        return "pdf_parser"

    def description(self):
        return "to parse pdf files."

    def parse(self, file_url) -> pyarrow.Table:
        file_path = UnstructuredUtils.download_file_from_url(file_url, ".pdf")
        response_json = UnstructuredUtils.parse_unstructured_json(file_path)
        if response_json is not None:
            df = pd.DataFrame.from_records(response_json)
            return pyarrow.Table.from_pandas(df)
