from ParserInterface import ParserInterface
import pyarrow
import pandas as pd
from unstructured.UnstructuredUtils import UnstructuredUtils


class XmlParser(ParserInterface):

    def name(self):
        return "xml_parser"

    def description(self):
        return "to parse .xml files."

    def parse(self, file_url) -> pyarrow.Table:
        file_path = UnstructuredUtils.download_file_from_url(file_url, ".xml")
        response_json = UnstructuredUtils.parse_unstructured_json(file_path)
        if response_json is not None:
            df = pd.DataFrame.from_records(response_json)
            return pyarrow.Table.from_pandas(df)
