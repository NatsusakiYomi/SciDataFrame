from utils.Parser import ParserInterface
import pyarrow
import pandas as pd
from utils.Parser.unstructured.UnstructuredUtils import UnstructuredUtils


class MdParser(ParserInterface):

    def name(self):
        return "md_parser"

    def description(self):
        return "to parse md files."

    def parse(self, file_url) -> pyarrow.Table:
        file_path = UnstructuredUtils.download_file_from_url(file_url, ".md")
        response_json = UnstructuredUtils.parse_unstructured_json(file_path)
        if response_json is not None:
            df = pd.DataFrame.from_records(response_json)
            return pyarrow.Table.from_pandas(df)
if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=1340dfccd597a34826ad53f28ef5c6e8&path=/V1/SIDT1质粒载体构建测序数据/documents/README.md&fileName=README.md"+"&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=MdParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)