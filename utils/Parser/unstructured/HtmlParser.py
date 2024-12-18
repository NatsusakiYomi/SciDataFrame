from utils.Parser import ParserInterface
import pyarrow
import pandas as pd
from utils.Parser.unstructured.UnstructuredUtils import UnstructuredUtils


class HtmlParser(ParserInterface):

    def name(self):
        return "html_parser"

    def description(self):
        return "to parse .html, .h5 files."

    def parse(self, file_url) -> pyarrow.Table:
        file_path = UnstructuredUtils.download_file_from_url(file_url, ".html")
        response_json = UnstructuredUtils.parse_unstructured_json(file_path)
        if response_json is not None:
            df = pd.DataFrame.from_records(response_json)
            return pyarrow.Table.from_pandas(df)

if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=07a81294d8c2e6841f3e6ccf18f727f5&path=/V1/SIDT1质粒载体构建测序数据/documents/example-10k-1p.html&fileName=example-10k-1p.html"+"&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=HtmlParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)