from utils.Parser import ParserInterface
import pyarrow
import pandas as pd
from utils.Parser.unstructured.UnstructuredUtils import UnstructuredUtils


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
if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=d956d8d5802778cc6a313dc444cb8c9b&path=/V1/SIDT1质粒载体构建测序数据/xml/factbook-utf-16.xml&fileName=factbook-utf-16.xml"+"&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=XmlParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)