from utils.Parser import ParserInterface
import pyarrow
import pandas as pd
from utils.Parser.unstructured.UnstructuredUtils import UnstructuredUtils


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
if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=1fd63aed28b28d01fd334fb2d5270f9d&path=/V1/SIDT1质粒载体构建测序数据/documents/DA-1p.pdf&fileName=DA-1p.pdf"+"&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=PdfParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)