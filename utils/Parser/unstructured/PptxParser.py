from utils.Parser import ParserInterface
import pyarrow
import pandas as pd
from utils.Parser.unstructured.UnstructuredUtils import UnstructuredUtils


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
if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=4708aaad27c452e9abf2d0a8c7539e12&path=/V1/SIDT1质粒载体构建测序数据/documents/bullet_slide.pptx&fileName=bullet_slide.pptx"+"&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=PptxParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)