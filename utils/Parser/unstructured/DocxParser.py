from utils.Parser import ParserInterface
import pyarrow
import pandas as pd
from utils.Parser.unstructured.UnstructuredUtils import UnstructuredUtils


class DocxParser(ParserInterface):

    def name(self):
        return "docx_parser"

    def description(self):
        return "to parse doc,docx files."

    def parse(self, file_url) -> pyarrow.Table:
        file_path = UnstructuredUtils.download_file_from_url(file_url, ".docx")
        response_json = UnstructuredUtils.parse_unstructured_json(file_path)
        if response_json is not None:
            df = pd.DataFrame.from_records(response_json)
            return pyarrow.Table.from_pandas(df)
if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=1844ebdf2d68e201eff5861939d90a95&path=/V1/SIDT1质粒载体构建测序数据/documents/基于新闻文本的汉藏新词语数据集（2017-2022）.docx&fileName=%E5%9F%BA%E4%BA%8E%E6%96%B0%E9%97%BB%E6%96%87%E6%9C%AC%E7%9A%84%E6%B1%89%E8%97%8F%E6%96%B0%E8%AF%8D%E8%AF%AD%E6%95%B0%E6%8D%AE%E9%9B%86%EF%BC%882017-2022%EF%BC%89.docx&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=DocxParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)