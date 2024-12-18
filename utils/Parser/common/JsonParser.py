from urllib.parse import quote

from utils.Parser import ParserInterface
import pyarrow as pa
import pandas as pd


class JsonParser(ParserInterface):

    def name(self):
        return "json_parser"

    def description(self):
        return "to parse standard json files."

    def parse(self, file_url) -> pa.Table:
        encoded_url = quote(file_url, safe=":/?&=")
        df = pd.read_json(encoded_url)
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default json_parser.',
            'origin_format': 'json',
            'shape': ','.join(map(str, t.shape))
        })
        return t
if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=3c2968418d27e837fd2a5da717bfca4c&path=/V1/SIDT1质粒载体构建测序数据/json/水源涵养功能数据产品加工算子梳理_加数据源.json&fileName=%E6%B0%B4%E6%BA%90%E6%B6%B5%E5%85%BB%E5%8A%9F%E8%83%BD%E6%95%B0%E6%8D%AE%E4%BA%A7%E5%93%81%E5%8A%A0%E5%B7%A5%E7%AE%97%E5%AD%90%E6%A2%B3%E7%90%86_%E5%8A%A0%E6%95%B0%E6%8D%AE%E6%BA%90.json"+"&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=JsonParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)