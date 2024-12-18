from utils.Parser import ParserInterface
import pyarrow as pa
import pandas as pd
from urllib.parse import quote


class CsvParser(ParserInterface):

    def name(self):
        return "csv_parser"

    def description(self):
        return "to parse standard csv files."

    def parse(self, file_url) -> pa.Table:
        encoded_url = quote(file_url, safe=":/?&=")
        df = pd.read_csv(encoded_url)
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default csv_parser.',
            'origin_format': 'csv',
            'shape': ','.join(map(str, t.shape))
        })
        return t
if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=161235bc4ecdb6f105a43960173353d2&path=/V1/SIDT1质粒载体构建测序数据/csv/2019年中国榆林市沟道信息-02.csv&fileName=2019%E5%B9%B4%E4%B8%AD%E5%9B%BD%E6%A6%86%E6%9E%97%E5%B8%82%E6%B2%9F%E9%81%93%E4%BF%A1%E6%81%AF-02.csv&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)
    encoded_url=file_url
    encoded_url=quote(file_url, safe=":/?&=")
    # print(encoded_url)
    t=CsvParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=encoded_url)
    print(t)