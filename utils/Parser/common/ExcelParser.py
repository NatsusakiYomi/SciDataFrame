from urllib.parse import quote

from utils.Parser import ParserInterface
import pyarrow as pa
import pandas as pd


class ExcelParser(ParserInterface):

    def name(self):
        return "excel_parser"

    def description(self):
        return "to parse standard excel files."

    def parse(self, file_url) -> pa.Table:
        encoded_url = quote(file_url, safe=":/?&=")
        df = pd.read_excel(encoded_url)
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default excel_parser.',
            'origin_format': 'excel',
            'shape': ','.join(map(str, t.shape))
        })
        return t
if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=879a4ceab6f60c58cdc513edc1f5fc85&path=/V1/SIDT1质粒载体构建测序数据/excel/ACSM-AE33-00.xlsx&fileName=ACSM-AE33-00.xlsx"+"&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=ExcelParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)