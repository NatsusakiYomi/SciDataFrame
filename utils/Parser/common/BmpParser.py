import os
import tempfile
import pyarrow as pa
import requests
from PIL import Image
from utils.Parser import ParserInterface
import numpy as np
import pandas as pd


class BmpParser(ParserInterface):

    def name(self):
        return "bmp_parser"

    def description(self):
        return "parse .bmp files to a dataframe of pixels"

    def parse(self, file_url) -> pa.Table:
        fd, temp_file_path = tempfile.mkstemp(suffix='.bmp')
        with os.fdopen(fd, 'wb') as temp_file:
            response = requests.get(file_url)
            if response.status_code == 200:
                temp_file.write(response.content)
        image = Image.open(temp_file_path)
        colour_pixels = image.convert("RGB")
        colour_array = np.array(colour_pixels.getdata()).reshape(image.size + (3,))
        indices_array = np.moveaxis(np.indices(image.size), 0, 2)
        all_array = np.dstack((indices_array, colour_array)).reshape((-1, 5))
        df = pd.DataFrame(all_array, columns=["y", "x", "R", "G", "B"])
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default bmp_parser. Its data represents rgb values of every pixel.',
            'origin_format': 'bmp',
            'columns': "y, x, R, G, B",
            'columns_meta': "pixel y, pixel x, RED, GREEN, BLUE",
            'shape': ','.join(map(str, t.shape))
        })
        image.close()
        os.remove(temp_file_path)
        return t

if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=d012bb73f79404561c70b464bc37d84a&path=/V1/SIDT1质粒载体构建测序数据/images/HNeph_DrySCA.bmp&fileName=HNeph_DrySCA.bmp"+"&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=BmpParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)