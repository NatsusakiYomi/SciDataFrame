import os
import tempfile
from io import BytesIO
import pyarrow as pa
import requests
from utils.Parser import ParserInterface
import numpy as np
import pandas as pd
from osgeo import gdal


class TifParser(ParserInterface):

    def name(self):
        return "tif_parser"

    def description(self):
        return "parse tif files"

    def parse(self, file_url) -> pa.Table:
        fd, temp_file_path = tempfile.mkstemp(suffix='.tif')
        with os.fdopen(fd, 'wb') as temp_file:
            response = requests.get(file_url)
            if response.status_code == 200:
                temp_file.write(response.content)
        dataset = gdal.Open(temp_file_path)
        df = pd.DataFrame(index=[0], columns=['im_width', 'im_height', 'im_bands', 'im_data', 'im_geotrans', 'im_proj'])
        df.loc[0]['im_width'] = dataset.RasterXSize  # 栅格矩阵的列数
        df.loc[0]['im_height'] = dataset.RasterYSize  # 栅格矩阵的行数
        df.loc[0]['im_bands'] = dataset.RasterCount  # 波段数
        df.loc[0]['im_data'] = array_to_bytes(dataset.ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize))  # 数据
        df.loc[0]['im_geotrans'] = ','.join(map(str, dataset.GetGeoTransform()))  # 仿射矩阵信息
        df.loc[0]['im_proj'] = dataset.GetProjection()  # 投影信息
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default tif_parser. It contains a single row with data on each column.',
            'origin_format': 'tif',
            'columns': "im_width, im_height, im_bands, im_data, im_geotrans, im_proj",
            'columns_meta': "width value, height value, bands value, data bytes, geotrans info, projection info",
            'shape': ','.join(map(str, t.shape)),
        })
        del dataset
        os.remove(temp_file_path)
        return t


def array_to_bytes(x: np.ndarray) -> bytes:
    np_bytes = BytesIO()
    np.save(np_bytes, x, allow_pickle=True)
    return np_bytes.getvalue()

if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=7b76c5de73930fb64856dd879b5f3606&path=/V1/SIDT1质粒载体构建测序数据/images/ndvi_2000.tif&fileName=ndvi_2000.tif"+"&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=TifParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)