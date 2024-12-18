import os
import tempfile
from io import BytesIO
import pyarrow as pa
import requests
from ParserInterface import ParserInterface
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
        os.remove(temp_file_path)
        return t


def array_to_bytes(x: np.ndarray) -> bytes:
    np_bytes = BytesIO()
    np.save(np_bytes, x, allow_pickle=True)
    return np_bytes.getvalue()
