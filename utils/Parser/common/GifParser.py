import os
import tempfile
from io import BytesIO
import pyarrow as pa
import requests
from PIL import Image, ImageSequence
from utils.Parser import ParserInterface
import numpy as np
import pandas as pd


class GifParser(ParserInterface):

    def name(self):
        return "gif_parser"

    def description(self):
        return "parse .gif files"

    def parse(self, file_url) -> pa.Table:
        fd, temp_file_path = tempfile.mkstemp(suffix='.gif')
        with os.fdopen(fd, 'wb') as temp_file:
            response = requests.get(file_url)
            if response.status_code == 200:
                temp_file.write(response.content)
        image = Image.open(temp_file_path)
        frames = {}
        count = 0
        for frame in ImageSequence.Iterator(image):
            colour_pixels = frame.convert("RGB")
            colour_array = np.array(colour_pixels.getdata()).reshape(frame.size + (3,))
            indices_array = np.moveaxis(np.indices(frame.size), 0, 2)
            all_array = np.dstack((indices_array, colour_array)).reshape((-1, 5))
            frame_bytes = array_to_bytes(all_array)
            frames['frame' + str(count)] = frame_bytes
            count += 1

        df = pd.DataFrame.from_dict(frames, orient='index')
        df.columns = ['bytes']
        df = df.reset_index()
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default gif_parser. Each row is a frame of the gif image.',
            'origin_format': 'gif',
            'columns': "index, bytes",
            'columns_meta': "index of frames in a gif, data bytes of a pandas dataframe(rgb pixels)",
            'shape': ','.join(map(str, t.shape))
        })
        image.close()
        os.remove(temp_file_path)
        return t


def array_to_bytes(x: np.ndarray) -> bytes:
    np_bytes = BytesIO()
    np.save(np_bytes, x, allow_pickle=True)
    return np_bytes.getvalue()

if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=8d6c7be89bdfd9f0a26e8d3f6e21a450&path=/V1/SIDT1质粒载体构建测序数据/images/北京-新疆-海南卫星站分布.gif&fileName=%E5%8C%97%E4%BA%AC-%E6%96%B0%E7%96%86-%E6%B5%B7%E5%8D%97%E5%8D%AB%E6%98%9F%E7%AB%99%E5%88%86%E5%B8%83.gif"+"&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=GifParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)