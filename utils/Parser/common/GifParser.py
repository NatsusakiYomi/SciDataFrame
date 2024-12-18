import os
import tempfile
from io import BytesIO
import pyarrow as pa
import requests
from PIL import Image, ImageSequence
from ParserInterface import ParserInterface
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
        os.remove(temp_file_path)
        return t


def array_to_bytes(x: np.ndarray) -> bytes:
    np_bytes = BytesIO()
    np.save(np_bytes, x, allow_pickle=True)
    return np_bytes.getvalue()
