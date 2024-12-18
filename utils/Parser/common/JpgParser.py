import os
import tempfile
import pyarrow as pa
import requests
from PIL import Image
from utils.Parser import ParserInterface
import numpy as np
import pandas as pd


class JpgParser(ParserInterface):

    def name(self):
        return "jpg_parser"

    def description(self):
        return "parse .jpg files to a dataframe of pixels"

    def parse(self, file_url) -> pa.Table:
        fd, temp_file_path = tempfile.mkstemp(suffix='.png')
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
            'description': 'This dataframe is parsed by the default jpg_parser. Its data represents rgb values of every pixel.',
            'origin_format': 'jpg',
            'columns': "y, x, R, G, B",
            'columns_meta': "pixel y, pixel x, RED, GREEN, BLUE",
            'shape': ','.join(map(str, t.shape))
        })
        os.remove(temp_file_path)
        return t
