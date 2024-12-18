import os
import pickle
import tempfile
import requests
from utils.Parser import ParserInterface
import pyarrow as pa
import pandas as pd
from astropy.io import fits


class FitsParser(ParserInterface):

    def name(self):
        return "fits_parser"

    def description(self):
        return "to parse fits files."

    def parse(self, file_url) -> pa.Table:
        fd, temp_file_path = tempfile.mkstemp(suffix='.fits')
        with os.fdopen(fd, 'wb') as temp_file:
            response = requests.get(file_url)
            if response.status_code == 200:
                temp_file.write(response.content)
        hdu = fits.open(temp_file_path)
        hdu_headers = []
        hdu_datas = []
        hdu_types = []
        for i in range(len(hdu)):
            hdu_headers.append(pickle.dumps(hdu[i].header))
            hdu_datas.append(pickle.dumps(hdu[i].data))
            hdu_types.append(pickle.dumps(type(hdu[i])))
        df = pd.DataFrame()
        df['header'] = hdu_headers
        df['data'] = hdu_datas
        df['hdu_type'] = hdu_types
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default fits_parser. Each row is a hdu in the fits file.',
            'origin_format': 'fits',
            'columns': "header, data, hdu_type",
            'columns_meta': "hdu header bytes, hdu data bytes, hdu type",
            'shape': ','.join(map(str, t.shape)),
        })
        hdu.close()
        os.remove(temp_file_path)
        return t
if __name__=="__main__":
    from arrow_flight import MyFlightServer,char_det
    file_url="https://download.scidb.cn/download?fileId=d614d82fb31446c00878c7aeba21e34f&path=/V1/SIDT1质粒载体构建测序数据/outputs/fxt_a_11900000001_of_00_po_uf_evt_0aa.fits&fileName=fxt_a_11900000001_of_00_po_uf_evt_0aa.fits&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    # char_det(file_url)

    # print(encoded_url)
    t=FitsParser.parse(MyFlightServer('grpc://127.0.0.1:8815'),file_url=file_url)
    print(t)