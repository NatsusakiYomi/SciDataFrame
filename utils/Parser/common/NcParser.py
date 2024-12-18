import json
import os
import tempfile
from io import BytesIO
import numpy as np
import pyarrow as pa
import requests
from utils.Parser import ParserInterface
import xarray as xr
import pandas as pd


class NcParser(ParserInterface):

    def name(self):
        return "nc_parser"

    def description(self):
        return "to parse nc files"

    def parse(self, file_url) -> pa.Table:
        fd, temp_file_path = tempfile.mkstemp(suffix='.nc')
        with os.fdopen(fd, 'wb') as temp_file:
            response = requests.get(file_url)
            if response.status_code == 200:
                temp_file.write(response.content)
        ds = xr.open_dataset(temp_file_path)
        # dims shape
        dims_info = {}
        for dim in ds.dims.keys():
            dims_info[dim] = ds.dims[dim]
        dims_info_str = json.dumps(dims_info)
        # variables
        variable_names = ds.variables.keys()
        variable_value_dict = {}
        variable_value_dtype_dict = {}
        variable_dim_dict = {}
        variable_desc_dict = {}
        variable_unit_dict = {}
        for name in variable_names:
            var = ds.variables[name]
            variable_value_dict[name] = array_to_bytes(var[:].to_numpy())
            dtype_str = var.dtype.str
            if var.dtype == object:
                dtype_str = 'str'
            variable_value_dtype_dict[name] = dtype_str
            variable_dim_dict[name] = var.dims
            variable_desc_dict[name] = var.attrs.get('description', '')
            variable_unit_dict[name] = var.attrs.get('unit', '')
        df = pd.DataFrame.from_dict(variable_value_dict, orient='index')
        df.columns = ['bytes']
        df['dtype'] = ''
        df['dimensions'] = ''
        df['description'] = ''
        df['unit'] = ''
        for name in variable_names:
            df.loc[df.index == name, 'dtype'] = variable_value_dtype_dict[name]
            df.loc[df.index == name, 'dimensions'] = ','.join(map(str, variable_dim_dict[name]))
            df.loc[df.index == name, 'description'] = str(variable_desc_dict[name])
            df.loc[df.index == name, 'unit'] = str(variable_unit_dict[name])
        ds.close()
        os.remove(temp_file_path)
        df = df.reset_index()
        t = pa.Table.from_pandas(df)
        # metadata
        t = t.replace_schema_metadata({
            'description': 'This dataframe is parsed by the default nc_parser. Each row is a variable in the nc file.',
            'origin_format': 'nc',
            'columns': "index, bytes, dtype, dimensions, description, unit",
            'columns_meta': "variable name, variable data bytes, dtype, variable dimensions, variable description, data unit",
            'shape': ','.join(map(str, t.shape)),
            'dims_info': dims_info_str
        })
        return t


def array_to_bytes(x: np.ndarray) -> bytes:
    np_bytes = BytesIO()
    np.save(np_bytes, x, allow_pickle=True)
    return np_bytes.getvalue()
if __name__=="__main__":
    nc=NcParser.parse(NcParser,file_url="https://download.scidb.cn/download?fileId=5bcb7621c29286bb9847b773614c2218&path=/V1/SIDT1质粒载体构建测序数据/outputs/HDF_MPL_4202_202103230343.hdf&fileName=HDF_MPL_4202_202103230343.hdf&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a")
    nc=None