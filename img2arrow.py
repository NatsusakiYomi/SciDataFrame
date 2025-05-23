import requests
import pyarrow as pa
from PIL import Image
import io
url='http://10.0.90.221:50075/webhdfs/v1/testdata/spark5.jpeg?op=OPEN&namenoderpcaddress=master221:9000&offset=0'
url='http://10.0.90.221:50075/webhdfs/v1/testdata/ndvi_2000.tif?op=OPEN&namenoderpcaddress=master221:9000&offset=0'
buffer=b''
response = requests.get(url,stream=True)
records = []
for chunk in response.iter_content(chunk_size=1024):
    buffer+=chunk
    records.append({
        "bytes": chunk
    })
table = pa.Table.from_pylist(records)
concatenated_data = bytearray()
column = table['bytes']
for value in column:
    concatenated_data.extend(value.as_buffer())
image = Image.open(io.BytesIO(buffer))
image.save('output.tiff', format='TIFF')
