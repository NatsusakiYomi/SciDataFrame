import pyarrow as pa
with pa.OSFile('partition_0.arrow', 'rb') as source:
   loaded_array = pa.ipc.open_file(source).read_all()
print(loaded_array)