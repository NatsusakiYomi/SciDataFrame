import requests
import json
import tempfile
import gc
import pyarrow as pa
from pyarrow import csv
import csv as CSV
import io
import datetime
import psutil
from timeit import default_timer as timer
from tqdm import tqdm
import gzip
import timeit
from memory_profiler import profile
from gzip_stream import AsyncGZIPDecompressedStream
import zlib
import time
B = io.BytesIO
import os
from concurrent.futures import ThreadPoolExecutor
process = psutil.Process(os.getpid())
headers = {
        "Accept-Encoding": "deflate",
}

def process_gz(d,mode,buffer):
    return d.decompress(buffer) if mode=='gz' else buffer

# Function to process a chunk of data
def process_chunk(chunk, output_file):
    try:
        # Convert the CSV line to JSON (example conversion)
        data = chunk.decode('utf-8').strip().split(',')
        json_data = json.dumps(data)

        # Write the JSON data to the temporary file

        output_file.write(json_data + '\n')
    except Exception as e:
        print(f"Error processing chunk: {e}")
# @profile
def download_file(url, local_filename, chunk_size=1024*1024,iter=100):
    # Stream the request
    time=0
    response = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        # with tqdm(total=iter) as pbar:
            for i,chunk in enumerate(response.iter_content(chunk_size=chunk_size)):
                down_start=datetime.datetime.now()
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                if i==iter-1:
                    break
                # pbar.update()
                down_end = datetime.datetime.now()
                time+=(down_end-down_start).total_seconds()
    return local_filename,time
# Function to download data and distribute chunks to threads
# @profile
def read_file(local_filename,column_types,chunk_size=1024*1024,iter=100):

    read_options=csv.ReadOptions(block_size=chunk_size)
    convert_options = csv.ConvertOptions(column_types=column_types)
    # print("Before RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
    csv_reader = csv.open_csv(local_filename,read_options=read_options,
                              convert_options=convert_options)
    # print("Before write RSS: {}MB".format(pa.total_allocated_bytes() >> 20))

    output_stream = pa.BufferOutputStream()
    writer = None
    for i, batch in enumerate(csv_reader):
        # table = pa.Table.from_batches([batch])
        if writer is None:
            writer=pa.ipc.RecordBatchStreamWriter(output_stream,batch.schema)
        writer.write_batch(batch)
        # print("After RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
        if i==iter-1:
            break

    return writer
# @profile
def download_and_process_data(url, chunk_size=1024*1024, mode='', iter=100):
    time=0
    size = None
    response = requests.get(url, headers=headers,stream=True)
    raw_buffer = b''
    buffer = b''
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    # Create a temporary file to store processed data
    with tempfile.NamedTemporaryFile(mode='wb') as temp_file:
        output_file = temp_file.name
        column_types = None
        file_header = None
        with open('json_output', 'w') as f:
            # mem_before = process.memory_info().rss
            # with tqdm(total=iter) as pbar:
                for i,chunk in enumerate(response.iter_content(chunk_size=chunk_size,decode_unicode=True)):
                    process_start=datetime.datetime.now()
                    # buffer += chunk.encode('utf-8')
                    raw_buffer = chunk
                    if isinstance(raw_buffer,str):
                        raw_buffer = raw_buffer.encode('utf-8')
                    buffer+=process_gz(d,mode,raw_buffer)
                    # if file_header is None:
                    #     file_header=raw_buffer[:17]
                    # else:
                    #     raw_buffer=file_header+raw_buffer
                    # buffer+=process_gz(mode,raw_buffer,len(raw_buffer))
                    # del raw_buffer
                    # Find the last newline character in the buffer
                    if b'\n' in buffer:
                        if column_types is None:
                            first_newline_index=buffer.find(b'\n')
                            first_newline=buffer[:first_newline_index]
                            delimiter = CSV.Sniffer().sniff(first_newline.decode('utf-8')).delimiter.encode()
                            column_list=first_newline.split(delimiter) if first_newline[-1:]!=b'\r' else first_newline[:-1].split(delimiter)
                            column_types={col: pa.string() for col in column_list}
                            convert_options = csv.ConvertOptions(column_types=column_types)
                        last_newline = buffer.rfind(b'\n')
                        # lines = buffer[:last_newline].split(b'\n')
                        lines = buffer[:last_newline]
                        buffer = buffer[last_newline + 1:]
                        # mem_before_buffer = process.memory_info().rss
                        # print("Before RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
                        io_bytes = pa.py_buffer(lines+b'\n')
                        # mem_before_read = process.memory_info().rss
                        # print("After buffer RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
                        table=csv.read_csv(io_bytes,convert_options=convert_options)
                        if size==None:
                            size=table.__sizeof__()
                        # reader = pa.input_stream(io_bytes)
                        # with pa.input_stream(io_bytes) as stream:
                        #     stream.read()
                            # stream.seek(0)
                            # reader = csv.open_csv(stream).read_next_batch()
                        # io_bytes= (lines + b'\n')
                        #
                        #     batch=pa.record_batch(stream)
                        # print("After read RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
                        # mem_before_write = process.memory_info().rss
                        output_stream = pa.BufferOutputStream()
                        with pa.ipc.RecordBatchStreamWriter(output_stream, table.schema) as writer:
                                writer.write_table(table)
                        # pa.ipc.open_stream(io_bytes)
                        # mem_after = process.memory_info().rss
                        #         print("After writer RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
                        # for line in lines:
                        #     process_chunk(line + b'\n', f)
                        # break
                    else:
                        temp_file.write(buffer + b'\n')
                        del buffer
                        gc.collect()
                        buffer = b''
                    if i==iter-1:
                        break
                    # pbar.update()
                    process_end=datetime.datetime.now()
                    time+=(process_end-process_start).total_seconds()
                # time.sleep(1)
    # # Process any remaining buffer
        # if buffer:
        #     executor.submit(process_chunk, buffer, output_file)

        # return column_types,output_file,mem_before,mem_before_buffer,mem_before_read,mem_before_write,mem_after
        return column_types,output_file,time,size

t = pa.Table.from_arrays([[1, 2, 3], ["a", "b", "c"]], ["c1", "c2"])
buf = io.BytesIO()
csv.write_csv(t, buf, csv.WriteOptions(include_header=True))
buf.seek(0)
csv.read_csv(buf)
in_memory,disk=0,0

CHUNK_SIZE=100*1024*1024
MODE='gz'
ITER=1300

for i in range(5):
    # URL of the dataset
    # url = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2019/2019-10-29/nyc_squirrels.csv"
    url = 'http://10.0.82.71:81/api/resources/resourcesDownloadFile?resourcesId=b03f1e7da45d40d8a70ee46d0d560538&fileId=6667e30b1bb37b3d2c5e96f7&token=eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJpbnN0ZGIiLCJzdWIiOiJhZG1pbkBpbnN0ZGIuY24iLCJhdWQiOiJ3ZWIiLCJpYXQiOjE3MTkzODI3NzAsIm5hbWUiOiLotoXnuqfnrqHnkIblkZgiLCJ1c2VySWQiOiI2MThiNmQ4ZWIwMzM1ODc5YWNkYmZiZDkiLCJyb2xlcyI6WyJyb2xlX2FkbWluIl0sInVuQXV0aFBhdGgiOltdLCJ0eXBlIjoiYWNjZXNzVG9rZW4iLCJleHAiOjE3MTkzOTM1NzB9.MfTuq1S2qPC15PatUqaNmX_5h8Ydn8hofdKVjpZnFq3xNcn8LdwfyIL7Xr14bOrplAkdRz2PuHvnVtnkVJ9HSg'
    # url = 'https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD'
    url = 'https://tcga-pancan-atlas-hub.s3.us-east-1.amazonaws.com/download/jhu-usc.edu_PANCAN_HumanMethylation450.betaValue_whitelisted.tsv.synapse_download_5096262.xena.gz'
    # url = 'http://10.0.90.221:50075/webhdfs/v1/testdata/2019.csv?op=OPEN&namenoderpcaddress=master221:9000&offset=0'
    # Start the download and processing
    # url='http://10.0.90.221:50075/webhdfs/v1/testdata/ACSM-AE33.xlsx?op=OPEN&namenoderpcaddress=master221:9000&offset=0'
    # url='http://10.0.90.221:50075/webhdfs/v1/testdata/employee.txt?op=OPEN&namenoderpcaddress=master221:9000&offset=0'
    # url='http://10.0.90.221:50075/webhdfs/v1/testdata/test.json?op=OPEN&namenoderpcaddress=master221:9000&offset=0'
    start=timer()
    # column_types,output_file, \
    #     before, before_buffer, before_read,\
    #     before_write, after= download_and_process_data(url,chunk_size=CHUNK_SIZE,mode='gz',iter=100)
    column_types,output_file,process_time,size= download_and_process_data(url,mode=MODE,chunk_size=1024*1024,iter=1)
    #
    end1=timer()
    print(f"In memory {i}: {(end1-start)}")
    in_memory+=(end1-start)
    #
    time.sleep(5)
    end1=timer()
    # read_file('jhu-usc.edu_PANCAN_HumanMethylation450.betaValue_whitelisted.tsv.synapse_download_5096262.xena.gz',
    #           column_types=column_types,chunk_size=CHUNK_SIZE,iter=5)
    # file_name,down_time=download_file(url,'direct_write_2.'+MODE,chunk_size=CHUNK_SIZE,iter=ITER)
    file_name='jhu-usc.edu_PANCAN_HumanMethylation450.betaValue_whitelisted.tsv.synapse_download_5096262.xena.gz'
    read_file(file_name,
              column_types=column_types,chunk_size=size,iter=ITER)

    end2=timer()
    # The processed data is now stored in output_file
    # print(f"Processed data is stored in {output_file}")
    print(f"Disk {i}: {(end2-end1)}")
    disk+=(end2-end1)
    # print(timeit("find(string, text)", "from __main__ import download_and_process_data; url=''http://10.0.82.71:81/api/resources/resourcesDownloadFile?resourcesId=b03f1e7da45d40d8a70ee46d0d560538&fileId=6667e30b1bb37b3d2c5e96f7&token=eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJpbnN0ZGIiLCJzdWIiOiJhZG1pbkBpbnN0ZGIuY24iLCJhdWQiOiJ3ZWIiLCJpYXQiOjE3MTkzODI3NzAsIm5hbWUiOiLotoXnuqfnrqHnkIblkZgiLCJ1c2VySWQiOiI2MThiNmQ4ZWIwMzM1ODc5YWNkYmZiZDkiLCJyb2xlcyI6WyJyb2xlX2FkbWluIl0sInVuQXV0aFBhdGgiOltdLCJ0eXBlIjoiYWNjZXNzVG9rZW4iLCJleHAiOjE3MTkzOTM1NzB9.MfTuq1S2qPC15PatUqaNmX_5h8Ydn8hofdKVjpZnFq3xNcn8LdwfyIL7Xr14bOrplAkdRz2PuHvnVtnkVJ9HSg'"))
    # print(timeit("re_find(string, text)", "from __main__ import download_file; url=''http://10.0.82.71:81/api/resources/resourcesDownloadFile?resourcesId=b03f1e7da45d40d8a70ee46d0d560538&fileId=6667e30b1bb37b3d2c5e96f7&token=eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJpbnN0ZGIiLCJzdWIiOiJhZG1pbkBpbnN0ZGIuY24iLCJhdWQiOiJ3ZWIiLCJpYXQiOjE3MTkzODI3NzAsIm5hbWUiOiLotoXnuqfnrqHnkIblkZgiLCJ1c2VySWQiOiI2MThiNmQ4ZWIwMzM1ODc5YWNkYmZiZDkiLCJyb2xlcyI6WyJyb2xlX2FkbWluIl0sInVuQXV0aFBhdGgiOltdLCJ0eXBlIjoiYWNjZXNzVG9rZW4iLCJleHAiOjE3MTkzOTM1NzB9.MfTuq1S2qPC15PatUqaNmX_5h8Ydn8hofdKVjpZnFq3xNcn8LdwfyIL7Xr14bOrplAkdRz2PuHvnVtnkVJ9HSg'; local_filename='direct_write.csv'"))
    # Cleanup: delete the temporary file if no longer needed
    # os.remove(output_file)
    # print("python Process RSS\n"
    #       "before: {}MB\n"
    #       "before buffer: {}MB\n"
    #       "before read: {}MB\n"
    #       "before write: {}MB\n"
    #       "after: {}MB\n".format(before >> 20,before_buffer>>20,before_read>>20,before_write>>20,after>>20))
    time.sleep(5)
print(f"AVG In memory: {in_memory/5}")
print(f"AVG Disk: {disk/5}")
print(f"AVG Disk: {(in_memory-disk)/disk}")