import pyarrow as pa
import pyarrow.csv as pv
import requests
from io import StringIO
import os
from datetime import datetime
from timeit import timeit
import re

from memory_profiler import profile
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''
def test():
    def find(string, text):
        if string.find(text) > -1:
            pass

    def re_find(string, text):
        if re.match(text, string):
            pass

    def best_find(string, text):
        if text in string:
            pass

    print
    timeit("find(string, text)", "from __main__ import find; string='lookforme'; text='look'")
    print
    timeit("re_find(string, text)", "from __main__ import re_find; string='lookforme'; text='look'")
    print
    timeit("best_find(string, text)", "from __main__ import best_find; string='lookforme'; text='look'")

def stream_ic_csv_to_arrow_table(url, batch_size=1024):
    response = requests.get(url, stream=True, verify=False, timeout=10)
    response.raise_for_status()


# @profile(precision=3)
# Function to stream CSV from HTTP and convert to Arrow Table
def stream_csv_to_arrow_table(url, batch_size=1024):
    response = requests.get(url, stream=True,verify=False, timeout=10)
    response.raise_for_status()

    buffer = StringIO()
    table = None
    start = datetime.now()
    for chunk in response.iter_lines(chunk_size=2048, decode_unicode=True):
        buffer.write(chunk + '\n')
        buffer.seek(0)
        if table is None:
            table = pv.read_csv(buffer)
        else:
            new_table = pv.read_csv(buffer)
            table = pa.concat_tables([table, new_table])

        buffer.seek(0)
        buffer.truncate(0)

    return start,table


# Example usage
if __name__ == "__main__":
    # Define the URL of the CSV file
    csv_url = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2019/2019-10-29/nyc_squirrels.csv"
    # csv_url = 'https://geeksforgeeks.org'
    # img_url = 'http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz'
    # Stream CSV and convert to Arrow Table
    # start,arrow_table=arrow_table = stream_csv_to_arrow_table(csv_url)
    # end = datetime.now()
    # print(f"processing time: ", end-start)
    # # Print the Arrow Table
    # print(arrow_table.to_pandas())
    test()
