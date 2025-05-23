import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
import pyarrow.parquet as pq
from pyarrow import flight
import os # 导入os模块
os.environ['PYSPARK_PYTHON']='"C:\\Users\\NatsusakiYomi\\.conda\\envs\\PYSPARK\\python.exe"'#本机电脑所使用的python编译器的地址
import pyarrow as pa
from pyarrow import csv
import pyarrow.ipc as ipc
from datetime import datetime
# spark = SparkSession.builder \
#     .appName("example") \
#     .config("spark.executor.memory", "4g") \
#     .config("spark.driver.memory", "4g") \
#     .getOrCreate()
# spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")
def genArrow():
    pdf1 = pd.DataFrame(np.random.rand(1000000, 3))
    BATCH_SIZE = 1000000
    pdf1.columns = [str(col) for col in pdf1.columns]

    NUM_BATCHES = 1

    schema = pa.schema([pa.field('0', pa.float64()),
                        pa.field('1', pa.float64()),
                        pa.field('2', pa.float64())])

    with pa.OSFile('bigfile.arrow', 'wb') as sink:
        with pa.ipc.new_file(sink, schema) as writer:
            # for row in range(NUM_BATCHES):
                batch = pa.record_batch(pdf1, schema)
                writer.write(batch)
# pdf2 = pd.DataFrame(np.random.rand(1000000, 3))# Let’s test the conversion of
# start_time = datetime.now()
# df1 = spark.createDataFrame(pdf1)
# end_time = datetime.now()
# print("Pandas to Spark(without Arrow): ", end_time - start_time)
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# start_time = datetime.now()
# df2 = spark.createDataFrame(pdf2)
# end_time = datetime.now()
# print("Pandas to Spark(with Arrow): ", end_time - start_time)
# start_time = datetime.now()
# arrow_table = pa.Table.from_pandas(df2.toPandas())
# with pa.OSFile('./arrow_file.arrow', 'wb') as sink:
#     with ipc.new_file(sink, arrow_table.schema) as writer:
#         writer.write_table(arrow_table)
# end_time = datetime.now()
# print("write as Arrow IPC: ", end_time - start_time)
# start_time = datetime.now()
# arrow_table = pa.Table.from_pandas(df2.toPandas())
# arrow_file_path = "./arrow_file.parquet"
# pq.write_table(arrow_table, arrow_file_path)
# end_time = datetime.now()
# print("write as Arrow parquet: ", end_time - start_time)
# start_time = datetime.now()
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
# df2.write.parquet('./parquet_file_3',mode="overwrite")
# end_time = datetime.now()
# print("write as DataFrame parquet: ", end_time - start_time)
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
# data = [(1, 'Alice'), (2, 'Bob'), (3, 'Cathy')]
# columns = ['id', 'name']
# df = spark.createDataFrame(data, columns)
# start_time = datetime.now()
# pdf1 = df.toPandas()
# end_time = datetime.now()
# print("Spark to Pandas(without Arrow): ", end_time - start_time)
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# start_time = datetime.now()
# pdf2 = df.toPandas()
# end_time = datetime.now()
# print("Spark to Pandas(without Arrow): ", end_time - start_time)
# df1 = spark.createDataFrame(pdf1)
# pdf1.to_csv('test.csv',index=False)

table = csv.read_csv('test.csv')
print(f'len(table):{len(table)}')
print("csv.read_csv(file)")
print("Now RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
# genArrow()
print("pa.OSFile(file)")
with pa.OSFile('arrow_file.arrow') as source:
    array = pa.ipc.open_file(source).read_all()
print("Now RSS: {}MB".format(pa.total_allocated_bytes() >> 20))

print("pa.memory_map(file)")
mm=pa.create_memory_map('example_mmap.dat', 10)
with pa.memory_map(r'arrow_file.arrow','rb') as source:
    array=pa.ipc.open_file(source).read_all()
print("Now RSS: {}MB".format(pa.total_allocated_bytes() >> 20))

