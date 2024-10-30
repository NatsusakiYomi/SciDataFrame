import sys
# sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')
import pyarrow as pa
from random import random

from pyarrow.dataset import dataset

# sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')
import pyarrow
import pyarrow.flight as fl
from model import DataFrame
import pickle
from training_scripts import *
from utils import TrainingTask
import time
# server = grpc.server(..., options=[('grpc.so_reuseport', 1)])
# server.add_insecure_port('[::]:8815')  # 监听所有接口，包括 IPv6
import numpy as np


# 创建 FlightClient 实例

# upload_descriptor = pa.flight.FlightDescriptor.for_path("new.txt")
# flight = client.get_flight_info(upload_descriptor)
# descriptor = flight.descriptor
def main(dataset_id, folder_path, is_analyze, is_preprocess, is_get_dataset_str, is_streaming, task, batch_size, is_batch_cache):
    start_train = False
    while not start_train:
        client = fl.connect("grpc://localhost:8815")

        # 获取 schema
        action = fl.Action("get_schema", dataset_id.encode("utf-8"))
        client.do_action(action)
        schema_results = client.do_action(action)
        schema = None
        # 处理返回的 schema
        for schema_bytes in schema_results:
            with pyarrow.ipc.open_file(schema_bytes.body) as schema_file:
                schema = schema_file.read_all()
                print("Received Schema:", schema)
                print("Received bytes:", schema.nbytes)
            # 重建文件目录树

        if folder_path is None:
            folder_path = input("Enter folder path to retrieve files: ")

        # 用户输入文件夹路径
        ticket = fl.Ticket(folder_path.encode('utf-8'))
        action = fl.Action("put_folder_path", folder_path.encode("utf-8"))
        client.do_action(action)
        df = DataFrame(schema=schema)

        # 进行数值特征分析
        if is_analyze:
            action = fl.Action("numerical_analysis", "True".encode("utf-8"))
            # client.do_action(action)
            results = client.do_action(action)
            result = None
            # 处理返回的result
            for result_bytes in results:
                numerical_feature = pickle.loads(result_bytes.body)
                print("Received Numerical features:", numerical_feature)
                print("Received bytes:", result_bytes.body.size)
        else:
            action = fl.Action("numerical_analysis", "False".encode("utf-8"))
            results = client.do_action(action)
        # 进行数据预处理
        if is_preprocess:
            action = fl.Action("recommendation_preprocess", "True".encode("utf-8"))
            results = client.do_action(action)
            result = None
            # 处理返回的result
            for result_bytes in results:
                result = pickle.loads(result_bytes.body)
                print("Received Message:", result)
                print("Received bytes:", result_bytes.body.size)
        else:
            action = fl.Action("recommendation_preprocess", "False".encode("utf-8"))
            results = client.do_action(action)
        # 进行streaming
        if is_get_dataset_str:
            action = fl.Action("get_dataset_str", "True".encode("utf-8"))
            results = client.do_action(action)
        else:
            action = fl.Action("get_dataset_str", "False".encode("utf-8"))
            results = client.do_action(action)

        if batch_size is not None:
            action = fl.Action("batch_size", str(batch_size).encode("utf-8"))
            results = client.do_action(action)

        if is_streaming:
            action = fl.Action("streaming", "True".encode("utf-8"))
            client.do_action(action, pyarrow.flight.FlightCallOptions(timeout=60))
        else:
            action = fl.Action("streaming", "False".encode("utf-8"))
            client.do_action(action, pyarrow.flight.FlightCallOptions(timeout=60))
        download_start=time.time()
        if batch_size is None:
            reader = client.do_get(ticket)
            read_table = reader.read_all()
            df.concat(read_table)
            print(f'{read_table.num_rows} rows received: {read_table.nbytes} bytes')
        else:
            if is_batch_cache:
                action = fl.Action("batch_cache", "True".encode("utf-8"))
                results = client.do_action(action)
            reader = client.do_get(ticket)
            for index, chunk in enumerate(reader):
                print(f'Row {index*batch_size + 1} to {(index+1)*batch_size} received: {chunk.data.nbytes} bytes')
                # time.sleep(10)
                df.concat(pyarrow.Table.from_batches([chunk.data]))
            print(f'{df.data.num_rows} rows received: {df.data.nbytes} bytes')
        download_end=time.time()
        client.close()
        print(f'download time: {download_end-download_start}')
        start_train = input("Start train? ").lower() == 'yes'

    try:
        if task == TrainingTask.Recommendation:
            RecommendationModel(df)
        elif task == TrainingTask.MultiLabelClassification:
            BertMultiClassification(df)
        elif task == TrainingTask.ImageClassification:
            ImageClassification(df)
        else:
            raise NotImplementedError(f"Task {task} not implemented")
    except NotImplementedError as e:
        print(e)




if __name__ == '__main__':
    dataset_id = 'images_test' + ".txt"
    dataset_path = None
    # dataset_path = input("Enter folder path to retrieve files: ")
    # dataset_id = input("Enter dataset id: ") + ".txt"
    # is_analyze = input("Analyze num or not: ").lower() == 'yes'
    # is_preprocess = input("Preprocess or not: ").lower() == 'yes'
    # is_get_dataset_str = input("Get dataset as str table or not: ").lower() == 'yes'
    # is_streaming = input("Streaming or not: ").lower() == 'yes'
    is_analyze = False
    is_preprocess = False
    is_get_dataset_str = False
    is_streaming = False
    task = TrainingTask.ImageClassification
    batch_size = 1
    is_batch_cache = False
    main(dataset_id, dataset_path, is_analyze, is_preprocess, is_get_dataset_str, is_streaming, task, batch_size, is_batch_cache)
