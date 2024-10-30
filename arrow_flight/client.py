import sys
# sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')
import pyarrow as pa
from random import random

from pyarrow.dataset import dataset

# sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')
import pyarrow
import pyarrow.flight as fl
from torch.package import analyze

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
class Client():
    def __init__(self, dataset_id, folder_path, is_analyze, is_preprocess, is_get_dataset_str, is_streaming, task,
                 batch_size):
        start_train = False
        self.is_analyze = is_analyze
        self.is_preprocess = is_preprocess
        self.is_get_dataset_str = is_get_dataset_str
        self.is_streaming = is_streaming
        self.dataset_id = dataset_id
        self.task = task
        self.batch_size = batch_size
        self.fl_client = fl.connect("grpc://localhost:8815")
        self.folder_path = folder_path

    def get_schema(self):
        # 获取 schema
        action = fl.Action("get_schema", self.dataset_id.encode("utf-8"))
        self.fl_client.do_action(action)
        schema_results = self.fl_client.do_action(action)
        schema = None
        # 处理返回的 schema
        for schema_bytes in schema_results:
            with pyarrow.ipc.open_file(schema_bytes.body) as schema_file:
                schema = schema_file.read_all()
                print("Received Schema:", schema)
                print("Received bytes:", schema.nbytes)
        return schema
            # 重建文件目录树

    def load(self):
        # 进行数值特征分析
        ticket = fl.Ticket("".encode("utf-8"))
        if self.is_analyze:
            action = fl.Action("numerical_analysis", "True".encode("utf-8"))
            # self.client.do_action(action)
            results = self.fl_client.do_action(action)
            result = None
            # 处理返回的result
            for result_bytes in results:
                numerical_feature = pickle.loads(result_bytes.body)
                print("Received Numerical features:", numerical_feature)
                print("Received bytes:", result_bytes.body.size)
        else:
            action = fl.Action("numerical_analysis", "False".encode("utf-8"))
            results = self.fl_client.do_action(action)

        # 进行数据预处理
        if self.is_preprocess:
            action = fl.Action("recommendation_preprocess", "True".encode("utf-8"))
            results = self.fl_client.do_action(action)
            result = None
            # 处理返回的result
            for result_bytes in results:
                result = pickle.loads(result_bytes.body)
                print("Received Message:", result)
                print("Received bytes:", result_bytes.body.size)
        else:
            action = fl.Action("recommendation_preprocess", "False".encode("utf-8"))
            results = self.fl_client.do_action(action)

        # 进行streaming
        if self.is_get_dataset_str:
            action = fl.Action("get_dataset_str", "True".encode("utf-8"))
            results = self.fl_client.do_action(action)

        if self.batch_size is not None:
            action = fl.Action("batch_size", str(self.batch_size).encode("utf-8"))
            results = self.fl_client.do_action(action)

        if self.is_streaming:
            action = fl.Action("streaming", "True".encode("utf-8"))
            self.fl_client.do_action(action, pyarrow.flight.FlightCallOptions(timeout=60))
        else:
            # 使用 do_get 获取文件数据
            action = fl.Action("streaming", "False".encode("utf-8"))
            self.fl_client.do_action(action, pyarrow.flight.FlightCallOptions(timeout=60))

        reader = self.fl_client.do_get(ticket)
        if self.batch_size is None:
            reader = reader.read_all()
        return reader


if __name__ == '__main__':
    dataset_id = 'new' + ".txt"
    dataset_path = None
    # dataset_path = input("Enter folder path to retrieve files: ")
    # dataset_id = input("Enter dataset id: ") + ".txt"
    # is_analyze = input("Analyze num or not: ").lower() == 'yes'
    # is_preprocess = input("Preprocess or not: ").lower() == 'yes'
    # is_get_dataset_str = input("Get dataset as str table or not: ").lower() == 'yes'
    # is_streaming = input("Streaming or not: ").lower() == 'yes'
    is_analyze = True
    is_preprocess = True
    is_get_dataset_str = False
    is_streaming = True
    task = TrainingTask.Recommendation
    batch_size = 100
    kwargs={
        "dataset_id": dataset_id,
        "folder_path": dataset_path,
        "is_analyze": is_analyze,
        "is_preprocess": is_preprocess,
        "is_get_dataset_str": is_get_dataset_str,
        "is_streaming": is_streaming,
        "task": task,
        "batch_size": batch_size,
    }
    df = DataFrame(**kwargs)
    df.get_schema()
    df.open("数据集")
    dataset_path = input("Enter folder path to retrieve files: ")
    df.filter(dataset_path)
    df.load()
    if batch_size is not None:
        df.iter_to_instance()
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
