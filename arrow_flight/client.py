import sys
# sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')
import pyarrow as pa
from random import random

from pyarrow.dataset import dataset

# sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')
import pyarrow
import pyarrow.flight as fl
from torch.package import analyze

# from model import MyDataFrame
import pickle
from training_scripts import *
from utils import TrainingTask, Level, Source
import time
# server = grpc.server(..., options=[('grpc.so_reuseport', 1)])
# server.add_insecure_port('[::]:8815')  # 监听所有接口，包括 IPv6
import numpy as np


# 创建 FlightClient 实例

# upload_descriptor = pa.flight.FlightDescriptor.for_path("new.txt")
# flight = client.get_flight_info(upload_descriptor)
# descriptor = flight.descriptor
class Client():
    def __init__(self,address="127.0.0.1",port=8815):
        start_train = False

        self.fl_client = fl.connect("grpc://"+address+":"+str(port))


    def load_init(self,  is_analyze, is_preprocess, is_get_dataset_str, is_streaming, task,
                 is_iterate,batch_size=1):
        # self.folder_path = folder_path
        self.is_analyze = is_analyze
        self.is_preprocess = is_preprocess
        self.is_get_dataset_str = is_get_dataset_str
        self.is_streaming = is_streaming
        # self.dataset_id = dataset_id
        self.task = task
        self.batch_size = batch_size
        self.is_iterate = is_iterate

    def get_schema(self,dataset_id):
        # 获取 schema
        self.dataset_id = dataset_id
        action = fl.Action("get_schema", self.dataset_id.encode("utf-8"))
        self.fl_client.do_action(action)
        schema_results = self.fl_client.do_action(action,pyarrow.flight.FlightCallOptions(timeout=60))
        schema = None
        # 处理返回的 schema
        for schema_bytes in schema_results:
            with pyarrow.ipc.open_file(schema_bytes.body) as schema_file:
                schema = schema_file.read_all()
                print("Received Schema:", schema)
                print("Received bytes:", schema.nbytes)
        return schema
            # 重建文件目录树

    def flat_open(self,level=Level.FOLDER):
        # 进行数值特征分析
        ticket = fl.Ticket("".encode("utf-8"))
        if level==Level.FILE:

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

            if self.is_get_dataset_str:
                action = fl.Action("get_dataset_str", "True".encode("utf-8"))
                results = self.fl_client.do_action(action)

            action = fl.Action("parse_open", "True".encode("utf-8"))
            results = self.fl_client.do_action(action)

        if self.is_iterate:
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
        if not self.is_iterate:
            reader = reader.read_all()
        print(reader)
        return reader

    def close(self):
        self.fl_client.close()


if __name__ == '__main__':
    pass
    # dataset_id='images_test.txt'
    # dataset_path = None
    # is_analyze = True
    # is_preprocess = True
    # is_get_dataset_str = False
    # is_streaming = True
    # task = TrainingTask.Recommendation
    # batch_size = 1
    # kwargs={
    #     # "dataset_id": dataset_id,
    #     # "folder_path": dataset_path,
    #     "is_analyze": is_analyze,
    #     "is_preprocess": is_preprocess,
    #     "is_get_dataset_str": is_get_dataset_str,
    #     "is_streaming": is_streaming,
    #     "task": task,
    #     "batch_size": batch_size,
    # }
    # #根据配置新建DataFrame
    # df = MyDataFrame()
    # #获得数据集schema
    # schema=df.get_schema(dataset_id)
    # #打开文件（夹）
    # df=df.open("out")
    # dataset_path = input("Enter folder path to retrieve files: ")
    # df=df.open(dataset_path)
    # #加载数据
    # df.load(**kwargs)
    # #合并数据迭代器
    # if batch_size is not None:
    #     df.iter_to_instance()
    # start_train = input("Start train? ").lower() == 'yes'
    # try:
    #     if task == TrainingTask.Recommendation:
    #         RecommendationModel(df)
    #     elif task == TrainingTask.MultiLabelClassification:
    #         BertMultiClassification(df)
    #     elif task == TrainingTask.ImageClassification:
    #         ImageClassification(df)
    #     else:
    #         raise NotImplementedError(f"Task {task} not implemented")
    # except NotImplementedError as e:
    #     print(e)
