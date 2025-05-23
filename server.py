import sys
import os
import io

import datasets



# sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')

# from utils import Version, filter_url_from_index, Parser
# from utils.Parser import *

import pyarrow as pa
import pyarrow
from pyarrow.csv import read_csv, ReadOptions
import pyarrow.flight as fl
import pandas as pd
import numpy as np
import pickle
import chardet
import csv
from datasets import IterableDataset
# from SciDBLoader import load_schema, load_scidb_dataset
# import ast
#
SCHEMA_TABLE = pa.schema([
    ('text', pa.binary()),
    ('image', pa.binary()),
    ('binary', pa.binary()),
    ('ext', pa.string()),
])
#
# SCHEMA_DATASET = pa.schema([
#     ('text', pa.list_(pa.binary())),
#     ('image', pa.list_(pa.binary())),
#     ('binary', pa.list_(pa.binary())),
#     ('ext', pa.list_(pa.string())),
# ])


def char_det(file_binary, num_bytes=1024):
    file_size = len(file_binary)

    # 如果文件小于 max_bytes，读取整个文件，否则读取 max_bytes 字节
    read_size = min(file_size, num_bytes)
    result = chardet.detect(file_binary)
    encoding = result['encoding']
    print(f'Read with {read_size} file is encoded with {encoding}')
    return encoding


class MyFlightServer(fl.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self.dir_structure = None
        self.streaming = False
        self.numerical_analysis = False
        self.preprocess = False
        self.dataset_id = None
        self.folder_path = None
        # np.array格式的数据集
        self.dataset = None
        self.schema = None
        self.dataset_type = None
        # 如果是大于等于1的整型，则按需供给，否则一次性供给
        self.batch_size = None
        self.location=location

    def _make_flight_info(self, dataset):
        schema = SCHEMA_TABLE
        descriptor = pa.flight.FlightDescriptor.for_path(
            dataset.encode('utf-8')
        )
        endpoints = [pa.flight.FlightEndpoint(dataset, [self.location])]
        return pa.flight.FlightInfo(schema,
                                    descriptor,
                                    endpoints,
                                    -1,
                                    -1)

    def get_flight_info(self, context, descriptor):
        return self._make_flight_info(descriptor.path[0].decode('utf-8'))

    def list_actions(self, context):
        # TODO:描述可用操作
        pass
        # return [
        #     ("drop_dataset", "Delete a dataset."),
        # ]

    # def do_action(self, context, action):
    #
    #     else:
    #         raise NotImplementedError

    # def do_put(self, context, descriptor, reader, writer):


    def do_get(self, context, ticket):
        # 这里假设 ticket 的内容是文件名和文件夹名
        from ipc import arrowIPC
        table=arrowIPC()
        return fl.RecordBatchStream(table)

        # print(dataset)

if __name__ == "__main__":
    server = MyFlightServer('grpc://127.0.0.1:8815')
    server.serve()
