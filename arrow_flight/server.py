import sys
import os
import io

import datasets

sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')

from utils import Version
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
from SciDBLoader import load_schema, load_scidb_dataset
import ast

SCHEMA_TABLE = pa.schema([
    ('text', pa.binary()),
    ('image', pa.binary()),
    ('binary', pa.binary()),
    ('ext', pa.string()),
])

SCHEMA_DATASET = pa.schema([
    ('text', pa.list_(pa.binary())),
    ('image', pa.list_(pa.binary())),
    ('binary', pa.list_(pa.binary())),
    ('ext', pa.list_(pa.string())),
])


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

    def _make_flight_info(self, dataset):
        schema = SCHEMA_TABLE
        descriptor = pa.flight.FlightDescriptor.for_path(
            dataset.encode('utf-8')
        )
        endpoints = [pa.flight.FlightEndpoint(dataset, ["grpc://0.0.0.0:8815"])]
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

    def do_action(self, context, action):
        if action.type == "get_schema":
            # 创建示例 schema
            self.dataset = self.batch_size = self.dataset_type = None
            self.dataset_id = action.body.to_pybytes().decode('utf-8')
            self.schema, self.dir_structure = load_schema(self.dataset_id)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_file(sink, self.schema.schema) as writer:
                writer.write_table(self.schema)
            # 将 schema 序列化为 bytes
            return [sink.getvalue().to_pybytes()]
        elif action.type == "put_folder_path":
            self.folder_path = action.body.to_pybytes().decode('utf-8')
            # return []
        elif action.type == "streaming":
            if self.numerical_analysis:
                raise fl.FlightError(f"Processing error: You can't stream the dataset after setting numerical analysis")
            self.streaming = eval(action.body.to_pybytes().decode('utf-8'))
            # return []
        elif action.type == "batch_size":
            self.batch_size = int(action.body.to_pybytes().decode('utf-8'))
        elif action.type == "numerical_analysis":
            self.numerical_analysis = eval(action.body.to_pybytes().decode('utf-8'))
            self.dataset_type = 'num'
            if self.numerical_analysis:
                self.streaming = False
                self.get_dataset(self.dataset_type)
                result = self.analyze_num()
                return [result]
            # return []
        elif action.type == "recommendation_preprocess":
            self.preprocess = eval(action.body.to_pybytes().decode('utf-8'))
            self.dataset_type = 'num'
            if self.preprocess:
                self.recommendation_preprocess()
                return [pickle.dumps("Data Preprocessed!")]
            # return []
        elif action.type == "get_dataset_str":
            print("Getting dataset str table...")
            self.streaming = False
            self.dataset_type = 'str'
            self.get_dataset(self.dataset_type)
        elif action.type == "img_preprocess":
            pass
        else:
            raise NotImplementedError

    def do_get(self, context, ticket):
        # 这里假设 ticket 的内容是文件名和文件夹名
        dirs_string = self.folder_path
        # print(self.dataset_id)
        # 读取文件并构造 RecordBatch
        # 这里假设读取的文件内容符合 Arrow 的表格式

        if self.numerical_analysis:
            if self.streaming:
                return Exception
        # print(f"self.dataset: {self.dataset}")
        print(f"is self.dataset not None: {self.dataset is not None}")

        dataset = self.dataset

        def batch(ds: IterableDataset, batch_size: int, drop_last_batch: bool = False):

            def batch_fn(unbatched):
                return {k: [v] for k, v in unbatched.items()}

            return ds.map(batch_fn, batched=True, batch_size=batch_size, drop_last_batch=drop_last_batch)

        def _iterable_dataset_generator(ds, batch_size):
            return batch(ds, batch_size) if Version.IS_DATASETS_OUTDATED.value else ds.batch(batch_size)

        def generate_dataset_batches(batch_size, ds):
            # print(dataset)
            generator = _iterable_dataset_generator(ds, batch_size)
            # print(generator)
            for example in iter(generator):
                # print(example)
                table = pa.table(example)
                yield table

        def generate_table_batches(batch_size, ds):
            num_rows = ds.num_rows
            print(f"num_rows: {num_rows}")
            for start in range(0, num_rows, batch_size):
                end = min(start + batch_size, num_rows)
                yield ds.slice(start, end - start)

        def merge_dict_to_table(it_ds):
            merged_dict={}
            for example in it_ds:
                for key, value in example.items():
                    if key not in merged_dict:
                        merged_dict[key] = []  # 初始化为列表
                    merged_dict[key].append(value)
            return pa.table(merged_dict)



        def is_iter_batch(ds):
            if self.batch_size is not None:
                # print(ds)
                # 按需供给
                if not isinstance(ds, pyarrow.Table):
                    if not self.streaming:
                        return fl.GeneratorStream(SCHEMA_TABLE,
                                                  generate_table_batches(self.batch_size, ds['train'].data.table if Version.IS_DATASETS_OUTDATED.value else ds.data.table))
                    return fl.GeneratorStream(SCHEMA_TABLE, generate_dataset_batches(self.batch_size, ds))
                else:
                    return fl.GeneratorStream(ds.schema, generate_table_batches(self.batch_size, ds))

            else:
                # 返回 RecordBatchStream
                # 一次性供给
                if not isinstance(ds, pyarrow.Table):
                    if isinstance(ds, IterableDataset):
                        return fl.RecordBatchStream(merge_dict_to_table(ds))
                    return fl.RecordBatchStream(ds['train'].data.table if Version.IS_DATASETS_OUTDATED.value else ds.data.table)
                else:
                    print("一次性供给Table")
                    return fl.RecordBatchStream(ds)

        if dataset is not None:
            if self.dataset_type == 'num':
                return is_iter_batch(self.nparray_to_table(self.dataset))
            return is_iter_batch(self.dataset)

        dataset = load_scidb_dataset(
            self.dir_structure, dirs_string, streaming=not self.numerical_analysis and self.streaming)
        # print(dataset)
        return is_iter_batch(dataset)

        # print(dataset)

    def action_bool(self, action):
        return eval(action.body.to_pybytes().decode('utf-8'))

    def get_dataset(self, type):
        dataset = load_scidb_dataset(self.dir_structure, self.folder_path, streaming=self.streaming)
        dataset_table = None
        # print(dataset)
        if type == 'num' or type == 'str':
            table=dataset['train'].data.table if Version.IS_DATASETS_OUTDATED.value else dataset.data.table
            text_binary = table['text'][0].as_py()
            # print(text_binary)
            # print(text_binary)
            # print(text_binary)
            # print(f'is instance bytes: {isinstance(text_binary, pa.binary)}')
            dataset_table = read_csv(io.BytesIO(text_binary), read_options=ReadOptions(encoding=char_det(text_binary)))
            print(dataset_table)
            if type == 'num':
                df = dataset_table.to_pandas()

                # 将 Pandas MyDataFrame 转换为 NumPy 数组
                dataset_table = df.to_numpy()
                print(dataset_table)

            # print(dataset_table)
            # print(df['text'][0][0])
            # def convert_to_numeric_matrix(data):
            #     # 对每一行中的每个字符串解析成数值列表
            #     numeric_matrix = np.array([[float(value) for value in row] for row in reader])
            #     return numeric_matrix
            #
            # def convert_to_string_matrix(data):
            #     # print(isinstance(data,list))
            #     # print(type(data[0]))
            #     # print(data[1])
            #     string_matrix = np.array([[value for value in row] for row in reader])
            #     # string_matrix=np.array(string_matrix)
            #     # print(string_matrix[0])
            #     # string_matrix = np.array([row.split(',') for row in data])
            #     # print(string_matrix)
            #     return string_matrix

            # if type=='num':
            #     np_data_matrix = convert_to_numeric_matrix(reader)
            # else:
            #     np_data_matrix = convert_to_string_matrix(reader)

        self.dataset = dataset_table
        # print(self.dataset)

    def analyze_num(self):
        np_data_matrix = self.dataset
        column_stats = {
            "row": np.array([np_data_matrix.shape[0]]),
            "col": np.array([np_data_matrix.shape[1]]),
            "max": np.array([np.max(np_data_matrix, axis=0)]),
            "min": np.array([np.min(np_data_matrix, axis=0)]),
            "mean": np.array([np.mean(np_data_matrix, axis=0)]),
            "median": np.array([np.median(np_data_matrix, axis=0)]),
            "variance": np.array([np.var(np_data_matrix, axis=0)])
        }
        print(column_stats)
        return pickle.dumps(column_stats)

    def nparray_to_table(self, nparray):
        # 生成新的 schema，每列顺序命名并指定类型为 float64
        num_columns = nparray.shape[1]
        fields = [pa.field(f"column{i + 1}", pa.int64()) for i in range(num_columns)]
        new_schema = pa.schema(fields)

        # 将 NumPy 数组的每一列转为 PyArrow Array，并生成 PyArrow Table
        arrays = [pa.array(nparray[:, i], type=pa.int64()) for i in range(num_columns)]
        restored_table = pa.Table.from_arrays(arrays, schema=new_schema)
        return restored_table

    def recommendation_preprocess(self):
        if not isinstance(self.dataset, np.ndarray):
            print("Haven't downloaded dataset, downloading right now...")
            self.get_dataset(self.dataset_type)
        # 1. 填充缺失值
        data_filled = np.copy(self.dataset)
        data_filled[:, 0] = np.nan_to_num(data_filled[:, 0], nan=-1)  # 用户ID填充为-1
        data_filled[:, 1] = np.nan_to_num(data_filled[:, 1], nan=-1)  # 产品ID填充为-1
        data_filled[:, 2] = np.nan_to_num(data_filled[:, 2], nan=0)  # 交互值填充为0

        print("填充缺失值后的数据：")
        print(data_filled)

        # 2. 去重操作
        data_unique = np.unique(data_filled, axis=0)
        print("\n去重后的数据：")
        print(data_unique)

        # 3. 归一化操作
        interaction_values = data_unique[:, 2].astype(float)  # 交互值列
        interaction_min = interaction_values.min()
        interaction_max = interaction_values.max()

        # Min-Max 归一化
        interaction_values_normalized = (interaction_values - interaction_min) / (interaction_max - interaction_min)
        data_normalized = np.copy(data_unique)
        data_normalized[:, 2] = interaction_values_normalized

        print("\n归一化后的数据：")
        print(data_normalized)
        self.dataset = data_normalized


if __name__ == "__main__":
    server = MyFlightServer('grpc://127.0.0.1:8815')
    server.serve()

