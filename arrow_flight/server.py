import pyarrow as pa
import pyarrow.flight as fl
import pandas as pd
import numpy as np
import pickle

from pandas.core.internals.construction import arrays_to_mgr

from SciDBLoader import load_schema, load_scidb_dataset
import ast

SCHEMA = pa.schema([
    ('text', pa.string()),
    ('image', pa.binary()),
    ('binary', pa.binary()),
])


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

    def _make_flight_info(self, dataset):
        # TODO:每次请求创建一个flight_info
        pass

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
            self.dataset_id = action.body.to_pybytes().decode('utf-8')
            schema, self.dir_structure = load_schema(self.dataset_id)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_file(sink, schema.schema) as writer:
                writer.write_table(schema)
            # 将 schema 序列化为 bytes
            return [sink.getvalue().to_pybytes()]
        elif action.type == "put_folder_path":
            self.folder_path = action.body.to_pybytes().decode('utf-8')
            return []
        elif action.type == "streaming":
            if self.numerical_analysis:
                raise fl.FlightError(f"Processing error: You can't stream the dataset after setting numerical analysis")
            self.streaming = eval(action.body.to_pybytes().decode('utf-8'))
            print(self.streaming)
            print(f"is bool? {isinstance(self.streaming, bool)}")
        elif action.type == "numerical_analysis":
            self.streaming, self.numerical_analysis = False, True
            self.folder_path = action.body.to_pybytes().decode('utf-8')
            self.get_dataset()
            result = self.analyze_num()
            return [result]
        elif action.type == "recommendation_preprocess":
            self.preprocess = True
            self.recommendation_preprocess()
            return [pickle.dumps("Data Preprocessed!")]
        else:
            raise NotImplementedError

    def do_get(self, context, ticket):
        # 这里假设 ticket 的内容是文件名和文件夹名
        dirs_string = ticket.ticket.decode('utf-8')

        # 读取文件并构造 RecordBatch
        # 这里假设读取的文件内容符合 Arrow 的表格式
        dataset = load_scidb_dataset(
            self.dir_structure, dirs_string, streaming=not self.numerical_analysis and self.streaming)

        if self.numerical_analysis:
            if self.streaming:
                return Exception

        if self.preprocess:
            return fl.RecordBatchStream(self.nparray_to_table(self.dataset))

        def generate_batches():
            for example in iter(dataset):
                table = pa.table(example)
                yield table

        # print(dataset)
        if self.streaming:
            return fl.GeneratorStream(SCHEMA, generate_batches())
        else:
            # 返回 RecordBatchStream
            return fl.RecordBatchStream(dataset['train'].data.table)

    def action_bool(self, action):
        return eval(action.body.to_pybytes().decode('utf-8'))

    def get_dataset(self):
        dataset = load_scidb_dataset(self.dir_structure, self.folder_path, streaming=self.streaming)
        df = dataset['train'].data.table.to_pandas()
        # print(df['text'][0][0])

        def convert_to_numeric_matrix(data):
            # 对每一行中的每个字符串解析成数值列表
            # print(data)
            numeric_matrix = np.array([np.fromstring(item, sep=',', dtype=np.float64) for item in data])
            return numeric_matrix

        np_data_matrix = convert_to_numeric_matrix(ast.literal_eval(df['text'][0][0]))
        self.dataset = np_data_matrix

    def analyze_num(self):
        np_data_matrix=self.dataset
        column_stats = {
            "row": np_data_matrix.shape[0],
            "col": np_data_matrix.shape[1],
            "max": np.max(np_data_matrix, axis=0),
            "min": np.min(np_data_matrix, axis=0),
            "mean": np.mean(np_data_matrix, axis=0),
            "median": np.median(np_data_matrix, axis=0),
            "variance": np.var(np_data_matrix, axis=0)
        }
        return pickle.dumps(column_stats)

    def nparray_to_table(self,nparray):
        # 生成新的 schema，每列顺序命名并指定类型为 float64
        num_columns = nparray.shape[1]
        fields = [pa.field(f"column{i + 1}", pa.float64()) for i in range(num_columns)]
        new_schema = pa.schema(fields)

        # 将 NumPy 数组的每一列转为 PyArrow Array，并生成 PyArrow Table
        arrays = [pa.array(nparray[:, i], type=pa.float64()) for i in range(num_columns)]
        restored_table = pa.Table.from_arrays(arrays, schema=new_schema)
        return restored_table

    def recommendation_preprocess(self):
        if isinstance(self.dataset, np.ndarray):
            print("Haven't downloaded dataset, downloading right now...")
            self.get_dataset()
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
        self.dataset=data_normalized


if __name__ == "__main__":
    server = MyFlightServer(('0.0.0.0', 8815))
    server.serve()
