import uuid

import pandas as pd

from utils.Check import Level
import pyarrow as pa
from pyarrow import flight as fl
from arrow_flight import Client

SCHEMA = pa.schema([
    pa.field('text', pa.binary()),
    pa.field('image', pa.binary()),
    pa.field('binary', pa.binary())],
)


class MyDataFrame:

    def __init__(self, dataset_id=None, schema=None, nbytes=None, level=Level.FOLDER, data=None, client=None, address="127.0.0.1", port=8815, **kwargs):
        self.id = uuid.uuid4()
        self.schema = schema
        self.nbytes = nbytes
        self.data = data
        self.batch_size = kwargs.get('batch_size', None)
        self.client = Client(address,port) if client is None \
            else client
        self.counter = 0
        self.reader = None
        self.level = level
        self.dataset_id = dataset_id
        self.is_iterate =kwargs.get('is_iterate', None)
        self.load_kwargs = kwargs

    def to_pandas(self):
        try:
            return self.data.to_pandas()
        except:
            print("Data can't be converted to pandas dataframe.")

    def to_numpy(self):
        try:
            return self.to_pandas().to_numpy()
        except:
            print("Data can't be converted to numpy array.")

    def data_concat(self, obj):
        self.data = pa.concat_tables([self.data, obj]) if self.data else obj

    def _filter_depth_rows(self, df, index):
        # 获取给定索引行的 'depth' 值
        target_depth = df.at[index, 'depth']
        # 筛选出从给定索引开始的所有行
        filtered_rows = [index]
        for i in range(index + 1, len(df)):
            if df.at[i, 'depth'] > target_depth:
                filtered_rows.append(i)
            else:
                break  # 碰到第一个不超过目标 depth 的行时停止
        return df.loc[filtered_rows]

    def get_slice_from_name(self, name):
        try:
            slice = self.schema[self.schema["name"] == name]
            return slice
        except IndexError as e:
            print("Dirs/files not found, please retry.")
            return self

    def get_level_from_slice(self, slice):
        return Level(slice.iloc[0]['type'])

    def get_index_from_slice(self,slice):
        return slice.index[0]

    def is_paths_list_a_file(self, paths_list):
        return Level.FILE if (len(paths_list)==1
                              and self.get_level_from_slice(
                    self.get_slice_from_name(paths_list[0])) is
                              Level.FILE) else Level.FOLDER

    def _get_all_paths(self):
        paths_list=self.schema['name'].tolist()
        return ','.join(paths_list),self.is_paths_list_a_file(paths_list)

    def filter(self, pattern):
        filtered_schema = self.schema[self.schema['name'].str.contains(pattern, regex=True)]
        df = MyDataFrame(dataset_id=self.dataset_id, schema=filtered_schema,
                         level=Level.FOLDER,
                         client=self.client,
                         **self.load_kwargs)
        return df


    def open(self, name):
        slice = self.get_slice_from_name(name)
        level = self.get_level_from_slice(slice)
        index = self.get_index_from_slice(slice)
        df = MyDataFrame(dataset_id=self.dataset_id,schema=self._filter_depth_rows(self.schema, index)
        if level == Level.FOLDER else slice,
                           level=level,
                           client=self.client,
                         **self.load_kwargs)
        if level == Level.FILE:
            df.flat_open(name)
        return df


    def get_schema(self):
        self.schema = self.client.get_schema(self.dataset_id).to_pandas()
        return self.schema

    def flat_open(self, paths=None):
        try:

            print("file level:", self.level)
            self.client.load_init(**self.load_kwargs)
            self.is_iterate = self.load_kwargs.get('is_iterate', None)
            paths,level = self._get_all_paths() if paths is None else paths,self.is_paths_list_a_file(paths.split(","))
            action = fl.Action("put_folder_path", paths.encode("utf-8"))
            print("file level:",level)
            self.client.fl_client.do_action(action)
            self.reader = self.client.flat_open(level)
            if not self.is_iterate:
                    self.data=self.reader
        # self.client.close()
        except IndexError as e:
            print("Dirs/files not found, please retry.")
            return self

    def iter_to_instance(self):
        for example in self.reader:
            self.data_concat(pa.Table.from_batches([example.data]))

    def to_iterator(self):
        return iter(self)

    def __iter__(self):
        if self.is_iterate:
            for batch in self.reader:
                # print(batch.data.schema)
                self.data_concat(pa.Table.from_batches([batch.data]))
                print(
                    f'Row {self.counter * self.batch_size + 1} to {(self.counter + 1) * self.batch_size} received: {batch.data.nbytes} bytes')
                yield MyDataFrame(dataset_id=self.dataset_id,data=pa.Table.from_batches([batch.data]),client=self.client)
                self.counter += 1
        else:
            raise NotImplementedError('Batch size not implemented yet')
