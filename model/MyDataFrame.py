import uuid

from pyarrow.jvm import schema

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

    def concat(self, obj):
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

    def _get_all_paths(self):
        return ','.join(self.schema['name'].tolist())

    def filter(self, pattern):
        filtered_schema = self.schema[self.schema['name'].str.contains(pattern, regex=True)]
        df = MyDataFrame(dataset_id=self.dataset_id, schema=filtered_schema,
                         level=Level.FOLDER,
                         client=self.client,
                         **self.load_kwargs)
        return df


    def open(self, name):
        slice = self.schema[self.schema["name"] == name]
        try:
            level = Level(slice.iloc[0]['type'])
            index = slice.index[0]
            df = MyDataFrame(dataset_id=self.dataset_id,schema=self._filter_depth_rows(self.schema, index)
            if level == Level.FOLDER else slice,
                               level=level,
                               client=self.client,
                             **self.load_kwargs)
            if level == Level.FILE:
                df.flat_open(name)
            return df
        except IndexError as e:
            print("Dirs/files not found, please retry.")
            return self

    def get_schema(self):
        self.schema = self.client.get_schema(self.dataset_id).to_pandas()
        return self.schema

    def flat_open(self, paths=None):
        self.client.load_init(**self.load_kwargs)
        self.is_iterate = self.load_kwargs.get('is_iterate', None)
        paths = self._get_all_paths() if paths is None else paths
        action = fl.Action("put_folder_path", paths.encode("utf-8"))
        self.client.fl_client.do_action(action)
        self.reader = self.client.flat_open(self.level)
        if not self.is_iterate:
                self.concat(self.reader)
        self.client.close()

    def iter_to_instance(self):
        for example in self.reader:
            self.concat(pa.Table.from_batches([example.data]))

    def to_iterator(self):
        return iter(self)

    def __iter__(self):
        if self.is_iterate:
            for batch in self.reader:
                # print(batch.data.schema)
                self.concat(pa.Table.from_batches([batch.data]))
                print(
                    f'Row {self.counter * self.batch_size + 1} to {(self.counter + 1) * self.batch_size} received: {batch.data.nbytes} bytes')
                yield MyDataFrame(dataset_id=self.dataset_id,data=pa.Table.from_batches([batch.data]),client=self.client)
                self.counter += 1
        else:
            raise NotImplementedError('Batch size not implemented yet')
