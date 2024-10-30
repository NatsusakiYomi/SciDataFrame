import uuid
from utils.Check import Level
import pyarrow as pa
from pyarrow import flight as fl
from arrow_flight import Client

SCHEMA = pa.schema([
    pa.field('text', pa.string()),
    pa.field('image', pa.binary()),
    pa.field('binary', pa.binary())],
)


class DataFrame:

    def __init__(self, schema=None, nbytes=None, data=None, level=Level.FOLDER,**kwargs):
        self.id = uuid.uuid4()
        self.schema = schema
        self.nbytes = nbytes
        self.data = data
        self.batch_size = kwargs.get('batch_size', None)
        self.client = Client(**kwargs) if len(kwargs) != 0 else None
        self.counter = 0
        self.reader = None
        self.level = level

    def concat(self, obj):
        self.data = pa.concat_tables([self.data, obj]) if self.data else obj

    def _filter_depth_rows(self,df, index):
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

    def open(self, name):
        slice=self.schema[self.schema["name"] == name]
        level= Level(slice.iloc[0]['type'])
        index = slice.index[0]
        if level==Level.FOLDER:
            return DataFrame(schema=self._filter_depth_rows(self.schema, index))
        else :
            pass


    def get_schema(self):
        self.schema = self.client.get_schema().to_pandas()
        return self.schema

    def filter(self, paths):
        action = fl.Action("put_folder_path", paths.encode("utf-8"))
        self.client.fl_client.do_action(action)
        return "Files Filtered!"

    def load(self):
        self.reader = self.client.load()
        if self.batch_size is None:
            self.concat(pa.Table.from_batches([self.reader.data]))

    def iter_to_instance(self):
        for example in self.reader:
            self.concat(pa.Table.from_batches([example.data]))

    def __iter__(self):
        if self.batch_size is not None:
            self.counter += 1
            data = next(self.reader).data
            self.concat(pa.Table.from_batches([data]))
            print(
                f'Row {self.counter * self.batch_size + 1} to {(self.counter + 1) * self.batch_size} received: {data.nbytes} bytes')
            yield DataFrame(data=pa.Table.from_batches([data]))
        else:
            raise NotImplementedError('Batch size not implemented yet')
