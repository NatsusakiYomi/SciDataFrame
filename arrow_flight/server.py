import pyarrow as pa
import pyarrow.flight as fl

from SciDBLoader import load_schema, load_scidb_dataset

SCHEMA=pa.schema([
    ('text', pa.string()),
    ('image', pa.binary()),
    ('binary', pa.binary()),
])

class MyFlightServer(fl.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self.dir_structure = None
        self.streaming = False

    def do_action(self, context, action):
        if action.type == "get_schema":
            # 创建示例 schema
            dataset_id = action.body.to_pybytes().decode('utf-8')
            schema, self.dir_structure = load_schema(dataset_id)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_file(sink, schema.schema) as writer:
                writer.write_table(schema)
            # 将 schema 序列化为 bytes

            return [sink.getvalue().to_pybytes()]
        elif action.type == "streaming":
            self.streaming = True

        return []

    def do_get(self, context, ticket):
        # 这里假设 ticket 的内容是文件名和文件夹名
        dirs_string = ticket.ticket.decode('utf-8')

        # 读取文件并构造 RecordBatch
        # 这里假设读取的文件内容符合 Arrow 的表格式
        dataset = load_scidb_dataset(self.dir_structure, dirs_string, streaming=self.streaming)

        def generate_batches():
            for example in iter(dataset):
                table = pa.table(example)
                yield table

        if self.streaming:
            return fl.GeneratorStream(SCHEMA,generate_batches())
        else:
            # 返回 RecordBatchStream
            return fl.RecordBatchStream(dataset['train'].data.table)


if __name__ == "__main__":
    server = MyFlightServer(('0.0.0.0', 8815))
    server.serve()
