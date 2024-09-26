import pyarrow as pa
import pyarrow.flight as fl
from loadscienceDB import  load_schema

class MyFlightServer(fl.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)

    def do_action(self, context, action):
        dataset_id=action.body.to_pybytes().decode('utf-8')
        if action.type == "get_schema":
            # 创建示例 schema
            schema = load_schema(dataset_id)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_file(sink, schema.schema) as writer:
                writer.write_table(schema)
            # 将 schema 序列化为 bytes

            return [sink.getvalue().to_pybytes()]

        return []

    def do_get(self, context, ticket):
        # 这里假设 ticket 的内容是文件夹路径
        folder_path = ticket.ticket.decode('utf-8')

        # 读取文件并构造 RecordBatch
        # 这里假设读取的文件内容符合 Arrow 的表格式
        data = {
            'column1': [1, 2, 3],
            'column2': ['a', 'b', 'c'],
        }
        table = pa.table(data)

        # 返回 RecordBatchStream
        return fl.RecordBatchStream(table)

if __name__ == "__main__":
    server = MyFlightServer(('0.0.0.0',8815))
    server.serve()
