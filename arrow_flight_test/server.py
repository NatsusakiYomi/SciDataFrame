import pyarrow as pa
import pyarrow.flight as flight


class FlightServer(flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self.client_message = None

    def do_put(self, context, descriptor, reader, writer):
        # 接收客户端的 Arrow Table
        table = reader.read_all()
        print(f"Received table from client:\n{table}")

        # 假设第一列的第一个单元格是用户的输入信息
        self.client_message = table.column(0)[0].as_py()
        print(f"Client message: {self.client_message}")

    def do_get(self, context, ticket):
        # 根据客户端的输入生成一个新的 Arrow Table
        if self.client_message:
            response_table = pa.table({
                'message': [f"Received your message: {self.client_message}"],
                'length_of_message': [len(self.client_message)],
                'characters': [list(self.client_message)],
            })
        else:
            response_table = pa.table({
                'message': ["No message received"],
                'length_of_message': [0],
                'characters': [[]],
            })

        return flight.RecordBatchStream(response_table)


def main():
    server = FlightServer(("0.0.0.0", 8815))  # 监听在 8815 端口
    print("Flight server running on port 8815...")
    server.serve()


if __name__ == "__main__":
    main()
