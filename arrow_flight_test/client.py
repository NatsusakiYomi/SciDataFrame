import pyarrow as pa
import pyarrow.flight as flight
import time


def main():
    client = flight.FlightClient(("localhost", 8815))

    while True:
        # 从控制台获取用户输入
        user_input = input("Enter a message to send to the server (or 'exit' to quit): ")

        if user_input.lower() == 'exit':
            break

        # 将输入信息打包成 Arrow Table
        input_table = pa.table({
            'input': [user_input]
        })

        # 发送到服务端
        descriptor = flight.FlightDescriptor.for_command('user_message')
        writer, _ = client.do_put(descriptor, input_table.schema)
        writer.write_table(input_table)
        writer.close()

        # 接收服务端返回的表
        ticket = flight.Ticket(b'')
        reader = client.do_get(ticket)
        response_table = reader.read_all()

        # 打印服务端返回的表
        print("Received response from server:")
        print(response_table)

        time.sleep(1)  # 延时1秒以免过于频繁请求


if __name__ == "__main__":
    main()
