import sys
import time
import socket
class IPC(object):
    def __init__(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(("localhost", 32000))
        self.fid = self.s.makefile('rw') # file wrapper to read lines
        self.listenLoop() # wait listening for updates from server

    def listenLoop(self):
        fid = self.fid
        print("connected")
        while True:
            while True:
                line = fid.readline()
                if line[0]=='.':
                    break
            fid.write('.\n')
            fid.flush()
def arrowIPC():
    import pyarrow as pa
    import io

    def receive_arrow_data(sock):
        buffer = io.BytesIO()
        while True:
            data = sock.recv(4096)
            if not data:
                break
            buffer.write(data)
        buffer.seek(0)  # 重置缓冲区指针到起始位置
        reader = pa.ipc.open_file(buffer)
        table = reader.read_all()
        return table

    connected = False
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # 一直尝试连接直到成功
    while not connected:
        try:
            client_socket.connect(('localhost', 9090))
            connected = True
            print("Connected to server!")
        except ConnectionRefusedError:
            print("Connection refused. Retrying...")
            time.sleep(2)  # 等待2秒后再重试
        except Exception as e:
            print(f"An error occurred: {e}")
            break

    # client_socket.connect(('localhost', 9090))

    table = receive_arrow_data(client_socket)
    print(table.to_pandas())

    client_socket.close()
    return table


if __name__ == '__main__':
    # st = IPC()
    st = arrowIPC()