import pyarrow
import pyarrow.flight as fl


class DownstreamClientServer():
    def __init__(self):
        self.fl_client = fl.connect("grpc://127.0.0.1:8814")

    def do_get(self):
        ticket = fl.Ticket("".encode("utf-8"))
        reader = self.fl_client.do_get(ticket)
        table = reader.read_all()
        print(table.schema)

if __name__ == '__main__':
    client = DownstreamClientServer()
    client.do_get()