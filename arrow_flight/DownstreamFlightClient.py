import pyarrow
import pyarrow.flight as fl


class DownstreamFlightClient():
    def __init__(self, address="127.0.0.1", port=8814):
        self.fl_client = fl.connect("grpc://"+address+":"+str(port))
    def do_get(self):
        ticket = fl.Ticket("".encode("utf-8"))
        reader = self.fl_client.do_get(ticket)
        table = reader.read_all()
        print(table.schema)
        return table

if __name__ == '__main__':
    client = DownstreamFlightClient()
    client.do_get()