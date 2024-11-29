import sys
import os
import io


sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')

import pyarrow.flight as fl

class UpstreamFlightServer(fl.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self.dataset=None

    def do_get(self, context, ticket):
        print("Received request:", ticket)
        if self.dataset is not None:
            return fl.RecordBatchStream(self.dataset)
        else:
            raise Exception("data is not prepared.")

    def output(self, data):
        self.dataset=data