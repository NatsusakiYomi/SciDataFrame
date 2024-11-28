import sys
import os
import io


sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')

import pyarrow.flight as fl

class UpstreamFlightServer(fl.FlightServerBase):
    def __init__(self, location, data):
        super().__init__(location)
        self.dataset = data

    def do_get(self, context, ticket):
        print("Received request:", ticket)
        return fl.RecordBatchStream(self.dataset)