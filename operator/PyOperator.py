from arrow_flight import DownstreamFlightClient
# from tests.test import is_preprocess, is_streaming

from training_scripts import RecommendationModel
from model import MyDataFrame
import asyncio


class PyOperator:


    def __init__(self, address="127.0.0.1", port=8816, loop_interval=1):
        self.client = DownstreamFlightClient(address=address, port=port)
        self.interval = loop_interval
        self.df = None

    async def poll_for_data(self):
        """
        异步轮询获取数据，直到数据准备好
        :return: 数据表
        """
        while True:
            try:
                # self.df = MyDataFrame(data=self.client.do_get(),client="DATA")
                self.df = MyDataFrame(schema=self.client.do_get().to_pandas(), client=None,port=8815,
                                      is_iterate=False, is_analyze=False,is_get_dataset_str=False,task=None,
                                      is_preprocess=False,is_streaming=True)
                return self.df
            except Exception as e:
                print(e)
                await asyncio.sleep(self.interval)
                continue

    def get_data(self):
        loop = asyncio.get_event_loop()
        self.df = loop.run_until_complete(self.poll_for_data())
        self.df.flat_open()
        return self.df

    def start_train(self):
        RecommendationModel(self.df)

if __name__ == "__main__":
    py_operator=PyOperator()
    py_operator.get_data()
    # py_operator.start_train()
