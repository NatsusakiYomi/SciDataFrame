import pytest
from pyarrow import flight
from arrow_flight.server import MyFlightServer


from model import SciDataFrame
from training_scripts import *
from utils import TrainingTask, Level

# @pytest.fixture
# def server():
#     return MyFlightServer(("0.0.0.0", 8815))
#
# @pytest.fixture
# def client():
#     return flight.FlightClient(("localhost", 8815))

def test_do_action(server,client):
    assert server
    assert client.do_action(flight.Action("get_data_1", "dataset_id".encode('utf-8'))) is NotImplementedError
    client.do_action(flight.Action("get_schema", "dataset_id".encode('utf-8')))

def test_subtract(calculator):
    assert calculator.subtract(5, 2) == 3
    assert calculator.subtract(-1, -1) == 0
    assert calculator.subtract(-1, 1) == -2

def test_divide(calculator):
    assert calculator.divide(10, 2) == 5
    assert calculator.divide(5, 2) == 2.5
    with pytest.raises(ValueError, match="Cannot divide by zero."):
        calculator.divide(10, 0)

def test_mydataframe():
    server = MyFlightServer('grpc://127.0.0.1:8815')
    server.serve()
    dataset_id = 'images_test.txt'
    dataset_path = None
    is_analyze = True
    is_preprocess = True
    is_get_dataset_str = False
    is_streaming = True
    task = TrainingTask.Recommendation
    batch_size = 1
    kwargs = {
        # "dataset_id": dataset_id,
        # "folder_path": dataset_path,
        "is_analyze": is_analyze,
        "is_preprocess": is_preprocess,
        "is_get_dataset_str": is_get_dataset_str,
        "is_streaming": is_streaming,
        "task": task,
        "batch_size": batch_size,
    }
    # 根据配置新建DataFrame
    df = SciDataFrame()
    # 获得数据集schema
    schema = df.get_schema(dataset_id)
    # 打开文件（夹）
    # df = df.open("out")
    # df = df.open(dataset_path)
    # 加载数据
    df.flat_open()
    for i in df:
        pass
    # 合并数据迭代器
    # if batch_size is not None:
    #     df.iter_to_instance()
    # start_train = input("Start train? ").lower() == 'yes'
    # try:
    #     if task == TrainingTask.Recommendation:
    #         RecommendationModel(df)
    #     elif task == TrainingTask.MultiLabelClassification:
    #         BertMultiClassification(df)
    #     elif task == TrainingTask.ImageClassification:
    #         ImageClassification(df)
    #     else:
    #         raise NotImplementedError(f"Task {task} not implemented")
    # except NotImplementedError as e:
    #     print(e)
