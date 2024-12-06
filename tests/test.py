from model import MyDataFrame
from utils import TrainingTask

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
df = MyDataFrame()
# 获得数据集schema
schema = df.get_schema()
# 打开文件（夹）
# df = df.open("out")
# df = df.open(dataset_path)
# 加载数据
df.flat_open()
for i in df:
    pass