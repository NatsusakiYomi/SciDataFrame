from model import SciDataFrame
from utils import TrainingTask

dataset_id = 'new.txt'
dataset_path = None
is_analyze = False
is_preprocess = False
is_get_dataset_str = True
is_streaming = False
task = TrainingTask.Recommendation
batch_size = 1
is_iterate = False
kwargs = {
    # "dataset_id": dataset_id,
    # "folder_path": dataset_path,
    "is_analyze": is_analyze,
    "is_preprocess": is_preprocess,
    "is_get_dataset_str": is_get_dataset_str,
    "is_streaming": is_streaming,
    "task": task,
    "batch_size": batch_size,
    "is_iterate": is_iterate,
}
# 根据配置新建DataFrame
df = SciDataFrame(dataset_id, **kwargs)
# 获得数据集schema
schema = df.get_schema()
df.flat_open("dataset.csv")
# 打开文件（夹）
# df = df.open("out")