import sys

import pyarrow

sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')
import pytest
from pyarrow import flight
from model import MyDataFrame
from training_scripts import *
from utils import TrainingTask, Level

dataset_id = 'new.txt'
dataset_path = None
is_analyze = False
is_preprocess = False
is_get_dataset_str = False
is_streaming = False
task = TrainingTask.Recommendation
batch_size = 1
is_iterate = True
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
df=MyDataFrame(dataset_id, **kwargs)
df.get_schema()
df=df.filter("\.txt$")
print(df.schema)
df.flat_open()
for i in df.to_iterator():
    pass
# print(df.data.schema)
# print(f"Row count: {df.data.num_rows}")