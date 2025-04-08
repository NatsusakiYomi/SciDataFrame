import sys

# sys.path.append('..')
from model import SciDataFrame

from utils import TrainingTask, Level

dataset_id = '3b0113781ced456dba2e3036cab6096c.txt'
dataset_path = None
is_analyze = False
is_preprocess = False
is_get_dataset_str = True
is_streaming = False
task = TrainingTask.MultiLabelClassification
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
df=SciDataFrame(dataset_id, **kwargs)
df.get_schema()
df.flat_open("HDF_MPL_4202_202103230343.hdf")
print(df.data.schema)