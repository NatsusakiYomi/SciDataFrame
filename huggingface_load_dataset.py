import datasets
import pyarrow as pa
from datasets.iterable_dataset import FormattingConfig
# datasets.Dataset
# datasets.Dataset
# import datasets.load_dataset_builder
# datasets.load_dataset
# datasets.Dataset
# ds_builder = datasets.load_dataset_builder("ylecun/mnist")
# print(ds_builder.info)
ds = datasets.load_dataset("cornell-movie-review-data/rotten_tomatoes", split="train",streaming=True)
ds._formatting=FormattingConfig('arrow')
iterator=ds.iter(5)
print(next(iterator))
print(next(iterator))
# ds.save_to_disk("")
# with pa.OSFile(r'C:\\Users\NatsusakiYomi\.cache\huggingface\datasets\ylecun___mnist\mnist\\1.0.0\\b06aab39e05f7bcd9635d18ed25d06eae523c574\\mnist-train.arrow') as source:
#     array = pa.ipc.open_file(source).read_all()
with pa.memory_map('data-00000-of-00001.arrow','rb') as source:
    memory_mapped_stream = pa.ipc.open_stream(source)
print("Now RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
