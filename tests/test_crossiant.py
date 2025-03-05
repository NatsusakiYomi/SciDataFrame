
# ds = mlc.Dataset("https://raw.githubusercontent.com/mlcommons/croissant/main/datasets/1.0/gpt-3/metadata.json")
# ds.records()
import datasets as ds
import mlcroissant as mlc
import tensorflow_datasets as tfds
url = "https://huggingface.co/api/datasets/fashion_mnist/croissant"
data_dir="C:\\Users\\Yomi\\PycharmProjects\\SDB2\\cache_dir"
builder = tfds.dataset_builders.CroissantBuilder(
    jsonld=url,
    record_set_ids=["fashion_mnist"],
    file_format='array_record',
    data_dir=data_dir,
)