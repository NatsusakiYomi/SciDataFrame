import mlcroissant as mlc
ds = mlc.Dataset("https://raw.githubusercontent.com/mlcommons/croissant/main/datasets/1.0/gpt-3/metadata.json")
ds.records()
import datasets as ds
ds.b
import tensorflow_datasets as tfds
builder = tfds.core.dataset_builder.CroissantBuilder( jsonld=url, record_set_ids=["record_set_fashion_mnist"], file_format='array_record', )