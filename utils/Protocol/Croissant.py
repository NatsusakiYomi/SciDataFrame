# import os
# os.environ['HTTP_PROXY'] = '127.0.0.1:7890'
# os.environ['HTTPS_PROXY'] = '127.0.0.1:7890'
import itertools
from utils import url_parser, filter_url_from_index
import mlcroissant as mlc
import pandas as pd
import validators
import json
from model import SciDataFrame
# def test_croissant():
#     from mlcroissant import Dataset
#     # ds = Dataset(jsonld="https://hf-mirror.com/api/datasets/tasksource/blog_authorship_corpus/croissant")
#     # records = ds.records("default")
#     # df = (
#     #     pd.DataFrame(list(itertools.islice(records, 100)))
#     #     .groupby("default/sign")["default/text"]
#     #     .apply(lambda x: x.str.len().mean())
#     #     .sort_values(ascending=False)
#     #     .head(5)
#     # )
#     # print(df)
#     # import mlcroissant as mlc
#     # url = "http://huggingface.co/api/datasets/fashion_mnist/croissant"
#     # ds = mlc.Dataset(url)
#     # for x in ds.records(record_set="fashion_mnist"):
#     #   print(x)
#     #   break
#
#     dataset = mlc.Dataset(jsonld="C:\\Users\\Yomi\\PycharmProjects\\SDB2\\utils\\Protocol\\export.json")
#     # dataset = mlc.Dataset(jsonld="C:\\Users\\Yomi\\PycharmProjects\\SDB2\\utils\\Protocol\\croissant.json")
#     # dataset = mlc.Dataset(jsonld="C:\\Users\\Yomi\\PycharmProjects\\SDB2\\utils\\Protocol\\microsoft-catsvsdogs-dataset-metadata.json")
#     # dataset = mlc.Dataset(jsonld="C:\\Users\\Yomi\\PycharmProjects\\SDB2\\utils\\Protocol\\world-gdp-population-and-co2-emissions-dataset-metadata.json")
#     # records = dataset.records(record_set="jsonl")
#     #
#     # for i, record in enumerate(records):
#     #   print(record)
#     #   if i > 10:
#     #     break
#     records = dataset.records("default")
#     df = (
#         pd.DataFrame(list(itertools.islice(records, 100)))
#         .groupby("default/sign")["default/text"]
#         .apply(lambda x: x.str.len().mean())
#         .sort_values(ascending=False)
#         .head(5)
#     )
#     print(df)
# import tensorflow_datasets as tfds
# builder = tfds.dataset_builders.CroissantBuilder(
#     jsonld=url,
#     record_set_ids=["record_set_fashion_mnist"],
#     file_format='array_record',
# )
def croissant_to_dir_structure(path):
    dataset = mlc.Dataset(
        jsonld="C:\\Users\\Yomi\\PycharmProjects\\SDB2\\urls\\"+path)
    files=[]
    for file_object in dataset.metadata.file_objects:
        if validators.url(file_object.content_url):
            files.append((file_object.name, file_object.content_url))
    print(files)
    return files

def scidataframe_to_croissant(dataset_id,schema,dir_structure):
    # FileObjects and FileSets define the resources of the dataset.
    try:
        print("\n", dataset_id, "\n", schema, "\n", dir_structure)
        data_files = schema[schema['type'] != 'dir']
        print("\n", data_files, "\n")
        data_files_list=data_files['name'].tolist()
        print("\n",  data_files_list, "\n")
        urls,_=filter_url_from_index(dir_structure,data_files_list)

        import mimetypes
        def get_mime_type(filename):
            mime, _ = mimetypes.guess_type(filename)
            return mime if mime else "application/octet-stream"

        distribution=[]
        for url,name in zip(urls,data_files_list):
            url_id=url.split('=')[-1]
            distribution.append(
                # gpt-3 is hosted on a GitHub repository:
                mlc.FileObject(
                    id=url_id,
                    name=name,
                    description="SciDataFrame generated croissant format json",
                    content_url=url,
                    encoding_format=get_mime_type(name),
                    sha256="",
                )
            )

        # Metadata contains information about the dataset.
        metadata = mlc.Metadata(
            name=dataset_id,
            # Descriptions can contain plain text or markdown.
            description=(
                "SciDataFrame generated croissant format json"
            ),
            cite_as=(
                "SciDataFrame generated croissant format json"
            ),
            url="",
            distribution=distribution,
        )
        with open("".join(dataset_id.split(".")[:-1])+".json", "w", encoding="utf-8") as f:
            content = metadata.to_json()
            content = json.dumps(content, indent=2,ensure_ascii=False)
            print(content)
            f.write(content)
            f.write("\n")  # Terminate file with newline
    except Exception as e:
        print(e)
if __name__ == "__main__":

    from utils import TrainingTask, Level

    dataset_id = 'export.json'
    dataset_path = None
    is_analyze = False
    is_preprocess = False
    is_get_dataset_str = False
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
    df = SciDataFrame(dataset_id, **kwargs)
    df.get_schema()
    df.generate_croissant_json()
    # dir_dict = {'tertiary_industry_SSP5.zip': (
    # 'https://china.scidb.cn/download?fileId=ac1e618b36338ae455106bef7a5b79cf', 'zip'), }
    # scidataframe_to_croissant("export.json",df.schema,dir_dict)
    # df.generate_croissant_json()
    # croissant_to_dir_structure("export.json")
    # croissant_to_scidf()