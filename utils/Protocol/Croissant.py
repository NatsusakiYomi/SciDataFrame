# import os
# os.environ['HTTP_PROXY'] = '127.0.0.1:7890'
# os.environ['HTTPS_PROXY'] = '127.0.0.1:7890'
import itertools
import mlcroissant as mlc
import pandas as pd
def test_croissant():
    from mlcroissant import Dataset
    # ds = Dataset(jsonld="https://hf-mirror.com/api/datasets/tasksource/blog_authorship_corpus/croissant")
    # records = ds.records("default")
    # df = (
    #     pd.DataFrame(list(itertools.islice(records, 100)))
    #     .groupby("default/sign")["default/text"]
    #     .apply(lambda x: x.str.len().mean())
    #     .sort_values(ascending=False)
    #     .head(5)
    # )
    # print(df)
    # import mlcroissant as mlc
    # url = "http://huggingface.co/api/datasets/fashion_mnist/croissant"
    # ds = mlc.Dataset(url)
    # for x in ds.records(record_set="fashion_mnist"):
    #   print(x)
    #   break

    dataset = mlc.Dataset(jsonld="C:\\Users\\Yomi\\PycharmProjects\\SDB2\\utils\\Protocol\\export.json")
    # dataset = mlc.Dataset(jsonld="C:\\Users\\Yomi\\PycharmProjects\\SDB2\\utils\\Protocol\\croissant.json")
    # dataset = mlc.Dataset(jsonld="C:\\Users\\Yomi\\PycharmProjects\\SDB2\\utils\\Protocol\\microsoft-catsvsdogs-dataset-metadata.json")
    # dataset = mlc.Dataset(jsonld="C:\\Users\\Yomi\\PycharmProjects\\SDB2\\utils\\Protocol\\world-gdp-population-and-co2-emissions-dataset-metadata.json")
    # records = dataset.records(record_set="jsonl")
    #
    # for i, record in enumerate(records):
    #   print(record)
    #   if i > 10:
    #     break
    records = dataset.records("default")
    df = (
        pd.DataFrame(list(itertools.islice(records, 100)))
        .groupby("default/sign")["default/text"]
        .apply(lambda x: x.str.len().mean())
        .sort_values(ascending=False)
        .head(5)
    )
    print(df)
def croissant_to_dir_structure():
    dataset = mlc.Dataset(
        jsonld="C:\\Users\\Yomi\\PycharmProjects\\SDB2\\utils\\Protocol\\world-gdp-population-and-co2-emissions-dataset-metadata.json")
    files=[]
    for file_object in dataset.metadata.file_objects:
        files.append((file_object.name, file_object.content_url))
    print(files)
    return files
if __name__ == "__main__":
    test_croissant()
    # croissant_to_scidf()