# Copyright 2020 The HuggingFace Datasets Authors and the current dataset script contributor.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# TODO: Address all TODOs and remove all explanatory comments
"""TODO: Add a description here."""

import os
import shutil

import random
import chardet
import datasets
from PIL import Image
from fsspec.utils import file_size
from urllib.parse import urlparse, parse_qs

# TODO: Add BibTeX citation
# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = """\
@InProceedings{huggingface:dataset,
title = {A great new dataset},
author={huggingface, Inc.
},
year={2020}
}
"""

# TODO: Add description of the dataset here
# You can copy an official description
_DESCRIPTION = """\
This new dataset is designed to solve this great NLP task and is crafted with a lot of care.
"""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = ""

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""

# TODO: Add link to the official dataset URLs here
# The HuggingFace Datasets library doesn't host the datasets but only points to the original files.
# This can be an arbitrary nested dict/list of URLs (see below in `_split_generators` method)
_URLS = {
    "first_domain": [
        'https://download.scidb.cn/download?fileId=846e78f2763ca8da978efb2b4dd06b67&path=/V1/数据集/SEM试验结果/磷石膏/磷石膏 (8).tif&fileName=%E7%A3%B7%E7%9F%B3%E8%86%8F%20(8).tif',
        'https://download.scidb.cn/download?fileId=e9f7757114cec9801274ff997d4c4f76&path=/V2/DHB-G-166_1.81um_tiff/20240118_ljz_GHB-G-6_1.81um_recon_Export0001.tiff&fileName=20240118_ljz_GHB-G-6_1.81um_recon_Export0001.tiff',
        'https://download.scidb.cn/download?fileId=aec49141149b35cd784ab86973880454&path=/V2/DHB-G-166_1.81um_tiff/20240118_ljz_GHB-G-6_1.81um_recon_Export0004.tiff&fileName=20240118_ljz_GHB-G-6_1.81um_recon_Export0004.tiff'],
    "second_domain": [
        'https://download.scidb.cn/download?fileId=e27ffa47f7f9ba3fb72939a624a5e146&path=/V1/数据集/SEM试验结果/磷石膏/磷石膏 (1).tif&fileName=%E7%A3%B7%E7%9F%B3%E8%86%8F%20(1).tif'],
}

LIMIT=1024*1024*200


def get_file_extension(url):
    print(url)
    parsed_url = list(map(urlparse,url))
    query_params = list(map(lambda parse_url: parse_qs(parse_url.query), parsed_url))
    print(query_params)
    return list(map(lambda query_params: os.path.splitext(query_params['fileName'][0])[1].lower(), query_params))


def is_text_format(ext):
    return ext in ['txt', 'json', 'csv']


def is_pil_format(ext):
    ext = ext.lower() if ext.startswith('.') else f'.{ext.lower()}'
    return ext in Image.registered_extensions()

def get_file_size(file_path):
    return os.path.getsize(file_path)

def is_over_limit(file_path):
    return get_file_size(file_path) > LIMIT



# TODO: Name of the dataset usually matches the script name with CamelCase instead of snake_case
class NewDataset(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = datasets.Version("1.1.0")

    # This is an example of a dataset with multiple configurations.
    # If you don't want/need to define several sub-sets in your dataset,
    # just remove the BUILDER_CONFIG_CLASS and the BUILDER_CONFIGS attributes.

    # If you need to make complex sub-parts in the datasets with configurable options
    # You can create your own builder configuration class to store attribute, inheriting from datasets.BuilderConfig
    # BUILDER_CONFIG_CLASS = MyBuilderConfig

    # You will be able to load one or the other configurations in the following list with
    # data = datasets.load_dataset('my_dataset', 'first_domain')
    # data = datasets.load_dataset('my_dataset', 'second_domain')
    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="first_domain", version=VERSION,
                               description="This part of my dataset covers a first domain"),
        datasets.BuilderConfig(name="second_domain", version=VERSION,
                               description="This part of my dataset covers a second domain"),
    ]

    DEFAULT_CONFIG_NAME = "first_domain"  # It's not mandatory to have a default configuration. Just use one if it make sense.
    features = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if 'datafiles' in kwargs and 'datadir' in kwargs:
            self.config.data_files = kwargs['datafiles']
            self.config.data_dir = kwargs['datadir']
        # 根据文件名设置特征和文件扩展名
        # url = self.config.data_files
        # self.file_extension = os.path.splitext(url)[1][1:].lower()

    def _info(self):
        # TODO: This method specifies the datasets.DatasetInfo object which contains informations and typings for the dataset
        # self.urls=self.config.data_files
        # print('logging:',self.urls)
        # features = datasets.Features(
        #         {
        #             "image": datasets.Image()
        #             # These are the features of your dataset like images, labels ...
        #         }
        #     )
        return datasets.DatasetInfo(
            # This is the description that will appear on the datasets page.
            description=_DESCRIPTION,
            # This defines the different columns of the dataset and their types
            features=datasets.Features({
                "text": datasets.Sequence(datasets.Value('binary')),
                "image": datasets.Sequence(datasets.Value('binary')),
                "binary": datasets.Sequence(datasets.Value('binary')),
                "ext": datasets.Sequence(datasets.Value('string')),
            }),  # Here we define them above because they are different between the two configurations

            # If there's a common (input, target) tuple from the features, uncomment supervised_keys line below and
            # specify them. They'll be used if as_supervised=True in builder.as_dataset.
            # supervised_keys=("sentence", "label"),
            # Homepage of the dataset for documentation
            homepage=_HOMEPAGE,
            # License for the dataset if available
            license=_LICENSE,
            # Citation for the dataset
            citation=_CITATION,
        )

    def _split_generators(self, dl_manager):
        # TODO: This method is tasked with downloading/extracting the data and defining the splits depending on the configuration
        # If several configurations are possible (listed in BUILDER_CONFIGS), the configuration selected by the user is in self.config.name
        urls = self.config.data_files['train']

        # download_config = datasets.DownloadConfig(num_proc=1, max_retries=5)
        cache_dir = os.path.join(dl_manager.download_config.cache_dir, "datasets")
        # if os.path.exists(cache_dir):
        #     shutil.rmtree(cache_dir)
        downloaded_files = dl_manager.download(urls)
        exts = self.config.data_dir
        # print(self.config.data_dir)
        # file2ext = dict(zip(downloaded_files, self.config.data_dir))
        # dl_manager is a datasets.download.DownloadManager that can be used to download and extract URLS
        # It can accept any type or nested list/dict and will give back the same structure with the url replaced with path to local files.
        # By default the archives will be extracted and a path to a cached folder where they are extracted is returned instead of the archive
        # print('logging:',urls)
        return [
            datasets.SplitGenerator(
                name=datasets.Split.TRAIN,
                # These kwargs will be passed to _generate_examples
                gen_kwargs={
                    "zip_paths_exts": zip(downloaded_files, exts),
                },
            ),
        ]

    # method parameters are unpacked from `gen_kwargs` as given in `_split_generators`
    def _generate_examples(self, zip_paths_exts):
        # TODO: This method handles input defined in _split_generators to yield (key, example) tuples from the dataset.
        # The `key` is for legacy reasons (tfds) and is not important in itself, but must be unique for each example.
        #     print(f"Processing image from URL: {urls}")
        # print(images)
        id_ = 0
        for file_path, file_ext in zip_paths_exts:
            print(file_path)
            print(file_ext)
            chunk_flag=is_over_limit(file_path)
            # if chunk_flag  or (not is_pil_format(file_ext) and not is_text_format(file_ext)):
            if chunk_flag  or (not is_text_format(file_ext)):
                try:
                    offset=0
                    while True:
                        with open(file_path, 'rb') as f:
                            f.seek(offset)
                            chunk_binary_data = f.read(LIMIT)
                            yield id_, {
                                "text": [b''],
                                "image": [b''],
                                "binary": [chunk_binary_data],
                                "ext": [file_ext],
                            }
                            id_ += 1
                            chunk_binary_data = f.read(LIMIT)
                            if not chunk_binary_data:
                                break
                            offset+=LIMIT


                except PermissionError as e:
                    pass
                    # print(f"Warning: Permission error : {e}")
            elif is_text_format(file_ext):
                with open(file_path, 'rb' ) as f:
                    # print(file_path)
                    # print(f)
                    document = f.read()
                    # print(document)
                    yield id_, {
                                "text": [document],
                                "image": [b''],
                                "binary": [b''],
                                "ext": [file_ext],
                                }
            else :
                # print(file_path)
                with Image.open(file_path, 'r') as img:
                    print(img)
                    yield id_, {
                                "text": [b''],
                                "image": [img],
                                "binary": [b''],
                                "ext": [file_ext],
                                }
            id_+=random.randint(1,3)
            # print(id_)


    # def save(self, file_path, dataset):
    #     """ 利用 pyarrow 的分块写入功能保存数据 """
    #     with pa.OSFile(file_path, 'wb') as sink:
    #         with pa.RecordBatchFileWriter(sink, dataset.schema) as writer:
    #             for batch in dataset.to_batches():
    #                 writer.write_batch(batch)

def main():
    # 脚本内调试
    builder = NewDataset(config_name='second_domain', data_files=_URLS['second_domain'], data_dir=['zip'])
    dl_manager = datasets.DownloadManager()
    split_generators = builder._split_generators(dl_manager)
    for split_generator in split_generators:
        for example in builder._generate_examples(**split_generator.gen_kwargs):
            pass
            # print(example)


if __name__ == "__main__":
    main()
