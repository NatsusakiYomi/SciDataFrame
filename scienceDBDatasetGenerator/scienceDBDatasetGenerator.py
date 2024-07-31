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
"""TODO: Add a description here."""

import csv
import json
import os
from PIL import Image
import datasets

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

def get_file_extension(url):
    return os.path.splitext(url)[1][1:].lower()


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
    # BUILDER_CONFIGS = [
    #     datasets.BuilderConfig(name="first_domain", version=VERSION, description="This part of my dataset covers a first domain"),
    #     datasets.BuilderConfig(name="second_domain", version=VERSION, description="This part of my dataset covers a second domain"),
    # ]
    #
    # DEFAULT_CONFIG_NAME = "first_domain"  # It's not mandatory to have a default configuration. Just use one if it make sense.
    features = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.features = datasets.Features({
            "text": datasets.Value("string"),
            "image": datasets.Image(),
            "binary": datasets.Value("binary")
        })

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
            features=self.features,
            # Here we define them above because they are different between the two configurations

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
        urls = self.config.data_files['urls']
        downloaded_files = dl_manager.download_and_extract(urls)
        file2ext = dict(zip(downloaded_files, self.config.data_files['exts']))
        # dl_manager is a datasets.download.DownloadManager that can be used to download and extract URLS
        # It can accept any type or nested list/dict and will give back the same structure with the url replaced with path to local files.
        # By default the archives will be extracted and a path to a cached folder where they are extracted is returned instead of the archive
        # print('logging:', urls)
        return [
            datasets.SplitGenerator(
                name='allofdata',
                # These kwargs will be passed to _generate_examples
                gen_kwargs={
                    "file2ext": file2ext
                },
            ),
        ]

    # method parameters are unpacked from `gen_kwargs` as given in `_split_generators`
    def _generate_examples(self, file2ext):
        # TODO: This method handles input defined in _split_generators to yield (key, example) tuples from the dataset.
        # The `key` is for legacy reasons (tfds) and is not important in itself, but must be unique for each example.

        id_ = 0
        for (file_path, file_ext) in file2ext.items():
            if file_ext in ['txt', 'json', 'csv']:
                with open(file_path, 'r', encoding='utf-8') as f:
                    print(file_path)
                    print(f)
                    for line in f.readlines():
                        clean_line = line.strip()
                        id_ += 1
                        yield id_, {"text": clean_line,
                                    "image": None,
                                    "binary": None}
            elif file_ext in ['tif', 'png', 'jpg']:
                print(file_path)
                with Image.open(file_path, 'r') as img:
                    print(img)
                    id_ += 1
                    yield id_, {"text": None,
                                "image": img,
                                "binary": None}
            else:
                try:
                    with open(file_path, 'rb') as f:
                        print(f)
                        binary_data = f.read()
                        id_ += 1
                        yield id_, {"text": None,
                                    "image": None,
                                    "binary": binary_data}
                except PermissionError as e:
                    print(f"Permission error: {e}")

def main():
    """调试"""

    from datasets import load_dataset
    builder = NewDataset(split='allofdata')
    dl_manager = datasets.DownloadManager()
    split_generators = builder._split_generators(dl_manager)
    for split_generator in split_generators:
        for example in builder._generate_examples(**split_generator.gen_kwargs):
            print(example)

if __name__ == "__main__":
    main()
