from datasets import load_dataset, download, logging
from urlParser import url_parser
import requests
import os
import time

# 设置日志级别为 ERROR
# logging.set_verbosity_error()

# os.environ["HF_DATA SETS_NUM_THREADS"] = "1"
URL_TXT_ROOT = r'./urls'


def read_url_from_txt(path):
    with open(os.path.join(URL_TXT_ROOT, path), 'r', encoding='utf-8') as f:
        url_lists = f.readlines()
        return url_parser(url_lists)


if __name__ == '__main__':
    OPTION = 'ALL'
    REMOTE = True
    PATH = '7e0a4faa0d0649918ae3e94ef34b94af.txt'
    PATH = '25d185f01b8c43b29420c48e50a11fad.txt'
    # PATH='8cf1ce3983ad4d5d8f45ee6469ab5dcc.txt'
    # PATH="new.txt"
    LOCAL_FILE_PATH = 'E:\Study\Postgrad_1\\research\\arrow\LocalDataset'
    LOADING_SCRIPT_PATH = "C:\\Users\\NatsusakiYomi\\Documents\\Study\\Postgrad1\\research\\Sci2DB\\SciDB2Dataset\\scienceDBDatasetGenerator"

    if REMOTE:
        urls_and_extensions, url_split, file_structure = read_url_from_txt(path=PATH)
        # for url in urls_all:
        #     print(requests.get(url))
        if OPTION == 'ALL':
            ds = load_dataset(LOADING_SCRIPT_PATH,
                              cache_dir='.\cache_dir',
                              num_proc=5, data_files=urls_and_extensions,)
        else:
            # TODO: 高效从list中pick所有符合要求的url
            pass
        print(ds)
    else:
        ds = load_dataset(LOCAL_FILE_PATH, cache_dir='.\cache_dir')
        print(ds)
