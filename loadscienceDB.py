from datasets import load_dataset
from utils import url_parser
from utils import print_directory_tree
import os
import time



os.environ["HF_DATA SETS_NUM_THREADS"] = "1"
URL_TXT_ROOT = r'./urls'
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:49756'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:49756'


def read_url_from_txt(path):
    with open(os.path.join(URL_TXT_ROOT,path),'r',encoding='utf-8') as f:
        url_lists=f.readlines()
        return url_parser(url_lists)


if __name__ == '__main__':
    OPTION='ALL'
    PATH='7e0a4faa0d0649918ae3e94ef34b94af.txt'
    PATH='25d185f01b8c43b29420c48e50a11fad.txt'
    # PATH='8cf1ce3983ad4d5d8f45ee6469ab5dcc.txt'
    PATH="new.txt"
    REMOTE=True
    LOCAL_FILE_PATH='E:\Study\Postgrad_1\\research\\arrow\LocalDataset'
    LOCAL_SCRIPT_PATH="C:\\Users\\NatsusakiYomi\Documents\Study\Postgrad1\\research\Sci2DB\\test\SciDB2Dataset\scienceDBDatasetGenerator"
    CACHE_DIR='C:\\Users\\NatsusakiYomi\\Documents\\Study\\HF_cache_dir'
    if REMOTE:
        urls_all,file_extensions,url_split,file_structure=read_url_from_txt(path=PATH)
        if OPTION == 'ALL':
                ds = load_dataset(LOCAL_SCRIPT_PATH, cache_dir=CACHE_DIR, num_proc=5,data_files=urls_all,data_dir=file_extensions)
        print(ds)
    else:
        ds=load_dataset(LOCAL_FILE_PATH,cache_dir='.\cache_dir')
        print(ds)
    time.sleep(1)
    print(set(file_extensions))
    print_directory_tree(file_structure)
