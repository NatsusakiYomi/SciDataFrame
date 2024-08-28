from datasets import load_dataset
from utils import url_parser,filter_url_from_index
from utils import print_directory_tree
import os
import time



os.environ["HF_DATA SETS_NUM_THREADS"] = "1"
URL_TXT_ROOT = r'./urls'
# 修改端口号
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
OPTION = 'ALL'
REMOTE = True
LOCAL_FILE_PATH = 'E:\Study\Postgrad_1\\research\\arrow\LocalDataset'
# LOADING SCRIPT路径
LOCAL_SCRIPT_PATH = "\Study\Postgrad_1\\research\\arrow\\scienceDBDatasetGenerator"
# 缓存路径
CACHE_DIR = '\Study\\Postgrad_1\\research\\arrow\cache_dir'
URL_LIST=[]


def read_url_from_txt(path):
    with open(os.path.join(URL_TXT_ROOT,path),'r',encoding='utf-8') as f:
        url_lists=f.readlines()
        URL_LIST=url_lists
        return url_parser(url_lists)



def load_sciencedb(txt):
    if REMOTE:
        dir_structure = read_url_from_txt(path=txt)
        print_directory_tree(dir_structure)
        target_dir = input("请输入要下载的文件或者文件夹名称，以逗号分隔: ").split(',')
        urls_all,file_extensions=filter_url_from_index(dir_structure,target_dir)
        if OPTION == 'ALL':
            ds = load_dataset(LOCAL_SCRIPT_PATH, cache_dir=CACHE_DIR, num_proc=5, data_files=urls_all,
                              data_dir=file_extensions)
    else:
        ds = load_dataset(LOCAL_FILE_PATH, cache_dir='.\cache_dir')
    time.sleep(1)
    return ds


if __name__ == '__main__':
    # 选择数据集url
    PATH = '7e0a4faa0d0649918ae3e94ef34b94af.txt'
    PATH = '25d185f01b8c43b29420c48e50a11fad.txt'
    # PATH='8cf1ce3983ad4d5d8f45ee6469ab5dcc.txt'
    # 超大数据集
    PATH = '91574142078b45c79d532d97b294ed44.txt'

    PATH = 'c0bd7f5c79a24e48849432629f59639f.txt'
    PATH = 'e416c488169f484485ad7575dcfc43ce.txt'
    # PATH = "new.txt"
    #
    # PATH = 'b6a1d3f42b014fa9ae9cce04679a5e0f.txt'
    print(load_sciencedb(txt=PATH))
