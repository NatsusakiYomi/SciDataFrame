import os
import tempfile

from datasets import load_dataset, dataset_dict, IterableDataset

from utils import DirectoryTree
from utils import url_parser, filter_url_from_index
from utils import Version

os.environ["FSSPEC_TIMEOUT"] = "36000"  # 设置超时时间为1200秒
os.environ["HF_DATA SETS_NUM_THREADS"] = "5"
# 修改端口号
# os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
# os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
OPTION = 'TEMP'
OPTION = 'IN-MEMORY'
REMOTE = True
LOCAL_FILE_PATH = 'E:\Study\Postgrad_1\\research\\arrow\LocalDataset'
# LOADING SCRIPT路径
CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
LOCAL_SCRIPT_PATH = os.path.join(CURRENT_DIRECTORY, 'scienceDBDatasetGenerator')
URL_TXT_ROOT = os.path.join(CURRENT_DIRECTORY, 'urls')
# 缓存路径
desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
# 要创建的文件夹名称
folder_name = "cache_dir"
# 构建完整路径
CACHE_DIR = os.path.join(desktop_path, folder_name)
dir_path = os.makedirs(CACHE_DIR, exist_ok=True)
URL_LIST = []
MAX_LEN = 30


def read_url_from_txt(path):
    with open(os.path.join(URL_TXT_ROOT, path), 'r', encoding='utf-8') as f:
        url_lists = f.readlines()
        URL_LIST = url_lists
        return url_parser(url_lists)


def preprocess_dataset(dataset_dict: dataset_dict):
    # data_operator=DataOperator(dataset_dict)
    # print(data_operator.get_dataset())
    # print(data_operator.split_dataset(0.2))
    # print(data_operator.filter_dataset((lambda example: example["text"] is not None and isinstance(example["text"], str) and len(example["text"]) > 100)))
    # print(data_operator.select_dataset([0,1,2]))
    # print(data_operator.unique_dataset('text'))
    # print(data_operator.sort_dataset('text'))
    # print(data_operator.flat_dataset())
    pass


def load_schema(txt):
    # method_directory = os.path.dirname(os.path.abspath(__file__))
    # os.chdir(method_directory)
    # print(os.getcwd())
    dir_structure = read_url_from_txt(path=txt)
    directory_tree = DirectoryTree()
    return directory_tree.get_schema(dir_structure), dir_structure


def load_scidb_dataset(dir_structure, string, streaming=False):
    target_dirs = string.split(',')
    print(f"Streaming: {streaming}")
    urls_all, file_extensions = filter_url_from_index(dir_structure, target_dirs)
    print(urls_all  )
    print(file_extensions)
    kwargs={
        "path":LOCAL_SCRIPT_PATH,
        "name": "custom_config",
        "cache_dir": CACHE_DIR,
        "data_files": urls_all,
        "split": 'train',
        "data_exts": file_extensions,
    }
    if not Version.IS_DATASETS_OUTDATED.value:
        kwargs['trust_remote_code']=True
    if streaming:
        kwargs['streaming'] = True
    ds = load_dataset(**kwargs,)
    # print(ds)
    return ds


def load_sciencedb(txt, streaming=True):
    if REMOTE:
        dir_structure = read_url_from_txt(path=txt)
        DirectoryTree()
        target_dir = input("请输入要下载的文件或者文件夹名称，以逗号分隔: ").split(',')
        urls_all, file_extensions = filter_url_from_index(dir_structure, target_dir)
    else:
        ds = load_dataset(LOCAL_FILE_PATH, cache_dir='.\cache_dir')
    kwargs = {
        "path": LOCAL_SCRIPT_PATH,
        "name": "custom_config",
        "cache_dir": CACHE_DIR,
        "data_files": urls_all,
        "split": 'train',
        "data_exts": file_extensions,
    }
    if not Version.IS_DATASETS_OUTDATED.value:
        kwargs['trust_remote_code'] = True
    if streaming:
        kwargs['streaming'] = True
    import aiohttp
    ds = load_dataset(**kwargs, storage_options={'client_kwargs': {'timeout': aiohttp.ClientTimeout(total=36000)}})
    # print(ds)
    # time.sleep(1)
    # preprocess_dataset(ds)

    return ds


if __name__ == '__main__':
    # 选择数据集url
    PATH = '7e0a4faa0d0649918ae3e94ef34b94af.txt'
    # 多种类型 小型
    PATH = '25d185f01b8c43b29420c48e50a11fad.txt'
    # PATH='8cf1ce3983ad4d5d8f45ee6469ab5dcc.txt'
    # 超大数据集
    # PATH = '91574142078b45c79d532d97b294ed44.txt'
    #
    # PATH = 'c0bd7f5c79a24e48849432629f59639f.txt'
    PATH = '533223505102110720.txt'
    # PATH = "new.txt"
    PATH = "2gb.txt"
    PATH = "8621e1b39c9a4acd87fee2be516d22ce.txt"
    load_sciencedb(PATH, streaming=False)
    read_url_from_txt("7e0a4faa0d0649918ae3e94ef34b94af.txt")
    read_url_from_txt("GSA_url.txt")
    #
    # PATH = 'b6a1d3f42b014fa9ae9cce04679a5e0f.txt'
    # dataset = load_dataset("mc4", "en", streaming=True, split="train")
    # ds = next(iter(dataset))
    iterable_ds = load_sciencedb(txt=PATH, streaming=False)
    # iterable_ds._format_type = 'arrow'
    # data_dict = {key: [row[key] for row in iterable_ds] for key in iterable_ds.features}

    # for example in iterable_ds:
    #     for column, value in example.items():
    #         if value is not None:
    #             # output = value if len(value) <= MAX_LEN else value[:MAX_LEN].decode() + '...'
    #             print(f"{column} loaded")
    #             break  # 如果只需要打印第一个非空列，找到后即可停止
    # ds = next(iter(iterable_ds))
    # print(ds)
    # ds = next(iter(iterable_ds))
    # print(ds)
