import os
import time

import numba

from SciDBLoader import read_url_from_txt, URL_TXT_ROOT

API_KEY = '&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a'


def url_preprocess(url):
    clean_url = url.strip()
    file_extension = os.path.splitext(clean_url)[1][1:].lower()
    clean_url += API_KEY
    # clean_url = quote(clean_url, safe='/:=&?#+!,')
    parts = clean_url.strip().split('&')
    filepath_part = None
    for part in parts:
        if part.startswith("path="):
            filepath_part = part
    if filepath_part:
        filepath = filepath_part.split('=')[1]
        # filename = filename_part.split('=')[1]
        # split = filepath.rsplit('/', 1)[0]
        dirs = filepath.strip('/').split('/')
    else:
        # split = parts[0].rsplit('/', 1)[0]
        dirs = parts[0].split('/')[2:]
    return dirs, clean_url, file_extension

# @numba.jit
def custom_splitext(url):
    index = url.rfind('.')
    if index != -1:
        return url[index:]
    return ''

# @numba.jit
def modified_url_preprocess(url):
    clean_url = url.strip()
    file_extension = os.path.splitext(clean_url)[1][1:].lower()
    clean_url += API_KEY
    # clean_url = quote(clean_url, safe='/:=&?#+!,')
    parts = clean_url.strip().split('&')
    filepath_part = None
    # filepath_part = next(part for part in parts if part.startswith('path='))
    # filename_part = next(part for part in parts if part.startswith('fileName='))
    for part in parts:
        if part.startswith("path="):
            filepath_part = part
    if filepath_part:
        filepath = filepath_part.split('=')[1]
        # filename = filename_part.split('=')[1]
        # split = filepath.rsplit('/', 1)[0]
        split = filepath[:filepath.rfind('/')]
        dirs = filepath.strip('/').split('/')
    else:
        # split = parts[0].rsplit('/', 1)[0]
        dirs = parts[0].split('/')[2:]
        split = parts[0][:parts[0].rfind('/')]
        # split=''.join(dirs[:-1])



    return dirs, clean_url, file_extension, split


def get_deepest_level(i, dirs, current_level):
    for dir in dirs[i][:-1]:
        if dir not in current_level:
            current_level[dir] = {}
        current_level = current_level[dir]
    return current_level


def loop_construct(current, dirs, clean_urls, file_extensions, dir_structure):
    current_level = dir_structure
    for dir in dirs[current][:-1]:
        if dir not in current_level:
            current_level[dir] = {}
        current_level = current_level[dir]
    current_level[dirs[current][-1]] = (clean_urls[current], file_extensions[current])


def url_parser(url_lists):
    dir_structure = {}
    splits = {}
    clean_urls = []
    file_extensions = []
    for url in url_lists:
        dirs, clean_url, file_extension = url_preprocess(url)

        # split = os.path.join(*dirs[:-1])

        current_level = dir_structure
        for dir in dirs[:-1]:
            if dir not in current_level:
                current_level[dir] = {}
            current_level = current_level[dir]
        current_level[dirs[-1]] = (clean_url, file_extension)

    return dir_structure

def index_construct_loop(last_split,url_list, size):
    cnt = 0
    dirs = []
    clean_urls = []
    file_extensions = []
    partition_index = [(0, 0)]
    for index, url in enumerate(url_list):
        dir, clean_url, file_extension, split = modified_url_preprocess(url)

        dirs.append(dir)
        clean_urls.append(clean_url)
        file_extensions.append(file_extension)

        if cnt != 0 and cnt < size:
            cnt += 1
            continue
        if cnt == size:
            cnt = 0
            last_split = split
            partition_index.append((index, 1))
        if split != last_split:
            partition_index.append((index, 0))
            cnt += 1
        last_split = split
    return dirs, clean_urls, file_extensions, partition_index


def modified_url_parser(url_lists, size):
    """
    Use local order
    """
    dir_structure = {}
    splits = {}
    # dir_structure = Dict.empty(key_type=types.unicode_type, value_type=types.unicode_type)
    dirs = []
    clean_urls = []
    file_extensions = []
    partition_index = [(0, 0)]
    last_dirs, _, _, last_split = modified_url_preprocess(url_lists[0])



    dirs, clean_urls, file_extensions, partition_index= index_construct_loop(last_split, url_lists, size)

    partition_index.append((len(url_lists), 1))
    for _i in range(1, len(partition_index)):

        current_index = partition_index[_i][0]
        last_index = partition_index[_i - 1][0]

        if partition_index[_i][1] == 0:
            current_level = get_deepest_level(last_index, dirs, dir_structure)
            for i in range(last_index, current_index):
                current_level[dirs[i][-1]] = (clean_urls[i], file_extensions[i])
        else:
            for i in range(last_index, current_index):
                loop_construct(i, dirs, clean_urls, file_extensions, dir_structure)

    return dir_structure


if __name__ == "__main__":
    path = "633694460986785795.txt"
    path = "e2393619cb65490fbd6932e943a363fc.txt"
    path = "4d9f04de7dec49008faad35d85f28041.txt"
    # path = "7e0a4faa0d0649918ae3e94ef34b94af.txt"
    path="urls_2.txt"
    url_path = URL_TXT_ROOT
    url_path = "./"
    with open(os.path.join(url_path, path), 'r', encoding='utf-8') as f:
        url_lists = f.readlines()
        URL_LIST = url_lists
        # print(modified_url_parser(url_lists, 10))3
        import timeit
        from UrlGenerator import generate_line

        dir_structure_1 = modified_url_parser(url_lists, 10)
        dir_structure_2 = url_parser(url_lists)

        # url_lists = generate_line(10)
        TIMES = 100

        elapsed_time = timeit.timeit(lambda: url_parser(url_lists),
                                     number=TIMES) / TIMES  # 运行100次
        print(f"Average execution time per run: {elapsed_time :.6f} seconds")
        time.sleep(1)
        elapsed_time_m = timeit.timeit(lambda: modified_url_parser(url_lists, 10),
                                       number=TIMES) / TIMES  # 运行100次
        print(f"Average execution time per run: {elapsed_time_m :.6f} seconds")
        print(f"Execution time reduced per run: {(elapsed_time - elapsed_time_m) / elapsed_time :.2%} seconds")
