import os
from urllib.parse import quote

API_KEY='&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a'

def url_parser(url_lists):
    dir_structure = {}
    splits = {}
    clean_urls=[]
    file_extensions=[]
    for url in url_lists:
        clean_url = url.strip()
        file_extension = os.path.splitext(clean_url)[1][1:].lower()
        clean_url += API_KEY
        # clean_url = quote(clean_url, safe='/:=&?#+!,')
        parts = clean_url.strip().split('&')
        filepath_part = next(part for part in parts if part.startswith('path='))
        filename_part = next(part for part in parts if part.startswith('fileName='))

        filepath = filepath_part.split('=')[1]
        filename = filename_part.split('=')[1]

        dirs = filepath.strip('/').split('/')

        split = os.path.join(*dirs[:-1])
        if split not in splits:
            splits[split]=[]
        splits[split].append(clean_url)

        current_level = dir_structure
        for dir in dirs[:-1]:
            if dir not in current_level:
                current_level[dir] = {}
            current_level = current_level[dir]
        current_level[dirs[-1]] = (clean_url,file_extension)


        clean_urls.append(clean_url)
        file_extensions.append(file_extension)

    return dir_structure


def filter_url_from_index(directory_structure, target_dir):
    all_urls = []
    all_exts = []

    def recursive_search(current_dir, current_structure):
        # 如果当前文件夹是目标文件夹
        if current_dir == target_dir:
            collect_urls(current_structure)

        # 如果当前结构是字典，继续递归
        if isinstance(current_structure, dict):
            for sub_dir, sub_structure in current_structure.items():
                recursive_search(sub_dir, sub_structure)

    def collect_urls(structure):
        # 如果结构是字典，且包含文件名到URL的映射
        if isinstance(structure, dict):
            for value in structure.values():
                if isinstance(value, tuple):
                    # 如果值是列表，收集它
                    all_urls.append(value[0])
                    all_exts.append(value[1])
                elif isinstance(value, dict):
                    # 继续递归收集子文件夹中的URL
                    collect_urls(value)

    # 从顶层开始递归搜索
    for root_dir, root_structure in directory_structure.items():
        recursive_search(root_dir, root_structure)

    # 如果没有找到目标文件夹，提醒用户
    if not all_urls:
        print(f"目录 '{target_dir}' 不存在或没有包含文件。")

    return all_urls,all_exts
