import os
from urllib.parse import quote

API_KEY='&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a'

def url_parser(url_lists):
    file_structure = {}
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

        current_level = file_structure
        for dir in dirs[:-1]:
            if dir not in current_level:
                current_level[dir] = {}
            current_level = current_level[dir]
        current_level[dirs[-1]] = None


        clean_urls.append(clean_url)
        file_extensions.append(file_extension)

    return clean_urls,file_extensions,splits,file_structure

