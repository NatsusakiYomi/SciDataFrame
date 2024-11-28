import time

import requests
from bs4 import BeautifulSoup

urls_list=[]

def get_download_links(folder_url,f):
    response = requests.get(folder_url)
    response.raise_for_status()  # 检查请求是否成功

    soup = BeautifulSoup(response.text, 'html.parser')
    # time.sleep(5)
    # links = []

    # 解析所有的超链接
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and not href.startswith('?') and not href.startswith('/') and not href.startswith('..'):
            if href.endswith('.gz'):
                urls_list.append(folder_url + href)
                f.write(folder_url + href + '\n')
                continue
            # links.append(folder_url + href)
            if '.' not in href:

                print(f"url in {get_download_links(folder_url + href,f)} got, {len(urls_list)} records in total.")

    return folder_url

# 替换为目标文件夹路径
folder_url = "https://dap.ceda.ac.uk/neodc/avhrr3_metop_b/data/l1b/v1-0N/2023/"
with open("./urls_2.txt", "a+") as f:
    get_download_links(folder_url,f)

# 输出所有下载链接
print(len(urls_list))
