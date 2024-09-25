import concurrent.futures
from datasets import load_dataset
import numpy as np


MAX_WORKERS=15

def split_list(lst, num_parts):
    # 确定每个部分的大小
    k, m = divmod(len(lst), num_parts)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(num_parts)]

# 定义一个函数，用于加载单个分片或文件
def load_single_shard(split_name):
    # 这里可以设置其他参数，比如streaming=True，选择具体的数据集
    return load_dataset("your_dataset_name", split=split_name, streaming=True)

def multi_thread(urls):
    # 使用 ThreadPoolExecutor 来并行加载
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        # 提交每个分片的加载任务到线程池中
        future_to_split = {executor.submit(load_single_shard, split): split for split in split_list(urls,MAX_WORKERS)}

        # 获取加载结果
        for future in concurrent.futures.as_completed(future_to_split):
            split_name = future_to_split[future]
            try:
                dataset = future.result()
                # 处理每个数据集分片的流式数据
                for example in dataset:
                    print(f"{split_name} example:", example)
            except Exception as exc:
                print(f"{split_name} generated an exception: {exc}")
