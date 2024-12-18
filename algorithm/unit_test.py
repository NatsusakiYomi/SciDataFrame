import timeit
URL="https://download.scidb.cn/download?fileId=2406aae475695757fe9300638d6dfee3&path=/V2/Vayyar雷达数据集/原始点云数据集/jump/people2_jump_2.xlsx&fileName=people2_jump_2.xlsx"
url_lists=[URL for i in range(10000)]
def parse():
    for url in url_lists:
        split = url.rsplit('/', 1)[0]
        dirs = url.split('/')[2:]
def m_parse():
    for url in url_lists:
        split = url[:url.rfind('/')]
        dirs = url.split('/')[2:]

TIMES=1000
elapsed_time = timeit.timeit(lambda: parse(),
                             number=TIMES) / TIMES  # 运行100次
print(f"Average execution time per run: {elapsed_time :.6f} seconds")

elapsed_time_m = timeit.timeit(lambda: m_parse(),
                               number=TIMES) / TIMES  # 运行100次
print(f"Average execution time per run: {elapsed_time_m :.6f} seconds")
