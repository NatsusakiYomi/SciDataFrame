import numba
import timeit
@numba.njit
def modified_url_preprocess(url):
    return url.replace("http", "https")

@numba.njit
def process_urls(urls):
    results = []
    for url in urls:
        results.append(modified_url_preprocess(url))  # 调用已经修饰的函数
    return results

urls = ["http://example.com", "http://test.com"]
TIMES = 10

elapsed_time = timeit.timeit(lambda: process_urls(urls),
                             number=TIMES) / TIMES  # 运行100次
print(f"Average execution time per run: {elapsed_time :.6f} seconds")
# print(process_urls(urls))
