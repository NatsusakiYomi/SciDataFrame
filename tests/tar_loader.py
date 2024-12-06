import tarfile,gzip
import urllib.request
import io

url = "https://download.cncb.ac.cn/3cdb/fasta/Ciona_intestinalis/Ciona_intestinalis.KH.dna.chromosome.MT.fa.gz"
t=gzip.open("C:\\Users\\Yomi\\Downloads\\Ciona_intestinalis.KH.dna.chromosome.MT.fa.gz",'rt')
# 打开远程文件流
with urllib.request.urlopen(url) as response:
    print(response.info())  # 打印响应头，确认是 gzip 文件
    print(response.status)  # 确认状态码是 200
    data=response.read()
    gzip_data = io.BytesIO(data)
    # 用 tarfile 读取流
    with tarfile.open(fileobj=gzip_data, mode="r:gz") as tar:
        for member in tar:
            print(member.name)  # 输出每个文件/文件夹的名字