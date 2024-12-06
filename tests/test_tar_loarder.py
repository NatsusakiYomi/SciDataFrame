import tarfile
import urllib.request

url = "https://download.cncb.ac.cn/3cdb/fasta/Ciona_intestinalis/Ciona_intestinalis.KH.dna.chromosome.MT.fa.gz"

# 打开远程文件流
with urllib.request.urlopen(url) as response:
    # 用 tarfile 读取流
    with tarfile.open(fileobj=response, mode="r|gz") as tar:
        for member in tar:
            print(member.name)  # 输出每个文件/文件夹的名字