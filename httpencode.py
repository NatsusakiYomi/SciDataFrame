from urllib.parse import quote
import requests

url = "https://download.scidb.cn/download?fileId=f903c13c10c4add2430b523a1be02bab&path=/V2/DHB-G-166_1.81um_tiff/20240118_ljz_GHB-G-6_1.81um_recon_Export0073.tiff&fileName=20240118_ljz_GHB-G-6_1.81um_recon_Export0073.tiff&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
encoded_url = quote(url, safe='/:=&?#+!,')

print(f"原始URL: {url}")
print(f"编码URL: {encoded_url}")
req=requests.get(encoded_url)
print(req)
