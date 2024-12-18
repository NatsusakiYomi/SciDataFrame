import requests
import tempfile
import os


class UnstructuredUtils:

    @staticmethod
    def parse_unstructured_json(file_path):
        if file_path is None:
            print("file path is none")
            return
        print("file_path=", file_path)
        file_name, mime_type = UnstructuredUtils.retrieve_file_name_and_mime_type(file_path)
        print("file_name=", file_name, ", mime_type=", mime_type)
        if file_name is None or mime_type is None:
            print("file name is none, or mime type is none")
            return
        url = "https://api.unstructured.io/general/v0/general"
        headers = {
            'accept': 'application/json',
            'unstructured-api-key': 'KmLJJXr3yQ0hB1tbgTM9ergPsyA535'
        }
        files = [
            ('files', (file_name, open(file_path, 'rb'), mime_type))
        ]
        response = requests.post(url, headers=headers, files=files, timeout=60)
        os.remove(file_path)
        if response.status_code != 200:
            print("unstructured api error, code=", response.status_code, ", content=", response.content)
            return
        return response.json()

    @staticmethod
    def download_file_from_url(file_url, suffix):
        r = requests.get(file_url)
        if r.status_code == 200:
            # 创建临时文件
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as temp_file:
                # 将下载的文件内容写入临时文件
                temp_file.write(r.content)
                # 获取临时文件路径
                temp_file_path = temp_file.name
        else:
            print("Failed to download file from URL:", file_url)
            temp_file_path = None
        return temp_file_path

    @staticmethod
    def retrieve_file_name_and_mime_type(file_path):
        file_name = os.path.basename(file_path)
        file_suffix = file_name.split(".")[1]
        if file_suffix == "pdf":
            return file_name, "application/pdf"
        elif file_suffix in {"doc", "docx"}:
            return file_name, "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        elif file_suffix == "pptx":
            return file_name, "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        elif file_suffix == "md":
            return file_name, "application/octet-stream"
        elif file_suffix == "html":
            return file_name, "text/html"
        elif file_suffix == "xml":
            return file_name, "application/xml"
        elif file_suffix == "txt":
            return file_name, "text/plain"
        else:
            return file_name, None
