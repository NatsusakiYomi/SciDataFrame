"""文件目录树工具"""
from cgi import print_directory

import pyarrow as pa


class DirectoryTree:

    def __init__(self):
        self.schema_dict = {
            'name': [],
            'type': [],
            'depth': [],
        }

    def print_directory_tree(self, directory, indent=0):
        """
        打印文件目录树,同时更新视图

        参数:
        directory (dict): 表示文件目录的字典
        indent (int): 当前缩进级别
        """
        file_count = 0
        for key, value in directory.items():
            # 打印当前文件夹或文件，前面加上适当数量的缩进
            file_flag=False
            if isinstance(value, tuple):
                file_flag=True
                file_count += 1
                if file_count == 4:
                    print(' ' * indent + '...')
                    self.schema_dict['name'].append(key)
                    self.schema_dict['type'].append('file' if file_flag else 'dir')
                    self.schema_dict['depth'].append(indent // 4)
                    continue
                elif file_count > 4:
                    self.schema_dict['name'].append(key)
                    self.schema_dict['type'].append('file' if file_flag else 'dir')
                    self.schema_dict['depth'].append(indent // 4)
                    continue
            print(' ' * indent + str(key))
            self.schema_dict['name'].append(key)
            self.schema_dict['type'].append('file' if file_flag else 'dir')
            self.schema_dict['depth'].append(indent//4)

            # 如果 value 是字典，则递归调用 print_directory_tree
            if isinstance(value, dict):
                self.print_directory_tree(value, indent + 4)

    def get_schema(self,structure):
        """

        :param directory: 数据集结构
        :return: 数据集的视图 -> arrow schema
        """
        self.print_directory_tree(structure)
        return pa.table(self.schema_dict)
