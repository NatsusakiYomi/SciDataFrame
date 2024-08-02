"""文件目录树工具"""


def print_directory_tree(directory, indent=0):
    """
    打印文件目录树

    参数:
    directory (dict): 表示文件目录的字典
    indent (int): 当前缩进级别
    """
    for key, value in directory.items():
        # 打印当前文件夹或文件，前面加上适当数量的缩进
        if directory[key] is None:
            continue
        print(' ' * indent + str(key))

        # 如果 value 是字典，则递归调用 print_directory_tree
        if isinstance(value, dict):
            print_directory_tree(value, indent + 4)
