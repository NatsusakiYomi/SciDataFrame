import sys
import os

import importlib.metadata
from enum import Enum


def parse_version(version_str):
    return list(map(int, version_str.split(".")))


# 库的版本检查
class Version(Enum):
    CURRENT_DATASETS_VERSION = parse_version(importlib.metadata.version("datasets"))
    OUTDATED_DATASETS_VERSION = parse_version('2.20')
    IS_DATASETS_OUTDATED = True if CURRENT_DATASETS_VERSION <= OUTDATED_DATASETS_VERSION else False


class Path(Enum):
    PRORJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
