from abc import ABCMeta, abstractmethod
import pyarrow


class ParserInterface(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def description(self):
        pass

    @abstractmethod
    def parse(self, file_url) -> pyarrow.Table:
        pass
