import datasets

class DataOperator:
    def __init__(self,dataset_dict):
        self.dataset_dict=dataset_dict
        self.dataset=self.dataset_dict['allofdata']
    def get_dataset(self):
        return self.dataset

    def split_dataset(self, test_size: float = 0.2):
        return self.dataset.train_test_split(test_size=test_size)

    def map_dataset(self, function):
        return self.dataset_dict.map(function)

    def filter_dataset(self, function):
        return self.dataset_dict.filter(function)

    def select_dataset(self, index_list: list):
        return self.dataset.select(index_list)

    def unique_dataset(self, column):
        return self.dataset.unique(column)

    def sort_dataset(self, column):
        return self.dataset.sort(column)

    def flat_dataset(self):
        return self.dataset_dict.flatten()



