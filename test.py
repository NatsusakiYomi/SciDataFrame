import unittest

from loadscienceDB import load_sciencedb


class TestLoadDataset(unittest.TestCase):

    def tearDown(self):
        result = load_sciencedb(self.txt)
        self.assertIsNotNone(result)
        print(result)
        print(next(iter(result)))
    #
    # def test_load_txt_1(self):
    #     self.txt = 'DM3_f263bd9132e824ab3a715244942a91c4.txt'
    #
    # def test_load_txt_2(self):
    #     self.txt = 'b6a1d3f42b014fa9ae9cce04679a5e0f.txt'

    def test_load_txt_3(self):
        self.txt = '533223505102110720.txt'

    # def test_load_txt_4(self):
    #     self.txt = '557575362591064064.txt'


if __name__ == '__main__':
    unittest.main()
# 运行测试
