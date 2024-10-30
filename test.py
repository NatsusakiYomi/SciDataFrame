import unittest

from SciDBLoader import load_sciencedb


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
        self.txt = 'new.txt'

    # def test_load_txt_4(self):
    #     self.txt = '557575362591064064.txt'


if __name__ == '__main__':
    unittest.main()
# 运行测试
def main() -> None:
    result = asyncio.run(supervisor())
    print(f'Answer: {result}')
async def supervisor() -> int:
    spinner = asyncio.create_task(spin('thinking!'))
    print(f'spinner object: {spinner}')
    result = await slow()
    spinner.cancel()
    return result

import asyncio
import itertools
async def spin(msg: str) -> None:
    for char in itertools.cycle(r'\|/-'):
        status = f'\r{char} {msg}'
        print(status, flush=True, end='')
        try:
            await asyncio.sleep(.1)
        except asyncio.CancelledError:
            break
    blanks = ' ' * len(status)
    print(f'\r{blanks}\r', end='')
async def slow() -> int:
    await asyncio.sleep(3)
    return 42