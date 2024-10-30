class PreloadGenerator:
    def __init__(self, it):
        self.iterator=it
        self.current_batch=next(self.iterator)

    def load_next_batch(self):
        try:
            self.current_batch = next(self.iterator)  # 加载下一个批次
        except StopIteration:
            self.current_batch = None  # 没有更多数据

    def __iter__(self):
        while self.current_batch is not None:
            yield self.current_batch  # 返回当前批次
            self.load_next_batch()  # 预加载下一个批次