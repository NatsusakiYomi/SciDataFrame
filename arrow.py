class MyClass:
    count = 0

    def __init__(self, value):
        self.value = value
        MyClass.count += 1

    @classmethod
    def how_many(cls):
        return cls.count

    @classmethod
    def create_instance(cls, value):
        return cls(value)  # 创建并返回一个MyClass的实例

# 访问类属性
print(MyClass.how_many())  # 调用类方法，输出: 0

# 创建实例
instance = MyClass.create_instance(5)
print(instance.value)  # 输出: 5
instance_2 = MyClass.create_instance(5)
print(MyClass.how_many())  # 输出: 1
