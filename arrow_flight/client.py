import pyarrow as pa
import pyarrow.flight as fl
import model.DataFrame
from model import DataFrame
import pickle

# 创建 FlightClient 实例
client = fl.FlightClient('grpc://localhost:8815')
start_train = False
while not start_train:
    dataset_id = input("Enter dataset id: ")+".txt"

    # 获取 schema
    action = fl.Action("get_schema", dataset_id.encode("utf-8"))
    schema_results = client.do_action(action)
    schema=None
    # 处理返回的 schema
    for schema_bytes in schema_results:
        schema = pa.ipc.open_file(schema_bytes.body).read_all()
        print("Received Schema:", schema)
        print("Received bytes:", schema.nbytes)
        # 重建文件目录树

    # 用户输入文件夹路径
    folder_path = input("Enter folder path to retrieve files: ")
    ticket = fl.Ticket(folder_path.encode('utf-8'))
    action=fl.Action("put_folder_path", folder_path.encode("utf-8"))
    client.do_action(action)
    df = DataFrame(schema=schema)

    #进行数值特征分析
    if input("Analyze num or not: ").lower() == 'yes':
        action = fl.Action("numerical_analysis", folder_path.encode("utf-8"))
        results = client.do_action(action)
        result=None
        # 处理返回的result
        for result_bytes in results:
            numerical_feature = pickle.loads(result_bytes.body)
            print("Received Numerical features:", numerical_feature)
            print("Received bytes:", result_bytes.body.size)

    # 进行数据预处理
    if input("Preprocess or not: ").lower() == 'yes':
        action = fl.Action("recommendation_preprocess", folder_path.encode("utf-8"))
        results = client.do_action(action)
        result = None
        # 处理返回的result
        for result_bytes in results:
            result = pickle.loads(result_bytes.body)
            print("Received Message:", result)
            print("Received bytes:", result_bytes.body.size)

    # 进行streaming

    if input("Streaming or not: ").lower() == 'yes':
        client.do_action("streaming","True".encode("utf-8"))
        reader = client.do_get(ticket)
        for index, chunk in enumerate(reader):
            print(f'Row {index + 1} received: {chunk.data.nbytes} bytes')
            df.concat(pa.Table.from_batches([chunk.data]))
    else:
        # 使用 do_get 获取文件数据
        reader = client.do_get(ticket)
        read_table = reader.read_all()
        df.concat(read_table)
        print(f'{read_table.num_rows} rows received: {read_table.nbytes} bytes')

    start_train = input("Start train? ").lower() == 'yes'

# from lightfm import LightFM
# from lightfm.data import Dataset
#
# # 创建 LightFM 数据集
# dataset = Dataset()
#
# # 构建用户和产品特征
# dataset.fit((x for x in df['user_id']), (x for x in df['product_id']))
#
# # 将数据转换为交互矩阵
# (interactions, weights) = dataset.build_interactions(
#     ((row['user_id'], row['product_id'], row['interaction']) for _, row in df.iterrows())
# )
#
# # 训练 LightFM 模型（可以选择隐式或显式反馈）
# model = LightFM(loss='warp')  # 'warp' 适合隐式反馈数据
# model.fit(interactions, epochs=30, num_threads=2)
#
# # 模型预测
# user_id = 1
# scores = model.predict(user_id, np.arange(interactions.shape[1]))
# top_items = np.argsort(-scores)
# print("Recommended products for user:", top_items[:10])