import sys
sys.path.append('C:\\Users\\Yomi\\PycharmProjects\\SDB2')
import pyarrow as pa
import pyarrow.flight as fl
import model.DataFrame
from model import DataFrame
import pickle
import time
import grpc
# server = grpc.server(..., options=[('grpc.so_reuseport', 1)])
# server.add_insecure_port('[::]:8815')  # 监听所有接口，包括 IPv6
import numpy as np


# 创建 FlightClient 实例

# upload_descriptor = pa.flight.FlightDescriptor.for_path("new.txt")
# flight = client.get_flight_info(upload_descriptor)
# descriptor = flight.descriptor
def main(dataset_id,folder_path,is_analyze,is_preprocess,is_get_dataset_str,is_streaming):
    start_train = False
    while not start_train:
        client = pa.flight.connect("grpc://localhost:8815")

        # 获取 schema
        action = fl.Action("get_schema", dataset_id.encode("utf-8"))
        client.do_action(action)
        schema_results = client.do_action(action)
        schema=None
        # 处理返回的 schema
        for schema_bytes in schema_results:
            with pa.ipc.open_file(schema_bytes.body) as schema_file:
                schema=schema_file.read_all()
                print("Received Schema:", schema)
                print("Received bytes:", schema.nbytes)
            # 重建文件目录树

        # 用户输入文件夹路径
        ticket = fl.Ticket(folder_path.encode('utf-8'))
        action=fl.Action("put_folder_path", folder_path.encode("utf-8"))
        client.do_action(action)
        df = DataFrame(schema=schema)

        # 进行数值特征分析
        if is_analyze:
            action = fl.Action("numerical_analysis", "True".encode("utf-8"))
            # client.do_action(action)
            results = client.do_action(action)
            result=None
            # 处理返回的result
            for result_bytes in results:
                numerical_feature = pickle.loads(result_bytes.body)
                print("Received Numerical features:", numerical_feature)
                print("Received bytes:", result_bytes.body.size)
        else:
            action = fl.Action("numerical_analysis", "False".encode("utf-8"))
            results = client.do_action(action)
        # time.sleep(5)
        # 进行数据预处理
        if is_preprocess:
            action = fl.Action("recommendation_preprocess", "True".encode("utf-8"))
            results = client.do_action(action)
            result = None
            # 处理返回的result
            for result_bytes in results:
                result = pickle.loads(result_bytes.body)
                print("Received Message:", result)
                print("Received bytes:", result_bytes.body.size)
        else:
            action = fl.Action("recommendation_preprocess", "False".encode("utf-8"))
            results = client.do_action(action)
        # 进行streaming
        # time.sleep(5)
        if is_get_dataset_str:
            action = fl.Action("get_dataset_str", "True".encode("utf-8"))
            results = client.do_action(action)

        if is_streaming:
            action = fl.Action("streaming", "True".encode("utf-8"))
            client.do_action(action,pa.flight.FlightCallOptions(timeout=60))
            reader = client.do_get(ticket)
            for index, chunk in enumerate(reader):
                print(f'Row {index + 1} received: {chunk.data.nbytes} bytes')
                df.concat(pa.Table.from_batches([chunk.data]))
        else:
            # 使用 do_get 获取文件数据
            action = fl.Action("streaming", "False".encode("utf-8"))
            client.do_action(action,pa.flight.FlightCallOptions(timeout=60))
            reader = client.do_get(ticket)
            read_table = reader.read_all()
            df.concat(read_table)
            print(f'{read_table.num_rows} rows received: {read_table.nbytes} bytes')
        client.close()
        start_train = input("Start train? ").lower() == 'yes'
    #
    # from lightfm import LightFM
    # from lightfm import LightFM
    # from lightfm.evaluation import precision_at_k
    # from scipy.sparse import coo_matrix
    #
    # df=df.data.to_pandas()
    # # 创建 LightFM 数据集
    # df['user_index'] = df['column1'].astype('category').cat.codes
    # df['item_index'] = df['column2'].astype('category').cat.codes
    #
    # # 3. 构建用户-产品交互矩阵
    # num_users = df['user_index'].nunique()
    # num_items = df['item_index'].nunique()
    #
    # # 使用 COO 格式构建稀疏矩阵
    # interaction_matrix = coo_matrix((df['column3'], (df['user_index'], df['item_index'])), shape=(num_users, num_items))
    #
    # # 4. 训练模型
    # model = LightFM(loss='logistic')  # 使用 WARP 损失函数
    # model.fit(interaction_matrix, epochs=1)
    #
    # # 5. 进行预测
    # # 选择要进行预测的用户 ID
    # user_id = 1  # 替换为你感兴趣的用户 ID
    # user_index = df[df['column1'] == user_id]['user_index'].values[0]  # 获取用户的索引
    #
    # # 生成用户的推荐
    # scores = model.predict(np.array([x for x in range(100)]),np.array([x for x in range(100)]))
    # top_items = scores.argsort()[::-1][:10]  # 获取前 10 个推荐的产品索引
    #
    # # 将产品索引转换回产品 ID
    # item_ids = df['column2'].astype('category').cat.categories
    # recommended_items = item_ids[top_items]
    #
    # print("推荐的产品 ID:", recommended_items)

    # 2 HF
    from datasets import Dataset
    dataset = Dataset.from_pandas(df.data)
    from transformers import Trainer, TrainingArguments
    from transformers import BertForSequenceClassification
    pa.ChunkedArray

    model = BertForSequenceClassification.from_pretrained('bert-base-uncased', num_labels=2)

    # 定义训练参数
    training_args = TrainingArguments(
        output_dir='./results',
        evaluation_strategy='epoch',
        learning_rate=5e-5,
        per_device_train_batch_size=16,
        num_train_epochs=1,
    )

    # 使用 Trainer 进行训练
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=dataset,
    )

    trainer.train()

    predictions = trainer.predict(dataset)
    print(predictions.predictions)

if __name__ == '__main__':
    dataset_id='new'+ ".txt"
    dataset_path='dataset.csv'
    # dataset_path = input("Enter folder path to retrieve files: ")
    # dataset_id = input("Enter dataset id: ") + ".txt"
    # is_analyze = input("Analyze num or not: ").lower() == 'yes'
    # is_preprocess = input("Preprocess or not: ").lower() == 'yes'
    # is_get_dataset_str = input("Get dataset as str table or not: ").lower() == 'yes'
    # is_streaming = input("Streaming or not: ").lower() == 'yes'
    is_analyze = False
    is_preprocess = False
    is_get_dataset_str = True
    is_streaming = False
    main(dataset_id,dataset_path,is_analyze,is_preprocess,is_get_dataset_str,is_streaming)