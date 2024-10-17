from lightfm import LightFM
from lightfm import LightFM
from lightfm.evaluation import precision_at_k
from scipy.sparse import coo_matrix
import numpy as np

def RecommendationModel(df):
    df = df.data.to_pandas()
    # 创建 LightFM 数据集
    df['user_index'] = df['column1'].astype('category').cat.codes
    df['item_index'] = df['column2'].astype('category').cat.codes

    # 3. 构建用户-产品交互矩阵
    num_users = df['user_index'].nunique()
    num_items = df['item_index'].nunique()

    # 使用 COO 格式构建稀疏矩阵
    interaction_matrix = coo_matrix((df['column3'], (df['user_index'], df['item_index'])), shape=(num_users, num_items))

    # 4. 训练模型
    model = LightFM(loss='logistic')  # 使用 WARP 损失函数
    model.fit(interaction_matrix, epochs=1)

    # 5. 进行预测
    # 选择要进行预测的用户 ID
    user_id = 1  # 替换为你感兴趣的用户 ID
    user_index = df[df['column1'] == user_id]['user_index'].values[0]  # 获取用户的索引

    # 生成用户的推荐
    scores = model.predict(np.array([x for x in range(100)]), np.array([x for x in range(100)]))
    top_items = scores.argsort()[::-1][:10]  # 获取前 10 个推荐的产品索引

    # 将产品索引转换回产品 ID
    item_ids = df['column2'].astype('category').cat.categories
    recommended_items = item_ids[top_items]

    print("推荐的产品 ID:", recommended_items)
