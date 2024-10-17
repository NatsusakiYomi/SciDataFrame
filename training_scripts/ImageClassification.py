from transformers import AutoFeatureExtractor
from datasets import Dataset
from torchvision.transforms import Compose, Resize, ToTensor, Normalize
import torchvision.transforms as transforms
from PIL import Image
import io
import random

# 假设 `table` 是包含 TIFF 二进制数据的 PyArrow 表格
# 其中 'image_data' 列存储了 TIFF 格式的二进制文件 (BinaryScalar)
def ImageClassification(df):
    def binary_to_pil(binary_scalar):
        """将 BinaryScalar 转换为 PIL 图像"""
        # 将 BinaryScalar 转换为字节数据
        image_bytes = binary_scalar.as_py()
        # 将字节数据包装为字节流
        image_stream = io.BytesIO(image_bytes)
        # 用 PIL 打开该字节流并返回图像对象
        return Image.open(image_stream)

    # 从 PyArrow 表格的 'image_data' 列提取出二进制数据
    binary_column = df.data['binary']

    # 将 PyArrow 列中的每个 BinaryScalar 转换为 PIL 图像
    pil_images = [binary_to_pil(binary_scalar) for binary_scalar in binary_column]
    labels = [random.randint(0,1) for i in range(len(pil_images))]
    # 使用 ViT 模型的特征提取器（也可以使用其他模型的特征提取器）
    feature_extractor = AutoFeatureExtractor.from_pretrained('google/vit-base-patch16-224')

    # 定义预处理函数：调整大小，转换为张量，归一化
    normalize = Normalize(mean=feature_extractor.image_mean, std=feature_extractor.image_std)
    transform = Compose([
        transforms.RandomHorizontalFlip(),
        transforms.RandomRotation(30),
        transforms.ColorJitter(brightness=0.2, contrast=0.2, saturation=0.2, hue=0.2),
        Resize((224, 224)),
        ToTensor(),
        normalize])

    # 将 PIL 图像列表和标签转换为 dataset 格式
    dataset_dict = {'image': pil_images, 'label': labels}
    dataset = Dataset.from_dict(dataset_dict)

    # 定义预处理函数，转换 PIL 图像为 pixel_values
    def preprocess_images(example):
        example['pixel_values'] = [transform(image.convert("RGB")) for image in example['image']]
        return example

    # 对数据集进行预处理
    dataset = dataset.map(preprocess_images, batched=True)

    # 删除原始的 'image' 列，只保留 pixel_values 和 label 列
    dataset = dataset.remove_columns(['image'])

    # 查看数据集
    # print(dataset[0])

    train_test_split = dataset.train_test_split(test_size=0.2)
    train_dataset = train_test_split['train']
    test_dataset = train_test_split['test']

    from transformers import ViTForImageClassification, TrainingArguments, Trainer
    model = ViTForImageClassification.from_pretrained('google/vit-base-patch16-224', num_labels=2,ignore_mismatched_sizes=True)

    training_args = TrainingArguments(
        output_dir="./results",
        evaluation_strategy="epoch",
        per_device_train_batch_size=2,
        num_train_epochs=1,
        save_steps=500,
        save_total_limit=2,
        remove_unused_columns=False,
        logging_dir="./logs",
        logging_steps=10,
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=test_dataset,  # 简单起见，用同样的数据集作验证
        tokenizer=feature_extractor,
    )

    trainer.train()

    predictions = trainer.predict(test_dataset)
    pred_labels = predictions.predictions.argmax(axis=-1)
    print(pred_labels)