import os

os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'
# 2 HF

from sklearn.preprocessing import LabelEncoder


def BertMultiClassification(df):
    from datasets import Dataset
    dataset = Dataset(df.data)
    import pandas as pd
    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments
    from torch.utils.data import Dataset

    # 文本标签编码
    label_encoder = LabelEncoder()
    encoded_labels = label_encoder.fit_transform(dataset['label'])  # 编码标签
    dataset = dataset.remove_columns('label')  # 移除原有标签列
    dataset = dataset.add_column('label', encoded_labels.tolist())  # 替换为编码后的标签

    model_name = 'distilbert-base-uncased'
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    # 定义预处理函数
    def preprocess_function(examples):
        return tokenizer(examples['content'], truncation=True, padding='max_length', max_length=128)

    # 应用预处理
    encoded_dataset = dataset.map(preprocess_function, batched=True)

    # 划分训练集和测试集
    train_test_split = encoded_dataset.train_test_split(test_size=0.2)
    train_dataset = train_test_split['train'].select(range(128))
    test_dataset = train_test_split['test'].select(range(10))

    # 添加 labels
    # train_dataset = train_dataset.rename_column("label", "labels")
    # test_dataset = test_dataset.rename_column("label", "labels")

    # 训练模型
    model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=len(label_encoder.classes_))
    training_args = TrainingArguments(
        output_dir='./results',
        num_train_epochs=1,
        per_device_train_batch_size=32,
        save_steps=10_000,
        save_total_limit=2,
        logging_dir='./logs',
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=test_dataset,
    )

    trainer.train()

    # 模型预测
    predictions = trainer.predict(test_dataset)
    pred_labels = predictions.predictions.argmax(axis=-1)
    print(pred_labels)
