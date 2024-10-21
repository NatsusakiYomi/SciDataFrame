from enum import Enum

class TrainingTask(Enum):
    MultiLabelClassification = "MultiLabelClassification"
    Recommendation = "Recommendation"
    ImageClassification = "ImageClassification"