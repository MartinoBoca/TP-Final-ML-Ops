from abc import ABC, abstractmethod
from typing import List, Dict
import pandas as pd

class BaseRecommendationModel(ABC):
    def __init__(self, n_recommendations: int = 20):
        self.n_recommendations = n_recommendations

    @abstractmethod
    def fit(self, data: pd.DataFrame) -> None:
        """Train the model with the given data"""
        pass

    @abstractmethod
    def predict(self, advertiser_id: str) -> List[Dict]:
        """Generate recommendations for a specific advertiser"""
        pass

    def _validate_data(self, data: pd.DataFrame, required_columns: List[str]) -> None:
        """Validate that the input data has the required columns"""
        missing_cols = set(required_columns) - set(data.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")