import pandas as pd
from typing import List, Dict
from .base_model import BaseRecommendationModel

class TopProductModel(BaseRecommendationModel):
    def __init__(self, n_recommendations: int = 20):
        super().__init__(n_recommendations)
        self.product_scores = {}

    def fit(self, data: pd.DataFrame) -> None:
        """
        Compute most viewed products per advertiser
        
        Parameters:
        -----------
        data : pd.DataFrame
            DataFrame containing columns: advertiser_id, product_id
        """
        required_columns = ['advertiser_id', 'product_id']
        self._validate_data(data, required_columns)

        # Count views per product for each advertiser
        product_counts = data.groupby(['advertiser_id', 'product_id']).size()
        
        # Store top N products per advertiser
        self.product_scores = {
            adv: products.sort_values(ascending=False).head(self.n_recommendations)
            for adv, products in product_counts.groupby(level=0)
        }

    def get_top_product_recommendations(self, advertiser_id: str) -> List[Dict]:
        """
        Get top N most viewed products for an advertiser
        
        Parameters:
        -----------
        advertiser_id : str
            ID of the advertiser to get recommendations for
            
        Returns:
        --------
        List[Dict] : List of recommendations with product_id and score
        """
        if advertiser_id not in self.product_scores:
            return []

        products = self.product_scores[advertiser_id]
        return [
            {
                'product_id': product_id,
                'score': float(views),  # Convert numpy.int64 to float
                'model_type': 'top_product'
            }
            for product_id, views in products.items()
        ]