import pandas as pd
from typing import List, Dict
from .base_model import BaseRecommendationModel

class TopCTRModel(BaseRecommendationModel):
    def __init__(self, n_recommendations: int = 20):
        super().__init__(n_recommendations)
        self.ctr_scores = {}

    def fit(self, data: pd.DataFrame) -> None:
        """
        Compute CTR scores for each product per advertiser
        
        Parameters:
        -----------
        data : pd.DataFrame
            DataFrame containing columns: advertiser_id, product_id, type (impression/click)
        """
        required_columns = ['advertiser_id', 'product_id', 'type']
        self._validate_data(data, required_columns)

        # Group by advertiser and product
        grouped = data.groupby(['advertiser_id', 'product_id', 'type']).size().unstack(fill_value=0)
        
        # Calculate CTR (clicks / impressions)
        grouped['ctr'] = grouped['click'] / grouped['impression'].clip(lower=1)
        
        # Store results per advertiser
        self.ctr_scores = {
            adv: products.sort_values('ctr', ascending=False).head(self.n_recommendations)
            for adv, products in grouped.groupby(level=0)
        }

    def get_top_ctr_recommendations(self, advertiser_id: str) -> List[Dict]:
        """
        Get top N products with highest CTR for an advertiser
        
        Parameters:
        -----------
        advertiser_id : str
            ID of the advertiser to get recommendations for
            
        Returns:
        --------
        List[Dict] : List of recommendations with product_id and score
        """
        if advertiser_id not in self.ctr_scores:
            return []

        products = self.ctr_scores[advertiser_id]
        return [
            {
                'product_id': product_id,
                'score': score,
                'model_type': 'top_ctr'
            }
            for product_id, score in products['ctr'].items()
        ]