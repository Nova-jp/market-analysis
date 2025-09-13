import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Tuple, Dict, Any


class PCAAnalyzer:
    """主成分分析を行うクラス"""
    
    def __init__(self, n_components: int = None):
        self.n_components = n_components
        self.pca = None
        self.scaler = StandardScaler()
        self.fitted = False
    
    def fit_transform(self, data: pd.DataFrame) -> Tuple[np.ndarray, Dict[str, Any]]:
        """主成分分析を実行"""
        # データの標準化
        scaled_data = self.scaler.fit_transform(data)
        
        # PCA実行
        self.pca = PCA(n_components=self.n_components)
        principal_components = self.pca.fit_transform(scaled_data)
        
        self.fitted = True
        
        # 結果をまとめる
        results = {
            'explained_variance_ratio': self.pca.explained_variance_ratio_,
            'cumulative_variance_ratio': np.cumsum(self.pca.explained_variance_ratio_),
            'components': self.pca.components_,
            'feature_names': data.columns.tolist()
        }
        
        return principal_components, results
    
    def plot_explained_variance(self, results: Dict[str, Any]) -> None:
        """寄与率をプロット"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
        
        # 各主成分の寄与率
        ax1.bar(range(1, len(results['explained_variance_ratio']) + 1), 
                results['explained_variance_ratio'])
        ax1.set_xlabel('主成分')
        ax1.set_ylabel('寄与率')
        ax1.set_title('各主成分の寄与率')
        
        # 累積寄与率
        ax2.plot(range(1, len(results['cumulative_variance_ratio']) + 1), 
                results['cumulative_variance_ratio'], 'bo-')
        ax2.axhline(y=0.8, color='r', linestyle='--', alpha=0.7, label='80%')
        ax2.set_xlabel('主成分')
        ax2.set_ylabel('累積寄与率')
        ax2.set_title('累積寄与率')
        ax2.legend()
        
        plt.tight_layout()
        plt.show()
    
    def get_component_loadings(self, results: Dict[str, Any]) -> pd.DataFrame:
        """主成分負荷量を取得"""
        if not self.fitted:
            raise ValueError("PCA has not been fitted yet")
        
        loadings = pd.DataFrame(
            results['components'].T,
            columns=[f'PC{i+1}' for i in range(results['components'].shape[0])],
            index=results['feature_names']
        )
        return loadings