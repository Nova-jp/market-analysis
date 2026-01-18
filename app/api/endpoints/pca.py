"""
PCA API Endpoints
主成分分析のAPIエンドポイント
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import logging

from app.services.pca_service import PCAService

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/analyze")
async def analyze_pca(
    days: int = Query(100, ge=30, le=200, description="PCA学習に使用する営業日数"),
    components: int = Query(3, ge=1, le=5, description="主成分の数"),
    end_date: Optional[str] = Query(None, description="分析基準日 (YYYY-MM-DD)。指定しない場合は最新日。")
):
    """
    主成分分析を実行

    Parameters:
    - days: PCA学習に使用する営業日数 (30-200)
    - components: 主成分の数 (1-5)
    - end_date: 分析基準日

    Returns:
    - PCA分析結果（主成分スコア、主成分ベクトル、復元誤差など）
    """
    try:
        logger.info(f"PCA分析開始: days={days}, components={components}, end_date={end_date}")

        pca_service = PCAService()
        result = pca_service.run_pca_analysis(
            lookback_days=days,
            n_components=components,
            end_date=end_date
        )
        
        if "error" in result:
             raise HTTPException(status_code=400, detail=result["error"])

        # フロントエンド向けにデータ構造を変換
        pca_model = result['pca_model']
        pca_scores = result['principal_component_scores']
        
        # 1. componentsの整形
        formatted_components = []
        cum_variance = 0.0
        for i in range(len(pca_model['explained_variance_ratio'])):
            variance = pca_model['explained_variance_ratio'][i]
            cum_variance += variance
            formatted_components.append({
                'pc_number': i + 1,
                'eigenvalue': 0, # 計算省略
                'explained_variance_ratio': variance,
                'cumulative_variance_ratio': cum_variance,
                'loadings': pca_model['components'][i]
            })
            
        # 2. scoresの整形
        formatted_scores = []
        dates = pca_scores['dates']
        scores_data = pca_scores['scores']
        
        for i, date_str in enumerate(dates):
            score_entry = {'date': date_str}
            for j, score in enumerate(scores_data[i]):
                score_entry[f'pc{j+1}'] = score
            formatted_scores.append(score_entry)

        logger.info("PCA分析完了")
        
        return {
            'components': formatted_components,
            'scores': formatted_scores,
            'maturities': result['common_grid'],
            'mean_vector': pca_model['mean'],
            'parameters': {
                'days': days,
                'components': components,
                'actual_end_date': result['parameters']['actual_end_date'],
                'date_range': result['parameters']['date_range']
            },
            'reconstruction_dates': result['reconstruction_dates'],
            'latest_reconstruction': result['latest_reconstruction']
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"PCA分析エラー: {str(e)}")
        raise HTTPException(status_code=500, detail=f"PCA分析エラー: {str(e)}")


@router.get("/reconstruction")
async def get_reconstruction(
    date: str = Query(..., description="復元対象の日付 (YYYY-MM-DD)"),
    end_date: str = Query(..., description="分析の基準日"),
    days: int = Query(100, description="分析に使用した営業日数"),
    components: int = Query(3, description="主成分の数")
):
    """
    キャッシュされたモデルを使用して、特定の日付の復元誤差を計算
    """
    try:
        pca_service = PCAService()
        result = pca_service.get_reconstruction_for_date(
            target_date=date,
            end_date=end_date,
            lookback_days=days,
            n_components=components
        )
        
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
            
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"復元データ取得エラー: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analyze_swap")
async def analyze_swap_pca(
    days: int = Query(100, ge=30, le=300, description="PCA学習に使用する営業日数"),
    components: int = Query(3, ge=1, le=5, description="主成分の数"),
    product_type: str = Query("OIS", description="プロダクトタイプ (OIS, 3M_TIBORなど)")
):
    """
    スワップ金利の主成分分析を実行

    Parameters:
    - days: PCA学習に使用する営業日数
    - components: 主成分の数
    - product_type: プロダクトタイプ

    Returns:
    - PCA分析結果
    """
    try:
        logger.info(f"Swap PCA分析開始: days={days}, components={components}, product={product_type}")

        pca_service = PCAService()
        result = pca_service.run_swap_pca_analysis(
            lookback_days=days,
            n_components=components,
            product_type=product_type
        )
        
        if "error" in result:
             raise HTTPException(status_code=400, detail=result["error"])

        logger.info("Swap PCA分析完了")
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Swap PCA分析エラー: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Swap PCA分析エラー: {str(e)}")


@router.get("/parameters")
async def get_parameters():
    """
    PCA分析で使用可能なパラメータの範囲を返す
    """
    return {
        "days": {
            "min": 30,
            "max": 200,
            "default": 100,
            "description": "PCA学習に使用する営業日数"
        },
        "components": {
            "min": 1,
            "max": 5,
            "default": 3,
            "description": "主成分の数",
            "labels": ["Level", "Slope", "Curvature", "PC4", "PC5"]
        }
    }
