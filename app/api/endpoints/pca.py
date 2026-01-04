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
    components: int = Query(3, ge=1, le=5, description="主成分の数")
):
    """
    主成分分析を実行

    Parameters:
    - days: PCA学習に使用する営業日数 (30-200)
    - components: 主成分の数 (1-5)

    Returns:
    - PCA分析結果（主成分スコア、主成分ベクトル、復元誤差など）
    """
    try:
        logger.info(f"PCA分析開始: days={days}, components={components}")

        pca_service = PCAService()
        result = pca_service.run_pca_analysis(
            lookback_days=days,
            n_components=components
        )

        logger.info("PCA分析完了")
        return result

    except Exception as e:
        logger.error(f"PCA分析エラー: {str(e)}")
        raise HTTPException(status_code=500, detail=f"PCA分析エラー: {str(e)}")


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
