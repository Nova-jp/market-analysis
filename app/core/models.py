"""
データモデル定義
APIの入出力とデータベーススキーマの型安全性を確保
"""
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime


class BondYieldData(BaseModel):
    """国債利回りデータモデル"""
    maturity: float = Field(..., description="残存年数")
    yield_rate: float = Field(..., alias="yield", description="利回り(%)")
    bond_name: str = Field(..., description="銘柄名")
    bond_code: Optional[str] = Field(None, description="銘柄コード (9桁)")

    class Config:
        populate_by_name = True


class SwapYieldData(BaseModel):
    """スワップ金利データモデル"""
    maturity: float = Field(..., description="残存年数")
    rate: float = Field(..., description="金利(%)")
    tenor: str = Field(..., description="期間 (1Y, 10Y等)")


class ASWData(BaseModel):
    """ASW (Asset Swap Spread) データモデル"""
    maturity: float = Field(..., description="残存年数")
    bond_code: Optional[str] = Field(None, description="銘柄コード")
    bond_name: str = Field(..., description="銘柄名")
    bond_yield: float = Field(..., description="国債利回り(%)")
    swap_rate: float = Field(..., description="補間スワップレート(%)")
    asw: float = Field(..., description="ASW (国債-スワップ)")  # %単位
    
class YieldCurveResponse(BaseModel):
    """イールドカーブレスポンスモデル"""
    date: str = Field(..., description="対象日付 (YYYY-MM-DD)")
    data: List[BondYieldData] = Field(..., description="国債イールドカーブデータ")
    swap_data: List[SwapYieldData] = Field(default=[], description="スワップイールドカーブデータ")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class ASWCurveResponse(BaseModel):
    """ASWカーブレスポンスモデル"""
    date: str = Field(..., description="対象日付 (YYYY-MM-DD)")
    data: List[ASWData] = Field(..., description="ASWデータ")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class QuickDatesResponse(BaseModel):
    """クイック日付選択レスポンスモデル"""
    latest: Optional[str] = Field(None, description="最新営業日")
    previous: Optional[str] = Field(None, description="前営業日")
    five_days_ago: Optional[str] = Field(None, description="5営業日前")
    month_ago: Optional[str] = Field(None, description="1ヶ月前")


class DateSearchResponse(BaseModel):
    """日付検索レスポンスモデル"""
    dates: List[str] = Field(..., description="検索結果の日付リスト")


class HealthCheckResponse(BaseModel):
    """ヘルスチェックレスポンスモデル"""
    status: str = Field(..., description="アプリケーション状態")
    app: str = Field(..., description="アプリケーション名")
    version: str = Field(..., description="バージョン")
    timestamp: str = Field(..., description="チェック実行時刻")
    database_config: Dict[str, Any] = Field(..., description="データベース設定状況")


class SystemInfoResponse(BaseModel):
    """システム情報レスポンスモデル"""
    app_name: str = Field(..., description="アプリケーション名")
    version: str = Field(..., description="バージョン")
    description: str = Field(..., description="説明")
    apis: Dict[str, str] = Field(..., description="利用可能API一覧")
    features: List[str] = Field(..., description="機能一覧")


class ErrorResponse(BaseModel):
    """エラーレスポンスモデル"""
    error: str = Field(..., description="エラーメッセージ")
    detail: Optional[str] = Field(None, description="詳細情報")
    timestamp: Optional[str] = Field(None, description="エラー発生時刻")

    def __init__(self, **data):
        if "timestamp" not in data:
            data["timestamp"] = datetime.now().isoformat()
        super().__init__(**data)


# バリデーション関数
def validate_date_format(date_string: str) -> bool:
    """日付形式 (YYYY-MM-DD) の検証"""
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def validate_maturity_range(min_years: Optional[float], max_years: Optional[float]) -> bool:
    """満期年数範囲の検証"""
    if min_years is None and max_years is None:
        return True
    if min_years is not None and min_years < 0:
        return False
    if max_years is not None and max_years < 0:
        return False
    if min_years is not None and max_years is not None and min_years > max_years:
        return False
    return True


class MarketAmountBucket(BaseModel):
    """市中残存額バケットデータモデル"""
    year: int = Field(..., description="残存年限バケット (0=0-1年, 1=1-2年, ...)")
    amount: float = Field(..., description="市中残存額合計 (億円)")


class MarketAmountResponse(BaseModel):
    """市中残存額レスポンスモデル"""
    date: str = Field(..., description="対象日付 (YYYY-MM-DD)")
    buckets: List[MarketAmountBucket] = Field(..., description="1年区切りバケットデータ")
    total_amount: float = Field(..., description="総市中残存額 (億円)")
    bond_count: int = Field(..., description="対象銘柄数")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class BondTimeseriesPoint(BaseModel):
    """時系列データポイント"""
    trade_date: str = Field(..., description="取引日 (YYYY-MM-DD)")
    market_amount: float = Field(..., description="市中残存額 (億円)")


class BondTimeseriesStatistics(BaseModel):
    """統計情報"""
    latest_date: str = Field(..., description="最新日付")
    latest_amount: float = Field(..., description="最新残存額 (億円)")
    min_amount: float = Field(..., description="最小残存額 (億円)")
    max_amount: float = Field(..., description="最大残存額 (億円)")
    avg_amount: float = Field(..., description="平均残存額 (億円)")
    data_points: int = Field(..., description="データ数")


class BondTimeseriesResponse(BaseModel):
    """銘柄時系列レスポンス"""
    bond_code: str = Field(..., description="銘柄コード")
    bond_name: str = Field(..., description="銘柄名")
    due_date: str = Field(..., description="償還日")
    timeseries: List[BondTimeseriesPoint] = Field(..., description="時系列データ")
    statistics: BondTimeseriesStatistics = Field(..., description="統計情報")


class BondSearchItem(BaseModel):
    """銘柄検索結果アイテム"""
    bond_code: str = Field(..., description="銘柄コード")
    bond_name: str = Field(..., description="銘柄名")
    due_date: str = Field(..., description="償還日")
    latest_market_amount: float = Field(..., description="最新市中残存額 (億円)")
    latest_trade_date: str = Field(..., description="最新取引日")


class BondSearchResponse(BaseModel):
    """銘柄検索レスポンス"""
    bonds: List[BondSearchItem] = Field(..., description="銘柄リスト")
    count: int = Field(..., description="件数")