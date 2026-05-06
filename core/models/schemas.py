"""
Pydantic レスポンスモデル定義
API の入出力の型安全性を確保する。
"""
from pydantic import BaseModel, Field, model_validator
from typing import List, Optional, Dict, Any
from datetime import datetime


class BondYieldData(BaseModel):
    maturity: float = Field(..., description="残存年数")
    yield_rate: float = Field(..., alias="yield", description="利回り(%)")
    bond_name: str = Field(..., description="銘柄名")
    bond_code: Optional[str] = Field(None, description="銘柄コード (9桁)")

    class Config:
        populate_by_name = True


class SwapYieldData(BaseModel):
    maturity: float = Field(..., description="残存年数")
    rate: float = Field(..., description="金利(%)")
    tenor: str = Field(..., description="期間 (1Y, 10Y等)")


class SwapCurveResponse(BaseModel):
    date: str = Field(..., description="対象日付 (YYYY-MM-DD)")
    data: List[SwapYieldData] = Field(..., description="スワップイールドカーブデータ")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class ForwardRateData(BaseModel):
    maturity: float = Field(..., description="残存年数 (X軸の値)")
    rate: float = Field(..., description="フォワードレート(%)")
    start_tenor: str = Field(..., description="開始期間 (1Y等)")
    swap_tenor: str = Field(..., description="スワップ期間 (5Y等)")


class ASWData(BaseModel):
    maturity: float = Field(..., description="残存年数")
    bond_code: Optional[str] = Field(None, description="銘柄コード")
    bond_name: str = Field(..., description="銘柄名")
    bond_yield: float = Field(..., description="国債利回り(%)")
    swap_rate: float = Field(..., description="補間スワップレート(%)")
    asw: float = Field(..., description="ASW (国債-スワップ)")


class YieldCurveResponse(BaseModel):
    date: str = Field(..., description="対象日付 (YYYY-MM-DD)")
    data: List[BondYieldData] = Field(..., description="国債イールドカーブデータ")
    swap_data: List[SwapYieldData] = Field(default=[], description="スワップイールドカーブデータ")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class ASWCurveResponse(BaseModel):
    date: str = Field(..., description="対象日付 (YYYY-MM-DD)")
    data: List[ASWData] = Field(..., description="ASWデータ")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class ForwardCurveResponse(BaseModel):
    date: str = Field(..., description="基準日付 (YYYY-MM-DD)")
    type: str = Field(..., description="フォワードカーブの種類 (fixed-start / fixed-tenor)")
    parameter: str = Field(..., description="固定パラメータ")
    data: List[ForwardRateData] = Field(..., description="フォワードカーブデータ")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class QuickDatesResponse(BaseModel):
    latest: Optional[str] = Field(None, description="最新営業日")
    previous: Optional[str] = Field(None, description="前営業日")
    five_days_ago: Optional[str] = Field(None, description="5営業日前")
    month_ago: Optional[str] = Field(None, description="1ヶ月前")


class DateSearchResponse(BaseModel):
    dates: List[str] = Field(..., description="検索結果の日付リスト")


class HealthCheckResponse(BaseModel):
    status: str
    app: str
    version: str
    timestamp: str
    database_config: Dict[str, Any]


class SystemInfoResponse(BaseModel):
    app_name: str
    version: str
    description: str
    apis: Dict[str, str]
    features: List[str]


class ErrorResponse(BaseModel):
    error: str = Field(..., description="エラーメッセージ")
    detail: Optional[str] = Field(None, description="詳細情報")
    timestamp: Optional[str] = Field(None, description="エラー発生時刻")

    @model_validator(mode="before")
    @classmethod
    def set_timestamp(cls, values: dict) -> dict:
        if not values.get("timestamp"):
            values["timestamp"] = datetime.now().isoformat()
        return values


class MarketAmountBucket(BaseModel):
    year: int = Field(..., description="残存年限バケット")
    amount: float = Field(..., description="市中残存額合計 (億円)")


class MarketAmountResponse(BaseModel):
    date: str
    buckets: List[MarketAmountBucket]
    total_amount: float
    bond_count: int
    error: Optional[str] = None


class BondTimeseriesPoint(BaseModel):
    trade_date: str
    market_amount: float


class BondTimeseriesStatistics(BaseModel):
    latest_date: str
    latest_amount: float
    min_amount: float
    max_amount: float
    avg_amount: float
    data_points: int


class BondTimeseriesResponse(BaseModel):
    bond_code: str
    bond_name: str
    due_date: str
    timeseries: List[BondTimeseriesPoint]
    statistics: BondTimeseriesStatistics


class BondSearchItem(BaseModel):
    bond_code: str
    bond_name: str
    due_date: str
    latest_market_amount: float
    latest_trade_date: str


class BondSearchResponse(BaseModel):
    bonds: List[BondSearchItem]
    count: int


# バリデーション関数
def validate_date_format(date_string: str) -> bool:
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def validate_maturity_range(min_years: Optional[float], max_years: Optional[float]) -> bool:
    if min_years is None and max_years is None:
        return True
    if min_years is not None and min_years < 0:
        return False
    if max_years is not None and max_years < 0:
        return False
    if min_years is not None and max_years is not None and min_years > max_years:
        return False
    return True
