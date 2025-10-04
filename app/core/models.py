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

    class Config:
        populate_by_name = True


class YieldCurveResponse(BaseModel):
    """イールドカーブレスポンスモデル"""
    date: str = Field(..., description="対象日付 (YYYY-MM-DD)")
    data: List[BondYieldData] = Field(..., description="イールドカーブデータ")
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