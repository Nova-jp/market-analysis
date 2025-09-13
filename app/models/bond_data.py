from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class BondData(BaseModel):
    """国債データのモデル"""
    id: Optional[int] = None
    date: datetime
    bond_type: str  # 国債種類（10年、5年など）
    yield_rate: float  # 利回り
    price: Optional[float] = None  # 価格
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class EconomicIndicator(BaseModel):
    """経済指標のモデル"""
    id: Optional[int] = None
    date: datetime
    indicator_name: str  # 指標名
    value: float  # 値
    unit: str  # 単位
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None