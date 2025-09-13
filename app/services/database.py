import os
from supabase import create_client, Client
from typing import Optional, List, Dict, Any
import pandas as pd
from datetime import datetime


class DatabaseService:
    """Supabaseデータベース操作サービス"""
    
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Supabase credentials not found in environment variables")
        
        self.client: Client = create_client(self.supabase_url, self.supabase_key)
    
    def insert_bond_data(self, data: List[Dict[str, Any]]) -> bool:
        """国債データを挿入"""
        try:
            result = self.client.table('bond_data').insert(data).execute()
            return True
        except Exception as e:
            print(f"Error inserting bond data: {e}")
            return False
    
    def get_bond_data(self, start_date: str, end_date: str, bond_type: Optional[str] = None) -> pd.DataFrame:
        """国債データを取得"""
        try:
            query = self.client.table('bond_data').select("*").gte('date', start_date).lte('date', end_date)
            
            if bond_type:
                query = query.eq('bond_type', bond_type)
            
            result = query.execute()
            return pd.DataFrame(result.data)
        except Exception as e:
            print(f"Error getting bond data: {e}")
            return pd.DataFrame()
    
    def insert_economic_indicator(self, data: List[Dict[str, Any]]) -> bool:
        """経済指標データを挿入"""
        try:
            result = self.client.table('economic_indicators').insert(data).execute()
            return True
        except Exception as e:
            print(f"Error inserting economic indicator data: {e}")
            return False