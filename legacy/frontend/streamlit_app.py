import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.database import DatabaseService
from analysis.pca.pca_analysis import PCAAnalyzer


def main():
    st.set_page_config(
        page_title="国債金利分析システム",
        page_icon="📈",
        layout="wide"
    )
    
    st.title("📈 国債金利分析システム")
    
    # サイドバー
    st.sidebar.title("ナビゲーション")
    page = st.sidebar.selectbox(
        "ページを選択",
        ["データ概要", "金利推移", "主成分分析", "データ管理"]
    )
    
    if page == "データ概要":
        show_data_overview()
    elif page == "金利推移":
        show_yield_trends()
    elif page == "主成分分析":
        show_pca_analysis()
    elif page == "データ管理":
        show_data_management()


def show_data_overview():
    """データ概要ページ"""
    st.header("📊 データ概要")
    
    try:
        db = DatabaseService()
        # TODO: データ取得と表示の実装
        st.info("データベースからのデータ取得機能を実装予定")
        
        # サンプルデータの表示
        st.subheader("サンプルデータ")
        sample_data = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10),
            '10年債': [0.5 + i * 0.1 for i in range(10)],
            '5年債': [0.3 + i * 0.05 for i in range(10)]
        })
        st.dataframe(sample_data)
        
    except Exception as e:
        st.error(f"データベース接続エラー: {e}")


def show_yield_trends():
    """金利推移ページ"""
    st.header("📈 金利推移")
    
    # 日付範囲選択
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("開始日", value=date(2024, 1, 1))
    with col2:
        end_date = st.date_input("終了日", value=date.today())
    
    # TODO: 実際のデータでグラフ作成
    st.info("実際のデータを使ったグラフ機能を実装予定")


def show_pca_analysis():
    """主成分分析ページ"""
    st.header("🔍 主成分分析")
    
    st.info("主成分分析機能を実装予定")
    
    # パラメータ設定
    n_components = st.slider("主成分数", min_value=2, max_value=10, value=3)
    
    if st.button("分析実行"):
        st.info("分析を実行します...")
        # TODO: 実際の分析実装


def show_data_management():
    """データ管理ページ"""
    st.header("⚙️ データ管理")
    
    st.subheader("データ収集")
    if st.button("最新データ取得"):
        st.info("データ収集機能を実装予定")
    
    st.subheader("データベース状態")
    st.info("データベース状態表示機能を実装予定")


if __name__ == "__main__":
    main()