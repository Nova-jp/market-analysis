#!/usr/bin/env python3
"""
イールドカーブ時系列分析 Streamlit アプリ
過去データのナビゲーション機能付き
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, date, timedelta
import sys
import os
import numpy as np
import time

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.yield_curve_analyzer import YieldCurveAnalyzer

# ページ設定
st.set_page_config(
    page_title="日本国債イールドカーブ時系列分析",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data(ttl=300)  # 5分間キャッシュ
def load_available_dates():
    """利用可能な日付一覧を取得（キャッシュ付き）"""
    try:
        analyzer = YieldCurveAnalyzer()
        import requests
        
        response = requests.get(
            f'{analyzer.db_manager.supabase_url}/rest/v1/clean_bond_data',
            params={
                'select': 'trade_date',
                'order': 'trade_date.desc',
                'limit': 50000  # より多くのデータを取得
            },
            headers=analyzer.db_manager.headers
        )
        
        if response.status_code == 200:
            data = response.json()
            dates = sorted(list(set([item['trade_date'] for item in data])), reverse=True)
            return dates
        else:
            return []
    except Exception as e:
        st.error(f"日付取得エラー: {e}")
        return ['2025-09-09']

@st.cache_data
def analyze_yield_curve_cached(target_date):
    """キャッシュ付きイールドカーブ分析"""
    try:
        analyzer = YieldCurveAnalyzer()
        result = analyzer.analyze_yield_curve(target_date, show_plots=False)
        return result
    except Exception as e:
        return {"error": str(e)}

def create_interactive_yield_curve(df, target_date, comparison_data=None):
    """インタラクティブなイールドカーブ作成"""
    fig = go.Figure()
    
    # メインのイールドカーブ
    fig.add_trace(go.Scatter(
        x=df['years_to_maturity'],
        y=df['ave_compound_yield'],
        mode='markers+lines',
        marker=dict(
            size=6,
            color=df['years_to_maturity'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="満期年数")
        ),
        line=dict(width=2, color='blue'),
        text=df['bond_name'],
        hovertemplate='<b>%{text}</b><br>満期: %{x:.1f}年<br>利回り: %{y:.3f}%<extra></extra>',
        name=f'{target_date}'
    ))
    
    # 比較データがある場合
    if comparison_data is not None and not comparison_data.empty:
        fig.add_trace(go.Scatter(
            x=comparison_data['years_to_maturity'],
            y=comparison_data['ave_compound_yield'],
            mode='markers+lines',
            marker=dict(size=4, color='red', opacity=0.6),
            line=dict(width=2, color='red', dash='dash'),
            name=f'比較日',
            hovertemplate='満期: %{x:.1f}年<br>利回り: %{y:.3f}%<extra></extra>'
        ))
    
    # レイアウト設定
    fig.update_layout(
        title=f'日本国債イールドカーブ - {target_date}',
        xaxis_title='満期までの年数',
        yaxis_title='利回り (%)',
        height=600,
        hovermode='x unified',
        showlegend=True,
        template='plotly_white'
    )
    
    return fig

def create_yield_change_heatmap(dates_data):
    """利回り変化のヒートマップ作成"""
    if len(dates_data) < 2:
        return None
    
    # データ準備
    heatmap_data = []
    dates = []
    
    for date_str, data in dates_data.items():
        if "error" not in data and len(data['data']) > 0:
            df = data['data']
            dates.append(date_str)
            # 主要年限の利回りを抽出
            yields_by_maturity = {}
            for _, row in df.iterrows():
                maturity_key = f"{row['years_to_maturity']:.0f}年"
                if maturity_key not in yields_by_maturity:
                    yields_by_maturity[maturity_key] = row['ave_compound_yield']
            heatmap_data.append(yields_by_maturity)
    
    if not heatmap_data:
        return None
    
    # DataFrameに変換
    heatmap_df = pd.DataFrame(heatmap_data, index=dates)
    heatmap_df = heatmap_df.fillna(method='ffill').fillna(method='bfill')
    
    # ヒートマップ作成
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_df.values,
        x=heatmap_df.columns,
        y=heatmap_df.index,
        colorscale='RdYlBu_r',
        hovertemplate='日付: %{y}<br>満期: %{x}<br>利回り: %{z:.3f}%<extra></extra>'
    ))
    
    fig.update_layout(
        title='利回りヒートマップ',
        xaxis_title='満期',
        yaxis_title='日付',
        height=400
    )
    
    return fig

# セッション状態の初期化
def init_session_state():
    if 'current_date_index' not in st.session_state:
        st.session_state.current_date_index = 0
    if 'available_dates' not in st.session_state:
        st.session_state.available_dates = []
    if 'auto_play' not in st.session_state:
        st.session_state.auto_play = False
    if 'comparison_date' not in st.session_state:
        st.session_state.comparison_date = None

def main():
    st.title("📈 日本国債イールドカーブ時系列分析")
    st.markdown("過去データのナビゲーション機能付き")
    st.markdown("---")
    
    init_session_state()
    
    # 利用可能日付の読み込み
    with st.spinner('利用可能な日付を取得中...'):
        available_dates = load_available_dates()
        st.session_state.available_dates = available_dates
    
    if not available_dates:
        st.error("利用可能なデータが見つかりません")
        return
    
    st.success(f"📅 利用可能な日付: {len(available_dates)}日分")
    
    # サイドバー：時系列ナビゲーション
    st.sidebar.title("⏯️ 時系列ナビゲーション")
    
    # 現在の日付インデックスを調整
    if st.session_state.current_date_index >= len(available_dates):
        st.session_state.current_date_index = 0
    
    current_date = available_dates[st.session_state.current_date_index]
    
    # 日付表示と基本情報
    st.sidebar.markdown(f"**📅 現在の日付:**")
    st.sidebar.markdown(f"## {current_date}")
    st.sidebar.markdown(f"**位置:** {st.session_state.current_date_index + 1} / {len(available_dates)}")
    
    # ナビゲーションボタン
    col1, col2, col3 = st.sidebar.columns(3)
    
    with col1:
        if st.button("⏮️ 前日", disabled=st.session_state.current_date_index == 0):
            st.session_state.current_date_index = max(0, st.session_state.current_date_index - 1)
            st.rerun()
    
    with col2:
        if st.button("⏭️ 次日", disabled=st.session_state.current_date_index >= len(available_dates) - 1):
            st.session_state.current_date_index = min(len(available_dates) - 1, st.session_state.current_date_index + 1)
            st.rerun()
    
    with col3:
        if st.button("🏠 最新"):
            st.session_state.current_date_index = 0
            st.rerun()
    
    # 日付スライダー
    st.sidebar.markdown("**📅 日付選択:**")
    slider_index = st.sidebar.slider(
        "日付位置",
        min_value=0,
        max_value=len(available_dates) - 1,
        value=st.session_state.current_date_index,
        format="%d"
    )
    
    if slider_index != st.session_state.current_date_index:
        st.session_state.current_date_index = slider_index
        st.rerun()
    
    # 自動再生機能
    st.sidebar.markdown("**▶️ 自動再生:**")
    auto_play_speed = st.sidebar.slider("再生速度（秒）", 0.5, 5.0, 2.0, 0.5)
    
    if st.sidebar.button("▶️ 自動再生開始"):
        st.session_state.auto_play = True
        st.rerun()
    
    if st.sidebar.button("⏸️ 停止"):
        st.session_state.auto_play = False
    
    # 比較機能
    st.sidebar.markdown("**📊 比較機能:**")
    enable_comparison = st.sidebar.checkbox("比較モード")
    comparison_date = None
    
    if enable_comparison:
        comparison_date = st.sidebar.selectbox(
            "比較する日付:",
            options=available_dates,
            index=min(10, len(available_dates) - 1) if len(available_dates) > 10 else len(available_dates) - 1
        )
    
    # メイン表示エリア
    current_date = available_dates[st.session_state.current_date_index]
    
    # データ分析実行
    with st.spinner(f'{current_date}のデータを分析中...'):
        result = analyze_yield_curve_cached(current_date)
        comparison_result = None
        if comparison_date:
            comparison_result = analyze_yield_curve_cached(comparison_date)
    
    if "error" in result:
        st.error(f"❌ エラー: {result['error']}")
        return
    
    # 基本統計表示
    df = result['data']
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 債券数", f"{len(df)}銘柄")
    
    with col2:
        st.metric("📈 平均利回り", f"{df['ave_compound_yield'].mean():.3f}%")
    
    with col3:
        st.metric("📊 利回り範囲", 
                 f"{df['ave_compound_yield'].min():.3f}% - {df['ave_compound_yield'].max():.3f}%")
    
    with col4:
        st.metric("⏱️ 平均満期", f"{df['years_to_maturity'].mean():.1f}年")
    
    # イールドカーブ表示
    st.subheader(f"📈 イールドカーブ - {current_date}")
    
    comparison_data = None
    if comparison_result and "error" not in comparison_result:
        comparison_data = comparison_result['data']
    
    fig_curve = create_interactive_yield_curve(df, current_date, comparison_data)
    st.plotly_chart(fig_curve, use_container_width=True)
    
    # 期間別統計
    short_term = df[df['years_to_maturity'] <= 2]
    medium_term = df[(df['years_to_maturity'] > 2) & (df['years_to_maturity'] <= 10)]
    long_term = df[df['years_to_maturity'] > 10]
    
    col_stat1, col_stat2, col_stat3 = st.columns(3)
    
    with col_stat1:
        st.metric(
            "🔷 短期（2年以下）",
            f"{len(short_term)}銘柄",
            f"{short_term['ave_compound_yield'].mean():.3f}%" if len(short_term) > 0 else "データなし"
        )
    
    with col_stat2:
        st.metric(
            "🔶 中期（2-10年）",
            f"{len(medium_term)}銘柄",
            f"{medium_term['ave_compound_yield'].mean():.3f}%" if len(medium_term) > 0 else "データなし"
        )
    
    with col_stat3:
        st.metric(
            "🔴 長期（10年超）",
            f"{len(long_term)}銘柄",
            f"{long_term['ave_compound_yield'].mean():.3f}%" if len(long_term) > 0 else "データなし"
        )
    
    # 自動再生処理
    if st.session_state.auto_play:
        if st.session_state.current_date_index < len(available_dates) - 1:
            time.sleep(auto_play_speed)
            st.session_state.current_date_index += 1
            st.rerun()
        else:
            st.session_state.auto_play = False
            st.success("自動再生が完了しました！")
    
    # 操作ヘルプ
    with st.expander("📖 操作方法"):
        st.markdown("""
        **⏯️ 基本ナビゲーション:**
        - ⏮️ 前日ボタン: 1日前のデータに移動
        - ⏭️ 次日ボタン: 1日後のデータに移動  
        - 🏠 最新ボタン: 最新日のデータに移動
        - 📅 日付スライダー: 任意の日付に移動
        
        **▶️ 自動再生:**
        - 再生速度を設定して時系列データを自動表示
        - ⏸️ 停止ボタンで中断可能
        
        **📊 比較機能:**
        - 比較モードで2つの日付のイールドカーブを重ね表示
        - 時系列での変化を視覚的に確認
        """)

if __name__ == "__main__":
    main()