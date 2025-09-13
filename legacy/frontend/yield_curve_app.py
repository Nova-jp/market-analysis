#!/usr/bin/env python3
"""
イールドカーブ表示 Streamlit アプリ
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, date
import sys
import os
import numpy as np

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.yield_curve_analyzer import YieldCurveAnalyzer

# ページ設定
st.set_page_config(
    page_title="日本国債イールドカーブ分析",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_available_dates():
    """利用可能な日付一覧を取得"""
    try:
        analyzer = YieldCurveAnalyzer()
        # 直近30日分のデータがある日付を取得
        import requests
        
        response = requests.get(
            f'{analyzer.db_manager.supabase_url}/rest/v1/clean_bond_data',
            params={
                'select': 'trade_date',
                'order': 'trade_date.desc',
                'limit': 10000
            },
            headers=analyzer.db_manager.headers
        )
        
        if response.status_code == 200:
            data = response.json()
            dates = sorted(list(set([item['trade_date'] for item in data])), reverse=True)
            return dates[:30]  # 最新30日分
        else:
            return []
    except:
        return ['2025-09-09']  # デフォルト

@st.cache_data
def analyze_yield_curve_cached(target_date):
    """キャッシュ付きイールドカーブ分析"""
    analyzer = YieldCurveAnalyzer()
    return analyzer.analyze_yield_curve(target_date, show_plots=False)

def create_yield_curve_plotly(df, target_date):
    """Plotlyでイールドカーブを作成"""
    fig = go.Figure()
    
    # 散布図プロット
    fig.add_trace(go.Scatter(
        x=df['years_to_maturity'],
        y=df['ave_compound_yield'],
        mode='markers',
        marker=dict(
            size=8,
            color=df['years_to_maturity'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="満期年数")
        ),
        text=df['bond_name'],
        hovertemplate='<b>%{text}</b><br>満期: %{x:.1f}年<br>利回り: %{y:.3f}%<extra></extra>',
        name='国債データ'
    ))
    
    # スムーズなカーブ（多項式フィッティング）
    if len(df) >= 3:
        try:
            curve_df = df.drop_duplicates('years_to_maturity').sort_values('years_to_maturity')
            if len(curve_df) >= 3:
                z = np.polyfit(curve_df['years_to_maturity'], curve_df['ave_compound_yield'], 3)
                p = np.poly1d(z)
                x_smooth = np.linspace(curve_df['years_to_maturity'].min(), 
                                     curve_df['years_to_maturity'].max(), 200)
                y_smooth = p(x_smooth)
                
                fig.add_trace(go.Scatter(
                    x=x_smooth,
                    y=y_smooth,
                    mode='lines',
                    line=dict(color='red', width=3),
                    name='イールドカーブ',
                    hovertemplate='満期: %{x:.1f}年<br>利回り: %{y:.3f}%<extra></extra>'
                ))
        except:
            pass
    
    # レイアウト設定
    fig.update_layout(
        title=f'日本国債イールドカーブ - {target_date}',
        xaxis_title='満期までの年数',
        yaxis_title='利回り (%)',
        height=600,
        hovermode='closest',
        showlegend=True
    )
    
    return fig

def create_maturity_distribution_plotly(df):
    """満期・利回り分布をPlotlyで作成"""
    fig = go.Figure()
    
    # 満期分布ヒストグラム
    fig.add_trace(go.Histogram(
        x=df['years_to_maturity'],
        nbinsx=20,
        name='満期分布',
        opacity=0.7,
        marker_color='skyblue'
    ))
    
    fig.update_layout(
        title='満期分布',
        xaxis_title='満期までの年数',
        yaxis_title='債券数',
        height=400
    )
    
    return fig

def create_yield_distribution_plotly(df):
    """利回り分布をPlotlyで作成"""
    fig = go.Figure()
    
    # 利回り分布ヒストグラム
    fig.add_trace(go.Histogram(
        x=df['ave_compound_yield'],
        nbinsx=20,
        name='利回り分布',
        opacity=0.7,
        marker_color='lightcoral'
    ))
    
    fig.update_layout(
        title='利回り分布',
        xaxis_title='利回り (%)',
        yaxis_title='債券数',
        height=400
    )
    
    return fig

# メインアプリケーション
def main():
    st.title("📈 日本国債イールドカーブ分析")
    st.markdown("---")
    
    # サイドバー設定
    st.sidebar.title("⚙️ 分析設定")
    
    # 日付選択
    available_dates = load_available_dates()
    if available_dates:
        selected_date = st.sidebar.selectbox(
            "📅 分析対象日を選択:",
            options=available_dates,
            index=0
        )
    else:
        selected_date = st.sidebar.date_input(
            "📅 分析対象日を選択:",
            value=date(2025, 9, 9)
        ).strftime('%Y-%m-%d')
    
    # フィルター設定
    st.sidebar.subheader("🔍 データフィルター")
    min_maturity = st.sidebar.slider("最小満期年数", 0.0, 40.0, 0.0)
    max_maturity = st.sidebar.slider("最大満期年数", 0.0, 40.0, 40.0)
    
    # 分析実行
    if st.sidebar.button("🚀 分析実行", type="primary"):
        st.session_state.analysis_done = True
        st.session_state.selected_date = selected_date
        st.session_state.min_maturity = min_maturity
        st.session_state.max_maturity = max_maturity
    
    # 初期状態または分析実行後の表示
    if hasattr(st.session_state, 'analysis_done') and st.session_state.analysis_done:
        
        with st.spinner('データ分析中...'):
            result = analyze_yield_curve_cached(st.session_state.selected_date)
        
        if "error" in result:
            st.error(f"❌ エラー: {result['error']}")
            return
        
        # データフィルタリング
        df = result['data']
        df_filtered = df[
            (df['years_to_maturity'] >= st.session_state.min_maturity) & 
            (df['years_to_maturity'] <= st.session_state.max_maturity)
        ]
        
        # メイン表示エリア
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 債券数", f"{len(df_filtered)}銘柄")
        
        with col2:
            st.metric("📈 平均利回り", f"{df_filtered['ave_compound_yield'].mean():.3f}%")
        
        with col3:
            st.metric("📊 利回り範囲", 
                     f"{df_filtered['ave_compound_yield'].min():.3f}% - {df_filtered['ave_compound_yield'].max():.3f}%")
        
        with col4:
            st.metric("⏱️ 平均満期", f"{df_filtered['years_to_maturity'].mean():.1f}年")
        
        # イールドカーブ表示
        st.subheader("📈 イールドカーブ")
        if len(df_filtered) > 0:
            fig_curve = create_yield_curve_plotly(df_filtered, st.session_state.selected_date)
            st.plotly_chart(fig_curve, use_container_width=True)
        else:
            st.warning("表示するデータがありません。フィルター条件を調整してください。")
        
        # 分布図表示
        col_dist1, col_dist2 = st.columns(2)
        
        with col_dist1:
            st.subheader("📊 満期分布")
            if len(df_filtered) > 0:
                fig_maturity = create_maturity_distribution_plotly(df_filtered)
                st.plotly_chart(fig_maturity, use_container_width=True)
        
        with col_dist2:
            st.subheader("📊 利回り分布")
            if len(df_filtered) > 0:
                fig_yield = create_yield_distribution_plotly(df_filtered)
                st.plotly_chart(fig_yield, use_container_width=True)
        
        # 期間別統計
        st.subheader("📊 期間別統計")
        
        short_term = df_filtered[df_filtered['years_to_maturity'] <= 2]
        medium_term = df_filtered[(df_filtered['years_to_maturity'] > 2) & (df_filtered['years_to_maturity'] <= 10)]
        long_term = df_filtered[df_filtered['years_to_maturity'] > 10]
        
        col_stat1, col_stat2, col_stat3 = st.columns(3)
        
        with col_stat1:
            st.metric(
                "🔷 短期（2年以下）",
                f"{len(short_term)}銘柄",
                f"平均利回り: {short_term['ave_compound_yield'].mean():.3f}%" if len(short_term) > 0 else "データなし"
            )
        
        with col_stat2:
            st.metric(
                "🔶 中期（2-10年）",
                f"{len(medium_term)}銘柄",
                f"平均利回り: {medium_term['ave_compound_yield'].mean():.3f}%" if len(medium_term) > 0 else "データなし"
            )
        
        with col_stat3:
            st.metric(
                "🔴 長期（10年超）",
                f"{len(long_term)}銘柄",
                f"平均利回り: {long_term['ave_compound_yield'].mean():.3f}%" if len(long_term) > 0 else "データなし"
            )
        
        # データテーブル
        if st.checkbox("📋 詳細データを表示"):
            st.subheader("📋 債券データ詳細")
            display_columns = ['bond_name', 'years_to_maturity', 'ave_compound_yield', 'coupon_rate', 'ave_price']
            st.dataframe(
                df_filtered[display_columns].round(3),
                use_container_width=True
            )
    
    else:
        # 初期画面
        st.info("👈 サイドバーから分析対象日を選択し、「🚀 分析実行」ボタンを押してください。")
        
        # サンプル説明
        st.subheader("📖 機能説明")
        st.markdown("""
        このアプリケーションでは以下の分析が可能です：
        
        **📈 イールドカーブ表示**
        - 満期年数と利回りの関係を可視化
        - 3次多項式フィッティングによるスムーズなカーブ
        - インタラクティブなホバー情報
        
        **📊 統計情報**
        - 期間別（短期・中期・長期）の統計
        - 利回りと満期の分布表示
        - 詳細な債券データテーブル
        
        **🔍 フィルター機能**
        - 満期年数による絞り込み
        - リアルタイムな表示更新
        """)

if __name__ == "__main__":
    main()