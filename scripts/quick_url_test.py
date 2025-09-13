#!/usr/bin/env python3
"""
JSDAの月別ディレクトリ構造を素早くテストするスクリプト
"""

import requests
import sys

def test_url_patterns():
    """異なるURL パターンをテスト"""
    
    print("🔍 JSDA URL パターンテスト")
    print("=" * 40)
    
    # テスト対象日: 2019年12月30日
    test_patterns = [
        # パターン1: 月別ディレクトリ
        "https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files/2019/12/S191230.csv",
        
        # パターン2: 年別ディレクトリ
        "https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files/2019/S191230.csv",
        
        # パターン3: 直接配置
        "https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/S191230.csv",
        
        # 2018年のテスト
        "https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files/2018/12/S181231.csv",
        "https://market.jsda.or.jp/shijyo/saiken/baibai/baisanchi/files/2018/S181231.csv",
    ]
    
    successful_patterns = []
    
    for i, url in enumerate(test_patterns, 1):
        print(f"\n📡 パターン {i}: {url}")
        try:
            response = requests.head(url, timeout=15)
            status = response.status_code
            print(f"   ステータス: {status}")
            
            if status == 200:
                print(f"   ✅ 成功!")
                successful_patterns.append(url)
            elif status == 404:
                print(f"   ❌ ファイルなし")
            else:
                print(f"   ⚠️  その他: {status}")
                
        except requests.exceptions.Timeout:
            print(f"   ⏰ タイムアウト")
        except Exception as e:
            print(f"   💥 エラー: {e}")
    
    print(f"\n📊 結果サマリー:")
    print(f"✅ 成功パターン数: {len(successful_patterns)}")
    
    if successful_patterns:
        print("🎯 有効なURL:")
        for url in successful_patterns:
            print(f"  - {url}")
        
        # パターン分析
        if "files/2019/12/" in successful_patterns[0]:
            print("\n💡 結論: 月別ディレクトリ構造 (/files/YYYY/MM/) が有効")
        elif "files/2019/" in successful_patterns[0]:
            print("\n💡 結論: 年別ディレクトリ構造 (/files/YYYY/) が有効")
    else:
        print("❌ 有効なパターンが見つかりませんでした")
    
    return successful_patterns

if __name__ == "__main__":
    try:
        successful = test_url_patterns()
        if successful:
            print(f"\n🚀 次のステップ: 発見されたパターンでヒストリカルデータ収集を実装")
        else:
            print(f"\n🔧 次のステップ: 他のURL構造を調査")
    except KeyboardInterrupt:
        print("\n⚠️ ユーザーによって中断されました")
    except Exception as e:
        print(f"\n💥 予期しないエラー: {e}")