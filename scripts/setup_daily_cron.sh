#!/bin/bash
# 日次データ収集のcron設定スクリプト

PROJECT_ROOT="/Users/nishiharahiroto/Documents/programs/market-analytics-ver1"
VENV_PATH="$PROJECT_ROOT/venv"

# スクリプトパス
SCRIPT_JSDA="$PROJECT_ROOT/scripts/collectors/daily_collector.py"
SCRIPT_IRS="$PROJECT_ROOT/scripts/collect_irs_data.py"
SCRIPT_ASW="$PROJECT_ROOT/scripts/runners/daily_asw_runner.py"

LOG_PATH="$PROJECT_ROOT/logs"

echo "🔧 日次データ収集・計算のcron設定"
echo "================================"

# ログディレクトリ作成
mkdir -p "$LOG_PATH"

# cronエントリ作成
# 1. 毎日18:00にJSDAデータ収集
CRON_JSDA="0 18 * * * cd $PROJECT_ROOT && source $VENV_PATH/bin/activate && python $SCRIPT_JSDA >> $LOG_PATH/cron_daily_collector.log 2>&1"

# 2. 毎日21:00にIRSデータ収集
CRON_IRS="0 21 * * * cd $PROJECT_ROOT && source $VENV_PATH/bin/activate && python $SCRIPT_IRS >> $LOG_PATH/cron_irs_collector.log 2>&1"

# 3. 毎日21:30にASW計算 (JSDAとIRSが揃った後に実行)
CRON_ASW="30 21 * * * cd $PROJECT_ROOT && source $VENV_PATH/bin/activate && python $SCRIPT_ASW >> $LOG_PATH/cron_asw_runner.log 2>&1"

echo "設定予定のcronエントリ:"
echo "1. JSDA (18:00): $CRON_JSDA"
echo "2. IRS  (21:00): $CRON_IRS"
echo "3. ASW  (21:30): $CRON_ASW"
echo ""

# 現在のcron設定をバックアップ
crontab -l > "$PROJECT_ROOT/cron_backup_$(date +%Y%m%d_%H%M%S).txt" 2>/dev/null || echo "既存のcron設定なし"

# 新しいcronエントリを追加
# 既存の設定からJSDA/IRS/ASW関連の行を削除して再追加（重複防止）
(crontab -l 2>/dev/null | grep -v "daily_collector.py" | grep -v "collect_irs_data.py" | grep -v "daily_asw_runner.py" || echo ""; echo "$CRON_JSDA"; echo "$CRON_IRS"; echo "$CRON_ASW") | crontab -

if [ $? -eq 0 ]; then
    echo "✅ cron設定完了"
    echo "毎日18:00: JSDAデータ収集"
    echo "毎日21:00: IRSデータ収集"
    echo "毎日21:30: ASW計算実行"
    echo ""
    echo "現在のcron設定:"
    crontab -l
else
    echo "❌ cron設定に失敗しました"
    echo "macOSの場合、ターミナル(iTermなど)に「フルディスクアクセス」権限が必要な場合があります。"
    echo "システム環境設定 > セキュリティとプライバシー > プライバシー > フルディスクアクセス を確認してください。"
    exit 1
fi

echo ""
echo "💡 手動テスト実行方法:"
echo "JSDA: cd $PROJECT_ROOT && source $VENV_PATH/bin/activate && python $SCRIPT_JSDA"
echo "IRS : cd $PROJECT_ROOT && source $VENV_PATH/bin/activate && python $SCRIPT_IRS"