#!/bin/bash
# 日次データ収集のcron設定スクリプト

PROJECT_ROOT="/Users/nishiharahiroto/Documents/programs/market-analytics-ver1"
VENV_PATH="$PROJECT_ROOT/venv"
SCRIPT_PATH="$PROJECT_ROOT/scripts/daily_collector.py"
LOG_PATH="$PROJECT_ROOT/logs"

echo "🔧 日次データ収集のcron設定"
echo "================================"

# ログディレクトリ作成
mkdir -p "$LOG_PATH"

# cronエントリ作成
CRON_ENTRY="0 18 * * * cd $PROJECT_ROOT && source $VENV_PATH/bin/activate && python $SCRIPT_PATH >> $LOG_PATH/cron_daily_collector.log 2>&1"

echo "設定予定のcronエントリ:"
echo "$CRON_ENTRY"
echo ""

# 現在のcron設定をバックアップ
crontab -l > "$PROJECT_ROOT/cron_backup_$(date +%Y%m%d_%H%M%S).txt" 2>/dev/null || echo "既存のcron設定なし"

# 新しいcronエントリを追加
(crontab -l 2>/dev/null || echo ""; echo "$CRON_ENTRY") | crontab -

if [ $? -eq 0 ]; then
    echo "✅ cron設定完了"
    echo "毎日18:00にデータ収集が実行されます"
    echo ""
    echo "現在のcron設定:"
    crontab -l
else
    echo "❌ cron設定に失敗しました"
    exit 1
fi

echo ""
echo "💡 手動テスト実行方法:"
echo "cd $PROJECT_ROOT && source $VENV_PATH/bin/activate && python $SCRIPT_PATH"