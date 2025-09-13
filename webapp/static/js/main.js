/**
 * Market Analytics - Main JavaScript
 * 共通のユーティリティ関数とイベントハンドラー
 */

// グローバル変数
const API_BASE_URL = '';
let currentChartInstance = null;

// ユーティリティ関数
const Utils = {
    /**
     * 数値を指定桁数で四捨五入
     */
    roundNumber: (num, digits = 3) => {
        return Math.round((num + Number.EPSILON) * Math.pow(10, digits)) / Math.pow(10, digits);
    },

    /**
     * 日付文字列をフォーマット
     */
    formatDate: (dateStr) => {
        const date = new Date(dateStr);
        return date.toLocaleDateString('ja-JP', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        });
    },

    /**
     * エラーメッセージを表示
     */
    showError: (message, title = 'エラー') => {
        // Bootstrap Toast or Alert を使用
        const alertDiv = document.createElement('div');
        alertDiv.className = 'alert alert-danger alert-dismissible fade show position-fixed';
        alertDiv.style.cssText = 'top: 20px; right: 20px; z-index: 9999; max-width: 400px;';
        alertDiv.innerHTML = `
            <strong>${title}</strong> ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        document.body.appendChild(alertDiv);
        
        // 5秒後に自動で削除
        setTimeout(() => {
            if (alertDiv.parentNode) {
                alertDiv.parentNode.removeChild(alertDiv);
            }
        }, 5000);
    },

    /**
     * 成功メッセージを表示
     */
    showSuccess: (message, title = '成功') => {
        const alertDiv = document.createElement('div');
        alertDiv.className = 'alert alert-success alert-dismissible fade show position-fixed';
        alertDiv.style.cssText = 'top: 20px; right: 20px; z-index: 9999; max-width: 400px;';
        alertDiv.innerHTML = `
            <strong>${title}</strong> ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        document.body.appendChild(alertDiv);
        
        setTimeout(() => {
            if (alertDiv.parentNode) {
                alertDiv.parentNode.removeChild(alertDiv);
            }
        }, 3000);
    },

    /**
     * ローディング状態の切り替え
     */
    setLoading: (element, isLoading) => {
        if (isLoading) {
            element.disabled = true;
            const originalText = element.textContent;
            element.dataset.originalText = originalText;
            element.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>読み込み中...';
        } else {
            element.disabled = false;
            element.textContent = element.dataset.originalText || 'データ読み込み';
        }
    }
};

// API呼び出し関数
const API = {
    /**
     * 利用可能な日付を取得
     */
    async getDates() {
        try {
            const response = await axios.get('/api/dates');
            return response.data;
        } catch (error) {
            throw new Error(`日付取得エラー: ${error.response?.data?.error || error.message}`);
        }
    },

    /**
     * イールドカーブデータを取得
     */
    async getYieldCurve(date) {
        try {
            const response = await axios.get(`/api/yield-curve/${date}`);
            return response.data;
        } catch (error) {
            throw new Error(`データ取得エラー: ${error.response?.data?.error || error.message}`);
        }
    },

    /**
     * 複数日比較データを取得
     */
    async compareYieldCurves(dates, maturityMin = 0, maturityMax = 50) {
        try {
            const dateStr = Array.isArray(dates) ? dates.join(',') : dates;
            const response = await axios.get('/api/compare', {
                params: {
                    dates: dateStr,
                    maturity_min: maturityMin,
                    maturity_max: maturityMax
                }
            });
            return response.data;
        } catch (error) {
            throw new Error(`比較データ取得エラー: ${error.response?.data?.error || error.message}`);
        }
    }
};

// Chart.js設定
const ChartConfig = {
    /**
     * デフォルトのChart.jsオプション
     */
    getDefaultOptions: (title) => ({
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            title: {
                display: true,
                text: title,
                font: {
                    size: 16,
                    weight: 'bold'
                }
            },
            legend: {
                display: true,
                position: 'top'
            },
            tooltip: {
                mode: 'index',
                intersect: false,
                backgroundColor: 'rgba(0,0,0,0.8)',
                titleColor: '#fff',
                bodyColor: '#fff',
                borderColor: '#ddd',
                borderWidth: 1
            }
        },
        scales: {
            x: {
                title: {
                    display: true,
                    text: '満期までの年数',
                    font: {
                        weight: 'bold'
                    }
                },
                grid: {
                    display: true,
                    color: 'rgba(0,0,0,0.1)'
                }
            },
            y: {
                title: {
                    display: true,
                    text: '利回り (%)',
                    font: {
                        weight: 'bold'
                    }
                },
                grid: {
                    display: true,
                    color: 'rgba(0,0,0,0.1)'
                }
            }
        },
        interaction: {
            mode: 'nearest',
            axis: 'x',
            intersect: false
        }
    }),

    /**
     * カラーパレット
     */
    colors: [
        '#0d6efd', // Blue
        '#dc3545', // Red
        '#198754', // Green
        '#ffc107', // Yellow
        '#6f42c1', // Purple
        '#fd7e14', // Orange
        '#20c997', // Teal
        '#e91e63'  // Pink
    ],

    /**
     * カラーを透明度付きで取得
     */
    getColorWithAlpha: (colorIndex, alpha = 0.2) => {
        const color = ChartConfig.colors[colorIndex % ChartConfig.colors.length];
        // hex to rgba conversion
        const r = parseInt(color.substr(1, 2), 16);
        const g = parseInt(color.substr(3, 2), 16);
        const b = parseInt(color.substr(5, 2), 16);
        return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    }
};

// 共通イベントハンドラー
document.addEventListener('DOMContentLoaded', function() {
    // ナビゲーションのアクティブ状態を設定
    const currentPath = window.location.pathname;
    const navLinks = document.querySelectorAll('.navbar-nav .nav-link');
    
    navLinks.forEach(link => {
        if (link.getAttribute('href') === currentPath) {
            link.classList.add('active');
        } else {
            link.classList.remove('active');
        }
    });

    // Bootstrap tooltipの初期化
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // 現在の日時を更新
    updateCurrentTime();
    setInterval(updateCurrentTime, 60000); // 1分ごとに更新
});

/**
 * 現在時刻の更新
 */
function updateCurrentTime() {
    const now = new Date();
    const timeString = now.toLocaleString('ja-JP', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    });
    
    const timeElements = document.querySelectorAll('#current-time');
    timeElements.forEach(el => {
        el.textContent = timeString;
    });
}

/**
 * ページタイトルの更新
 */
function updatePageTitle(title) {
    document.title = `${title} - Market Analytics`;
}

/**
 * チャートの破棄（メモリリーク防止）
 */
function destroyChart(chart) {
    if (chart && typeof chart.destroy === 'function') {
        chart.destroy();
    }
}

/**
 * データをCSVでエクスポート
 */
function exportToCSV(data, filename) {
    const csvContent = "data:text/csv;charset=utf-8," 
        + data.map(e => e.join(",")).join("\n");
    
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement("a");
    link.setAttribute("href", encodedUri);
    link.setAttribute("download", filename);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

/**
 * 印刷用にページを最適化
 */
function preparePrint() {
    window.print();
}

// Axiosのデフォルト設定
axios.defaults.timeout = 30000; // 30秒タイムアウト

// Axios interceptors
axios.interceptors.request.use(
    config => {
        // リクエスト開始時の処理
        return config;
    },
    error => {
        return Promise.reject(error);
    }
);

axios.interceptors.response.use(
    response => {
        return response;
    },
    error => {
        // エラーレスポンスの統一処理
        if (error.response) {
            // サーバーエラー (4xx, 5xx)
            console.error('API Error:', error.response.status, error.response.data);
        } else if (error.request) {
            // ネットワークエラー
            console.error('Network Error:', error.request);
        } else {
            // その他のエラー
            console.error('Error:', error.message);
        }
        return Promise.reject(error);
    }
);

// グローバルにユーティリティを公開
window.Utils = Utils;
window.API = API;
window.ChartConfig = ChartConfig;