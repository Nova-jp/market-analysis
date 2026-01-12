// グローバル変数
let selectedDates = [];
let marketAmountChart = null;
let diffChart = null;
let bucketSize = 1.0;
let maturityFilter = { min: null, max: null };
const MAX_DATES = 20;
let bondDetailChart = null;
let selectedBonds = new Map(); // key: bondCode, value: bondData
const MAX_BONDS = 10;
const colors = [
    '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
    '#FF9F40', '#FF6384', '#C9CBCF', '#4BC0C0', '#FF6384',
    '#9966FF', '#36A2EB', '#FFCD56', '#FF9F40', '#4BC0C0',
    '#C9CBCF', '#FF6384', '#36A2EB', '#FFCE56', '#9966FF'
];

// 初期化
document.addEventListener('DOMContentLoaded', function() {
    console.log('Market Amount page initializing...');
    try {
        initChart();
        initDiffChart();
        initBondDetailChart();
        setupEventListeners();
        console.log('Initialization complete.');
    } catch (e) {
        console.error('Initialization failed:', e);
    }
});

// グラフ初期化
function initChart() {
    console.log('Initializing main chart...');
    const ctx = document.getElementById('marketAmountChart').getContext('2d');
    marketAmountChart = new Chart(ctx, {
        type: 'line',
        data: { datasets: [] },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    type: 'linear',
                    position: 'bottom',
                    min: 0,
                    max: 40,
                    ticks: {
                        stepSize: 5,
                        callback: function(value) {
                            return value + '年';
                        }
                    },
                    title: {
                        display: true,
                        text: '残存年限 (年)',
                        font: { size: 14, weight: 'bold' }
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: '市中残存額 (億円)',
                        font: { size: 14, weight: 'bold' }
                    },
                    ticks: {
                        callback: function(value) {
                            return value.toLocaleString();
                        }
                    }
                }
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                },
                title: {
                    display: true,
                    text: '市中残存額分布 (1年区切り)'
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        title: function(context) {
                            const year = context[0].parsed.x;
                            return `残存年限: ${year}～${year+bucketSize}年`;
                        },
                        label: function(context) {
                            const date = context.dataset.label;
                            const amount = context.parsed.y.toLocaleString();
                            return `${date}: ${amount}億円`;
                        }
                    }
                }
            },
            interaction: {
                intersect: false,
                mode: 'index'
            }
        }
    });
}

// イベントリスナー設定
function setupEventListeners() {
    console.log('Setting up event listeners...');
    const dateInput = document.getElementById('dateInput');
    const addDateBtn = document.getElementById('addDateBtn');
    const clearAllBtn = document.getElementById('clearAllBtn');

    if (!dateInput || !addDateBtn) {
        console.error('Required DOM elements not found!');
        return;
    }

    // 日付入力フィールドのキー入力制御
    dateInput.addEventListener('keydown', function(e) {
        const allowedKeys = [
            'Backspace', 'Delete', 'Tab', 'ArrowLeft', 'ArrowRight',
            'ArrowUp', 'ArrowDown', 'Home', 'End', 'Enter'
        ];

        const isCtrlCmd = e.ctrlKey || e.metaKey;
        const isAllowedCtrl = isCtrlCmd && ['a', 'c', 'v', 'x'].includes(e.key);
        const isNumber = /^[0-9]$/.test(e.key);
        const isHyphen = e.key === '-';

        if (!allowedKeys.includes(e.key) && !isAllowedCtrl && !isNumber && !isHyphen) {
            e.preventDefault();
        }
    });

    // Enterキーで日付追加
    dateInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            console.log('Enter key pressed in dateInput');
            addDateFromInput();
        }
    });

    // 追加ボタン
    addDateBtn.addEventListener('click', function() {
        console.log('Add button clicked');
        addDateFromInput();
    });

    // 全削除ボタン
    clearAllBtn.addEventListener('click', function() {
        selectedDates = [];
        updateDisplay();
        updateChart();
        updateStats();
    });

    // 区切り幅選択イベント
    const bucketSizeSelector = document.getElementById('bucketSizeSelector');
    bucketSizeSelector.addEventListener('change', function(e) {
        bucketSize = parseFloat(e.target.value);
        showMessage(`区切り幅を${bucketSize}年に変更しました`, 'info');
        updateChart();
    });

    // 横軸フィルター適用
    const applyFilterBtn = document.getElementById('applyFilter');
    applyFilterBtn.addEventListener('click', function() {
        const minVal = parseFloat(document.getElementById('minMaturity').value);
        const maxVal = parseFloat(document.getElementById('maxMaturity').value);

        maturityFilter.min = isNaN(minVal) ? null : minVal;
        maturityFilter.max = isNaN(maxVal) ? null : maxVal;

        if (maturityFilter.min !== null && maturityFilter.max !== null &&
            maturityFilter.min > maturityFilter.max) {
            showMessage('最小値は最大値より小さくしてください', 'warning');
            return;
        }

        showMessage('年限フィルターを適用しました', 'success');
        updateChart();
    });

    // フィルタークリア
    const clearFilterBtn = document.getElementById('clearFilter');
    clearFilterBtn.addEventListener('click', function() {
        maturityFilter.min = null;
        maturityFilter.max = null;
        document.getElementById('minMaturity').value = '';
        document.getElementById('maxMaturity').value = '';
        showMessage('年限フィルターをクリアしました', 'info');
        updateChart();
    });

    // 銘柄検索ボタン
    document.getElementById('searchBondBtn').addEventListener('click', function() {
        const bondCode = document.getElementById('bondCodeInput').value.trim();
        if (bondCode.length === 9) {
            loadBondDetail(bondCode);
        } else {
            showMessage('9桁の銘柄コードを入力してください', 'warning');
        }
    });

    // Enterキーで検索
    document.getElementById('bondCodeInput').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            document.getElementById('searchBondBtn').click();
        }
    });
}

// 入力欄から日付を追加
function addDateFromInput() {
    const dateInput = document.getElementById('dateInput');
    const date = dateInput.value.trim();

    if (!date) {
        showMessage('日付を入力してください', 'warning');
        return;
    }

    if (!isValidDateFormat(date)) {
        showMessage('正しい日付形式で入力してください (YYYY-MM-DD)', 'warning');
        return;
    }

    addDate(date, true);

    if (selectedDates.includes(date)) {
        dateInput.value = '';
    }
}

// 日付形式バリデーション
function isValidDateFormat(dateStr) {
    const regex = /^\d{4}-\d{2}-\d{2}$/;
    if (!regex.test(dateStr)) return false;

    const date = new Date(dateStr);
    return date instanceof Date && !isNaN(date);
}

// 日付追加
function addDate(date, fromUserInput = false) {
    if (selectedDates.length >= MAX_DATES) {
        showMessage(`最大${MAX_DATES}個まで選択できます`, 'warning');
        return;
    }

    if (selectedDates.includes(date)) {
        showMessage('この日付は既に選択されています', 'info');
        return;
    }

    selectedDates.push(date);
    updateDisplay();
    loadMarketAmountData(date, fromUserInput);
}

// 日付削除
function removeDate(date) {
    selectedDates = selectedDates.filter(d => d !== date);
    updateDisplay();
    updateChart();
    updateStats();
}

// 表示更新
function updateDisplay() {
    const container = document.getElementById('selectedDates');
    const countElement = document.getElementById('dateCount');

    countElement.textContent = selectedDates.length;

    if (selectedDates.length === 0) {
        container.innerHTML = '<small class="text-muted">日付を選択してください</small>';
        return;
    }

    container.innerHTML = selectedDates.map(date => `
        <span class="date-chip">
            ${date}
            <span class="remove-btn" onclick="removeDate('${date}')">&times;</span>
        </span>
    `).join('');
}

// 市中残存額データ取得
async function loadMarketAmountData(date, fromUserInput = false) {
    try {
        const response = await fetch(`/api/market-amount/${date}`);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();

        if (data.error || !data.buckets || data.buckets.length === 0) {
            showMessage(`${date} のデータが見つかりません`, 'warning');
            removeDate(date);

            if (fromUserInput) {
                document.getElementById('dateInput').value = date;
            }
            return;
        }

        showMessage(
            `${date} のデータを取得 (総額: ${data.total_amount.toLocaleString()}億円, 銘柄数: ${data.bond_count})`,
            'success'
        );
        updateChart();
        updateStats();

    } catch (error) {
        console.error(`${date} データ取得エラー:`, error);
        showMessage(`${date} のデータ取得に失敗しました`, 'danger');
        removeDate(date);

        if (fromUserInput) {
            document.getElementById('dateInput').value = date;
        }
    }
}

// グラフ更新
async function updateChart() {
    marketAmountChart.data.datasets = [];

    for (let i = 0; i < selectedDates.length; i++) {
        const date = selectedDates[i];

        try {
            const response = await fetch(`/api/market-amount/${date}?bucket_size=${bucketSize}`);
            const data = await response.json();

            if (!data.error && data.buckets && data.buckets.length > 0) {
                // バケットデータを実際の年限に変換
                let filteredData = data.buckets.map(bucket => ({
                    x: bucket.year * bucketSize,
                    y: bucket.amount
                }));

                // 横軸フィルター適用
                if (maturityFilter.min !== null || maturityFilter.max !== null) {
                    filteredData = filteredData.filter(point => {
                        if (maturityFilter.min !== null && point.x < maturityFilter.min) return false;
                        if (maturityFilter.max !== null && point.x > maturityFilter.max) return false;
                        return true;
                    });
                }

                marketAmountChart.data.datasets.push({
                    label: date,
                    data: filteredData,
                    borderColor: colors[i % colors.length],
                    backgroundColor: colors[i % colors.length] + '20',
                    fill: false,
                    tension: 0.1,
                    pointRadius: 3
                });
            }
        } catch (error) {
            console.error(`${date} グラフデータ処理エラー:`, error);
        }
    }

    marketAmountChart.update();
    updateDiffChart();
}

// 統計情報更新
async function updateStats() {
    const statsCard = document.getElementById('statsCard');
    const statsContent = document.getElementById('statsContent');

    if (selectedDates.length === 0) {
        statsCard.style.display = 'none';
        return;
    }

    statsCard.style.display = 'block';
    let statsHtml = '';

    for (const date of selectedDates) {
        try {
            const response = await fetch(`/api/market-amount/${date}`);
            const data = await response.json();

            if (!data.error) {
                statsHtml += `
                    <div class="mb-2">
                        <strong>${date}</strong><br>
                        <small class="text-muted">
                            総額: ${data.total_amount.toLocaleString()}億円<br>
                            銘柄数: ${data.bond_count}銘柄
                        </small>
                    </div>
                `;
            }
        } catch (error) {
            console.error(`${date} 統計データ取得エラー:`, error);
        }
    }

    statsContent.innerHTML = statsHtml;
}

// メッセージ表示
function showMessage(message, type = 'info') {
    const existingAlert = document.querySelector('.alert-temp');
    if (existingAlert) {
        existingAlert.remove();
    }

    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type} alert-dismissible alert-temp mb-0`;
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;

    const messageContainer = document.getElementById('messageContainer');
    messageContainer.appendChild(alertDiv);

    setTimeout(() => {
        if (alertDiv && alertDiv.parentNode) {
            alertDiv.remove();
        }
    }, 3000);
}

// 差分グラフ初期化
function initDiffChart() {
    const ctx = document.getElementById('diffChart').getContext('2d');
    diffChart = new Chart(ctx, {
        type: 'line',
        data: { datasets: [] },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    type: 'linear',
                    position: 'bottom',
                    min: 0,
                    max: 40,
                    ticks: {
                        stepSize: 5,
                        callback: function(value) {
                            return value + '年';
                        }
                    },
                    title: {
                        display: true,
                        text: '残存年限 (年)',
                        font: { size: 14, weight: 'bold' }
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: '差分 (億円)',
                        font: { size: 14, weight: 'bold' }
                    },
                    ticks: {
                        callback: function(value) {
                            const sign = value >= 0 ? '+' : '';
                            return sign + value.toLocaleString();
                        }
                    }
                }
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                },
                title: {
                    display: true,
                    text: '基準日付からの差分'
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        title: function(context) {
                            const year = context[0].parsed.x;
                            return `残存年限: ${year}～${year+bucketSize}年`;
                        },
                        label: function(context) {
                            const date = context.dataset.label;
                            const amount = context.parsed.y;
                            const sign = amount >= 0 ? '+' : '';
                            return `${date}: ${sign}${amount.toLocaleString()}億円`;
                        }
                    }
                }
            },
            interaction: {
                intersect: false,
                mode: 'index'
            }
        }
    });
}

// 差分グラフ更新
async function updateDiffChart() {
    const diffChartCard = document.getElementById('diffChartCard');

    // 2日付以上ない場合は非表示
    if (selectedDates.length < 2) {
        diffChartCard.style.display = 'none';
        return;
    }

    diffChartCard.style.display = 'block';

    // 日付をソートして最古日付を基準とする
    const sortedDates = [...selectedDates].sort();
    const baseDate = sortedDates[0];
    document.getElementById('baseDate').textContent = baseDate;

    try {
        // 基準日付のデータ取得
        const baseResponse = await fetch(`/api/market-amount/${baseDate}?bucket_size=${bucketSize}`);
        const baseData = await baseResponse.json();

        if (baseData.error || !baseData.buckets) {
            diffChartCard.style.display = 'none';
            return;
        }

        // 基準データをマップに変換
        const baseMap = new Map();
        baseData.buckets.forEach(bucket => {
            baseMap.set(bucket.year, bucket.amount);
        });

        // 他の日付の差分を計算
        diffChart.data.datasets = [];

        for (let i = 1; i < sortedDates.length; i++) {
            const date = sortedDates[i];

            try {
                const response = await fetch(`/api/market-amount/${date}?bucket_size=${bucketSize}`);
                const data = await response.json();

                if (!data.error && data.buckets && data.buckets.length > 0) {
                    let diffData = data.buckets.map(bucket => {
                        const baseAmount = baseMap.get(bucket.year) || 0;
                        return {
                            x: bucket.year * bucketSize,  // 実際の年限
                            y: bucket.amount - baseAmount  // 差分
                        };
                    });

                    // 横軸フィルター適用
                    if (maturityFilter.min !== null || maturityFilter.max !== null) {
                        diffData = diffData.filter(point => {
                            if (maturityFilter.min !== null && point.x < maturityFilter.min) return false;
                            if (maturityFilter.max !== null && point.x > maturityFilter.max) return false;
                            return true;
                        });
                    }

                    diffChart.data.datasets.push({
                        label: date,
                        data: diffData,
                        borderColor: colors[i % colors.length],
                        backgroundColor: colors[i % colors.length] + '20',
                        fill: false,
                        tension: 0.1,
                        pointRadius: 3
                    });
                }
            } catch (error) {
                console.error(`${date} 差分データ処理エラー:`, error);
            }
        }

        diffChart.update();

    } catch (error) {
        console.error('差分グラフ更新エラー:', error);
        diffChartCard.style.display = 'none';
    }
}

// 銘柄詳細チャート初期化
function initBondDetailChart() {
    const ctx = document.getElementById('bondDetailChart').getContext('2d');
    bondDetailChart = new Chart(ctx, {
        type: 'line',
        data: { datasets: [] },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'month',
                        displayFormats: {
                            month: 'YYYY-MM'
                        }
                    },
                    title: {
                        display: true,
                        text: '取引日',
                        font: { size: 14, weight: 'bold' }
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: '市中残存額 (億円)',
                        font: { size: 14, weight: 'bold' }
                    },
                    ticks: {
                        callback: function(value) {
                            return value.toLocaleString();
                        }
                    }
                }
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        title: function(context) {
                            const date = new Date(context[0].parsed.x);
                            return date.toISOString().split('T')[0];
                        },
                        label: function(context) {
                            const label = context.dataset.label || '';
                            const amount = context.parsed.y.toLocaleString();
                            return `${label}: ${amount}億円`;
                        }
                    }
                }
            }
        }
    });
}

// 銘柄詳細データ取得（複数選択対応）
async function loadBondDetail(bondCode) {
    // 既に選択済みの場合はスキップ
    if (selectedBonds.has(bondCode)) {
        showMessage(`銘柄 ${bondCode} は既に選択されています`, 'info');
        return;
    }

    if (selectedBonds.size >= MAX_BONDS) {
        showMessage(`同時に表示できる銘柄は最大${MAX_BONDS}件です`, 'warning');
        return;
    }

    try {
        const response = await fetch(`/api/market-amount/bond/${bondCode}`);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();

        // データ追加
        selectedBonds.set(bondCode, data);

        // 表示更新
        updateBondListDisplay();
        updateBondDetailChart();
        displayBondStatistics(data.statistics);

        showMessage(`${data.bond_name} のデータを追加しました`, 'success');

    } catch (error) {
        console.error('銘柄詳細取得エラー:', error);
        showMessage(`銘柄コード ${bondCode} のデータ取得に失敗しました`, 'danger');
    }
}

// 選択中の銘柄一覧表示更新
function updateBondListDisplay() {
    const container = document.getElementById('selectedBondsContainer');
    
    if (selectedBonds.size === 0) {
        container.innerHTML = '<small class="text-muted">銘柄を選択してください</small>';
        return;
    }

    let html = '';
    selectedBonds.forEach((data, code) => {
        html += `
            <span class="date-chip" style="background-color: #17a2b8;">
                ${code}
                <span class="remove-btn" onclick="removeBond('${code}')">&times;</span>
            </span>
        `;
    });
    container.innerHTML = html;
}

// 銘柄削除
function removeBond(bondCode) {
    if (selectedBonds.delete(bondCode)) {
        updateBondListDisplay();
        updateBondDetailChart();
        
        if (selectedBonds.size > 0) {
            // 残っている銘柄のうち、最後のものの統計情報を表示
            const lastData = Array.from(selectedBonds.values()).pop();
            displayBondStatistics(lastData.statistics);
        } else {
            // 統計情報をクリア
            document.getElementById('bondStats').innerHTML = `
                <div class="col-12 text-center text-muted">
                    <small>銘柄を選択すると統計情報が表示されます</small>
                </div>
            `;
        }
    }
}

// チャート更新（複数データセット対応）
function updateBondDetailChart() {
    const datasets = [];
    let i = 0;

    selectedBonds.forEach((data, code) => {
        const chartData = data.timeseries.map(point => ({
            x: new Date(point.trade_date),
            y: point.market_amount
        }));

        const color = colors[i % colors.length];

        datasets.push({
            label: `${code} (${data.bond_name})`,
            data: chartData,
            borderColor: color,
            backgroundColor: color + '20', // 透明度追加
            fill: false, // 複数表示時は塗りつぶしなしの方が見やすい
            tension: 0.1,
            pointRadius: 2
        });
        i++;
    });

    bondDetailChart.data.datasets = datasets;
    bondDetailChart.update();
}

// 統計情報表示
function displayBondStatistics(stats) {
    const statsHtml = `
        <div class="col-md-3">
            <div class="card bg-light">
                <div class="card-body text-center">
                    <h6 class="text-muted">最新日付</h6>
                    <h5>${stats.latest_date}</h5>
                </div>
            </div>
        </div>
        <div class="col-md-3">
            <div class="card bg-light">
                <div class="card-body text-center">
                    <h6 class="text-muted">最新残存額</h6>
                    <h5>${stats.latest_amount.toLocaleString()}億円</h5>
                </div>
            </div>
        </div>
        <div class="col-md-3">
            <div class="card bg-light">
                <div class="card-body text-center">
                    <h6 class="text-muted">平均残存額</h6>
                    <h5>${stats.avg_amount.toLocaleString()}億円</h5>
                </div>
            </div>
        </div>
        <div class="col-md-3">
            <div class="card bg-light">
                <div class="card-body text-center">
                    <h6 class="text-muted">データ数</h6>
                    <h5>${stats.data_points}日分</h5>
                </div>
            </div>
        </div>
    `;

    document.getElementById('bondStats').innerHTML = statsHtml;
}