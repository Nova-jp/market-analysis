/**
 * PCA Analysis JavaScript
 * 主成分分析のフロントエンドロジック
 * Last updated: 2025-11-10 00:00 (localStorage quota fix)
 */

// グローバル変数
let pcaData = null;
let charts = {
    components: null,
    scores: null,
    errors: null
};
let selectedReconstructionDate = null;  // 復元結果テーブル用
let selectedErrorsDate = null;  // 復元誤差グラフ用
let maturityFilter = { min: null, max: null };  // 年限範囲フィルター

// 主成分のラベルと色
const PC_LABELS = ['Level', 'Slope', 'Curvature', 'PC4', 'PC5'];
const PC_COLORS = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6'];

// 初期化
document.addEventListener('DOMContentLoaded', function() {
    console.log('PCA.js loaded - version 2025-11-09 23:20');
    console.log('localStorage available:', typeof(Storage) !== "undefined");

    initializeEventListeners();
    updateParameterDisplays();

    // 保存された結果を復元
    console.log('Attempting to load saved state...');
    loadSavedState();
});

// イベントリスナー設定
function initializeEventListeners() {
    // スライダーの値表示更新
    document.getElementById('lookbackDays').addEventListener('input', updateParameterDisplays);
    document.getElementById('nComponents').addEventListener('input', updateParameterDisplays);

    // 分析実行ボタン
    document.getElementById('analyzeBtn').addEventListener('click', runPCAAnalysis);

    // 日付選択イベント
    document.getElementById('reconstructionDateSelect').addEventListener('change', onReconstructionDateChange);
    document.getElementById('errorsDateSelect').addEventListener('change', onErrorsDateChange);

    // 年限フィルターイベント
    document.getElementById('applyFilter').addEventListener('click', applyMaturityFilter);
    document.getElementById('clearFilter').addEventListener('click', clearMaturityFilter);
}

// パラメータ表示を更新
function updateParameterDisplays() {
    const days = document.getElementById('lookbackDays').value;
    const components = document.getElementById('nComponents').value;

    document.getElementById('lookbackDaysValue').textContent = `${days}日`;
    document.getElementById('nComponentsValue').textContent = components;
}

// エラー表示
function showError(message) {
    const errorAlert = document.getElementById('errorAlert');
    const errorMessage = document.getElementById('errorMessage');

    errorMessage.textContent = message;
    errorAlert.style.display = 'block';

    // 5秒後に非表示
    setTimeout(() => {
        errorAlert.style.display = 'none';
    }, 5000);
}

// ローディング表示制御
function showLoading(show) {
    const overlay = document.getElementById('loadingOverlay');
    if (show) {
        overlay.classList.add('active');
    } else {
        overlay.classList.remove('active');
    }
}

// PCA分析を実行
async function runPCAAnalysis() {
    const days = document.getElementById('lookbackDays').value;
    const components = document.getElementById('nComponents').value;

    showLoading(true);

    try {
        const response = await fetch(`/api/pca/analyze?days=${days}&components=${components}`);

        if (!response.ok) {
            throw new Error(`APIエラー: ${response.status}`);
        }

        pcaData = await response.json();

        // 結果を表示
        displayResults();

        // 結果を保存
        saveState();

    } catch (error) {
        console.error('PCA分析エラー:', error);
        showError(`分析中にエラーが発生しました: ${error.message}`);
    } finally {
        showLoading(false);
    }
}

// 結果を表示
function displayResults() {
    if (!pcaData) return;

    // 結果エリアを表示
    document.getElementById('resultsArea').style.display = 'block';

    // 日付選択の初期化
    selectedReconstructionDate = pcaData.reconstruction.latest_date;
    selectedErrorsDate = pcaData.reconstruction.latest_date;
    populateDateSelectors();

    // サマリー統計を更新
    updateSummaryStats();

    // グラフを描画
    drawComponentsChart();
    drawScoresChart();
    drawErrorsChart();

    // テーブルを更新
    updateVarianceTable();
    updateReconstructionTable();
    updateErrorStatsTable();
}

// 日付セレクターにオプションを追加
function populateDateSelectors() {
    const dates = pcaData.reconstruction.dates;
    const reconstructionSelect = document.getElementById('reconstructionDateSelect');
    const errorsSelect = document.getElementById('errorsDateSelect');

    console.log('populateDateSelectors: 日付数 =', dates.length);
    console.log('populateDateSelectors: 最初の5つの日付 =', dates.slice(0, 5));

    // 既存のオプションをクリア（最初の「選択してください」以外）
    reconstructionSelect.innerHTML = '';
    errorsSelect.innerHTML = '';

    // 日付オプションを追加（最新日が上）
    dates.forEach(date => {
        const option1 = document.createElement('option');
        option1.value = date;
        option1.textContent = date;
        if (date === selectedReconstructionDate) {
            option1.selected = true;
        }
        reconstructionSelect.appendChild(option1);

        const option2 = document.createElement('option');
        option2.value = date;
        option2.textContent = date;
        if (date === selectedErrorsDate) {
            option2.selected = true;
        }
        errorsSelect.appendChild(option2);
    });
}

// 復元結果テーブルの日付変更ハンドラー
function onReconstructionDateChange(event) {
    selectedReconstructionDate = event.target.value;
    updateReconstructionTable();
    updateErrorStatsTable();
}

// 復元誤差グラフの日付変更ハンドラー
function onErrorsDateChange(event) {
    selectedErrorsDate = event.target.value;
    drawErrorsChart();
}

// 年限フィルターを適用
function applyMaturityFilter() {
    const minVal = parseFloat(document.getElementById('minMaturity').value);
    const maxVal = parseFloat(document.getElementById('maxMaturity').value);

    maturityFilter.min = isNaN(minVal) ? null : minVal;
    maturityFilter.max = isNaN(maxVal) ? null : maxVal;

    if (maturityFilter.min !== null && maturityFilter.max !== null &&
        maturityFilter.min > maturityFilter.max) {
        showError('最小値は最大値より小さくしてください');
        return;
    }

    drawErrorsChart();
}

// 年限フィルターをクリア
function clearMaturityFilter() {
    maturityFilter.min = null;
    maturityFilter.max = null;
    document.getElementById('minMaturity').value = '';
    document.getElementById('maxMaturity').value = '';
    drawErrorsChart();
}

// サマリー統計を更新
function updateSummaryStats() {
    // 累積寄与率
    document.getElementById('statVariance').textContent =
        `${(pcaData.pca_model.cumulative_variance_ratio * 100).toFixed(1)}%`;

    // 分析日数
    document.getElementById('statDays').textContent =
        pcaData.parameters.valid_dates_count;
}

// 主成分ベクトルのグラフを描画
function drawComponentsChart() {
    const ctx = document.getElementById('componentsChart').getContext('2d');

    // 既存のチャートを破棄
    if (charts.components) {
        charts.components.destroy();
    }

    const commonGrid = pcaData.common_grid;
    const components = pcaData.pca_model.components;
    const nComponents = components.length;

    // データセットを作成
    const datasets = [];
    for (let i = 0; i < nComponents; i++) {
        datasets.push({
            label: `PC${i+1} (${PC_LABELS[i]})`,
            data: commonGrid.map((x, idx) => ({x: x, y: components[i][idx]})),
            borderColor: PC_COLORS[i],
            backgroundColor: PC_COLORS[i] + '20',
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.4
        });
    }

    charts.components = new Chart(ctx, {
        type: 'line',
        data: { datasets: datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: '主成分ベクトル (固有ベクトル)',
                    font: { size: 16 }
                },
                legend: {
                    position: 'top'
                }
            },
            scales: {
                x: {
                    type: 'linear',
                    title: {
                        display: true,
                        text: '残存期間 (年)'
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: 'Loading'
                    },
                    grid: {
                        drawBorder: true,
                        color: function(context) {
                            if (context.tick.value === 0) {
                                return '#000';
                            }
                            return '#e0e0e0';
                        },
                        lineWidth: function(context) {
                            if (context.tick.value === 0) {
                                return 2;
                            }
                            return 1;
                        }
                    }
                }
            }
        }
    });
}

// 主成分スコアのグラフを描画
function drawScoresChart() {
    const ctx = document.getElementById('scoresChart').getContext('2d');

    // 既存のチャートを破棄
    if (charts.scores) {
        charts.scores.destroy();
    }

    const dates = pcaData.principal_component_scores.dates;
    const scores = pcaData.principal_component_scores.scores;
    const nComponents = scores[0].length;

    // データセットを作成
    const datasets = [];
    for (let i = 0; i < nComponents; i++) {
        datasets.push({
            label: `PC${i+1} (${PC_LABELS[i]})`,
            data: dates.map((date, idx) => ({x: date, y: scores[idx][i]})),
            borderColor: PC_COLORS[i],
            backgroundColor: PC_COLORS[i] + '40',
            borderWidth: 2,
            pointRadius: 3,
            pointHoverRadius: 5,
            tension: 0.1
        });
    }

    charts.scores = new Chart(ctx, {
        type: 'line',
        data: { datasets: datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: '主成分スコア時系列 (直近30日)',
                    font: { size: 16 }
                },
                legend: {
                    position: 'top'
                }
            },
            scales: {
                x: {
                    type: 'category',
                    title: {
                        display: true,
                        text: '日付'
                    },
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: 'スコア'
                    },
                    grid: {
                        drawBorder: true,
                        color: function(context) {
                            if (context.tick.value === 0) {
                                return '#000';
                            }
                            return '#e0e0e0';
                        },
                        lineWidth: function(context) {
                            if (context.tick.value === 0) {
                                return 2;
                            }
                            return 1;
                        }
                    }
                }
            }
        }
    });
}

// 復元誤差のグラフを描画
function drawErrorsChart() {
    const ctx = document.getElementById('errorsChart').getContext('2d');

    // 既存のチャートを破棄
    if (charts.errors) {
        charts.errors.destroy();
    }

    const dateData = pcaData.reconstruction.all_dates[selectedErrorsDate];
    let reconstructionData = dateData.data;

    // 年限フィルターを適用
    if (maturityFilter.min !== null || maturityFilter.max !== null) {
        reconstructionData = reconstructionData.filter(item => {
            const maturity = item.maturity;
            if (maturityFilter.min !== null && maturity < maturityFilter.min) {
                return false;
            }
            if (maturityFilter.max !== null && maturity > maturityFilter.max) {
                return false;
            }
            return true;
        });
    }

    // 銘柄コードの下4桁でグループ化し、年限ごとに統合
    const bondYearGroups = {};
    reconstructionData.forEach(item => {
        const bondCode = item.bond_code || '';
        const bondType = bondCode.slice(-4);  // 下4桁を取得

        // 下4桁から年限を判定
        let yearLabel = '';
        if (bondType === '0032' || bondType === '0042') {
            yearLabel = '2年債';
        } else if (bondType === '0045' || bondType === '0057') {
            yearLabel = '5年債';
        } else if (bondType === '0058' || bondType === '0067') {
            yearLabel = '10年債';
        } else if (bondType === '0069') {
            yearLabel = '20年債';
        } else if (bondType === '0068') {
            yearLabel = '30年債';
        } else if (bondType === '0054') {
            yearLabel = '40年債';
        } else {
            yearLabel = 'その他';
        }

        if (!bondYearGroups[yearLabel]) {
            bondYearGroups[yearLabel] = [];
        }
        bondYearGroups[yearLabel].push({
            x: item.maturity,
            y: item.error * 100,  // bp単位に変換
            bond_name: item.bond_name,
            bond_code: item.bond_code
        });
    });

    // 債券年限ごとの色設定
    const bondYearColors = {
        '2年債': '#e74c3c',   // 赤
        '5年債': '#3498db',   // 青
        '10年債': '#2ecc71',  // 緑
        '20年債': '#f39c12',  // オレンジ
        '30年債': '#9b59b6',  // 紫
        '40年債': '#e67e22',  // ダークオレンジ
        'その他': '#95a5a6'   // グレー
    };

    // データセットを作成（年限順にソート）
    const yearOrder = ['2年債', '5年債', '10年債', '20年債', '30年債', '40年債', 'その他'];
    const datasets = yearOrder
        .filter(year => bondYearGroups[year])  // 存在する年限のみ
        .map(year => {
            const color = bondYearColors[year];
            return {
                label: year,
                data: bondYearGroups[year],
                backgroundColor: color + '80',
                borderColor: color,
                pointRadius: 4,
                pointHoverRadius: 6
            };
        });

    charts.errors = new Chart(ctx, {
        type: 'scatter',
        data: { datasets: datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: '復元誤差分布（債券種類別）',
                    font: { size: 16 }
                },
                legend: {
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const dataPoint = context.raw;
                            return [
                                `銘柄名: ${dataPoint.bond_name}`,
                                `銘柄コード: ${dataPoint.bond_code}`,
                                `残存期間: ${dataPoint.x.toFixed(3)}年`,
                                `誤差: ${dataPoint.y.toFixed(3)}bp`
                            ];
                        }
                    }
                }
            },
            scales: {
                x: {
                    type: 'linear',
                    title: {
                        display: true,
                        text: '残存期間 (年)'
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: '誤差 (bp)'
                    },
                    grid: {
                        drawBorder: true,
                        color: function(context) {
                            if (context.tick.value === 0) {
                                return '#000';
                            }
                            return '#e0e0e0';
                        },
                        lineWidth: function(context) {
                            if (context.tick.value === 0) {
                                return 2;
                            }
                            return 1;
                        }
                    }
                }
            }
        }
    });
}

// 寄与率テーブルを更新
function updateVarianceTable() {
    const tbody = document.getElementById('varianceTable');
    const variance = pcaData.pca_model.explained_variance_ratio;

    tbody.innerHTML = '';

    for (let i = 0; i < variance.length; i++) {
        const row = tbody.insertRow();
        row.innerHTML = `
            <td><strong>PC${i+1}</strong></td>
            <td>${PC_LABELS[i]}</td>
            <td><span class="badge" style="background-color: ${PC_COLORS[i]}">${(variance[i] * 100).toFixed(3)}%</span></td>
        `;
    }
}

// 復元結果テーブルを更新
function updateReconstructionTable() {
    const tbody = document.getElementById('reconstructionTable');
    const dateData = pcaData.reconstruction.all_dates[selectedReconstructionDate];
    const data = dateData.data;

    tbody.innerHTML = '';

    data.forEach((item, idx) => {
        const row = tbody.insertRow();
        const errorClass = Math.abs(item.error) > 0.05 ? 'text-danger fw-bold' : '';

        row.innerHTML = `
            <td>${idx + 1}</td>
            <td>${item.maturity.toFixed(3)}</td>
            <td title="${item.bond_name}"><code>${item.bond_code}</code></td>
            <td>${item.original_yield.toFixed(3)}</td>
            <td>${item.reconstructed_yield.toFixed(3)}</td>
            <td class="${errorClass}">${(item.error * 100).toFixed(3)}</td>
        `;
    });
}

// 誤差統計テーブルを更新
function updateErrorStatsTable() {
    const container = document.getElementById('errorStatsTable');
    const dateData = pcaData.reconstruction.all_dates[selectedReconstructionDate];
    const stats = dateData.statistics;

    container.innerHTML = `
        最大誤差: ${(stats.max_error * 100).toFixed(3)} bp |
        標準偏差: ${(stats.std * 100).toFixed(3)} bp |
        最小誤差: ${(stats.min * 100).toFixed(3)} bp
    `;
}

// ==================== localStorage 状態管理 ====================

/**
 * 現在の分析結果をlocalStorageに保存（最新1回分のみ、容量削減版）
 */
function saveState() {
    if (!pcaData) return;

    const latestDate = pcaData.reconstruction.latest_date;

    // 容量削減: 最新日のデータのみ保存
    const savedData = {
        // PCAモデル情報
        pca_model: {
            components: pcaData.pca_model.components,
            explained_variance_ratio: pcaData.pca_model.explained_variance_ratio,
            cumulative_variance_ratio: pcaData.pca_model.cumulative_variance_ratio
        },
        // 主成分スコア（直近30日）
        principal_component_scores: pcaData.principal_component_scores,
        // 共通グリッド
        common_grid: pcaData.common_grid,
        // 復元結果（最新日のみ保存 - 容量削減）
        reconstruction: {
            latest_date: latestDate,
            dates: [latestDate],  // 最新日のみ
            all_dates: {
                [latestDate]: pcaData.reconstruction.all_dates[latestDate]
            }
        },
        // パラメータ
        parameters: pcaData.parameters,
        // タイムスタンプ
        timestamp: new Date().toISOString()
    };

    try {
        const jsonString = JSON.stringify(savedData);
        const sizeKB = (jsonString.length / 1024).toFixed(2);

        localStorage.setItem('pcaAnalysisResult', jsonString);
        console.log(`✅ PCA分析結果を保存しました (${sizeKB} KB):`, new Date(savedData.timestamp).toLocaleString('ja-JP'));
        console.log(`   - パラメータ: ${savedData.parameters.lookback_days}日・${savedData.parameters.n_components}主成分`);
        console.log(`   - 最新日データ: ${savedData.reconstruction.latest_date}`);
    } catch (e) {
        console.error('❌ localStorage保存エラー:', e);
        if (e.name === 'QuotaExceededError') {
            showError('ブラウザのストレージ容量が不足しています。ブラウザのキャッシュをクリアしてください。');
        } else {
            showError('データの保存に失敗しました: ' + e.message);
        }
        // エラー時は古いデータを削除
        localStorage.removeItem('pcaAnalysisResult');
    }
}

/**
 * localStorageから保存された分析結果を復元
 */
async function loadSavedState() {
    try {
        console.log('loadSavedState: checking localStorage...');
        const saved = localStorage.getItem('pcaAnalysisResult');
        console.log('loadSavedState: saved data exists:', !!saved);
        if (!saved) {
            console.log('loadSavedState: No saved data found');
            return;
        }

        const state = JSON.parse(saved);

        // データの有効性チェック
        if (!state.pca_model || !state.timestamp) {
            console.warn('保存データが不正です');
            localStorage.removeItem('pcaAnalysisResult');
            return;
        }

        // パラメータを復元
        document.getElementById('lookbackDays').value = state.parameters.lookback_days;
        document.getElementById('nComponents').value = state.parameters.n_components;
        updateParameterDisplays();

        // データを復元
        pcaData = state;
        selectedReconstructionDate = state.reconstruction.latest_date;
        selectedErrorsDate = state.reconstruction.latest_date;

        // デバッグ: 復元されたデータを確認
        const sizeKB = (saved.length / 1024).toFixed(2);
        console.log(`PCA分析結果を復元しました (${sizeKB} KB):`, new Date(state.timestamp).toLocaleString('ja-JP'));
        console.log('復元された日付数:', state.reconstruction.dates.length);
        console.log('復元されたall_dates数:', Object.keys(state.reconstruction.all_dates).length);
        console.log('pcaData.reconstruction.dates:', pcaData.reconstruction.dates.length);
        console.log('pcaData.reconstruction.all_dates:', Object.keys(pcaData.reconstruction.all_dates).length);

        // 結果を表示
        displayResults();

        // 復元メッセージを表示
        showRestoredMessage(state.timestamp, state.parameters);

    } catch (e) {
        console.error('localStorage読み込みエラー:', e);
        // エラー時は保存データを削除
        localStorage.removeItem('pcaAnalysisResult');
    }
}

/**
 * 分析結果復元メッセージを表示
 */
function showRestoredMessage(timestamp, parameters) {
    const savedDate = new Date(timestamp);
    const now = new Date();
    const minutesAgo = Math.floor((now - savedDate) / (1000 * 60));

    let timeText;
    if (minutesAgo < 1) {
        timeText = 'たった今';
    } else if (minutesAgo < 60) {
        timeText = `${minutesAgo}分前`;
    } else if (minutesAgo < 1440) {
        const hoursAgo = Math.floor(minutesAgo / 60);
        timeText = `${hoursAgo}時間前`;
    } else {
        const daysAgo = Math.floor(minutesAgo / 1440);
        timeText = `${daysAgo}日前`;
    }

    // パラメータ情報を含むメッセージ
    const message = `前回の分析結果を表示しています（${parameters.lookback_days}日・${parameters.n_components}主成分、${timeText}に実行）`;

    // メッセージを表示するための要素を作成
    const alertDiv = document.createElement('div');
    alertDiv.className = 'alert alert-info alert-dismissible fade show';
    alertDiv.setAttribute('role', 'alert');
    alertDiv.innerHTML = `
        <i class="fas fa-info-circle me-2"></i>${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;

    // サマリー情報の前に挿入
    const summaryInfo = document.getElementById('summaryInfo');
    summaryInfo.parentNode.insertBefore(alertDiv, summaryInfo);

    // 10秒後に自動で非表示
    setTimeout(() => {
        alertDiv.remove();
    }, 10000);
}

/**
 * 保存された分析結果をクリア
 */
function clearSavedState() {
    localStorage.removeItem('pcaAnalysisResult');
    console.log('保存されたPCA分析結果をクリアしました');

    // ページをリロード
    location.reload();
}
