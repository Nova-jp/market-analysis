/**
 * PCA Analysis JavaScript
 * 主成分分析のフロントエンドロジック
 * Last updated: 2026-01-04
 */

// グローバル変数
let pcaData = null;
let swapPcaData = null; // Swap用データ
let charts = {
    components: null,
    scores: null,
    errors: null,
    swapComponents: null,
    swapScores: null,
    swapErrors: null
};
let selectedReconstructionDate = null;
let selectedErrorsDate = null;
let selectedSwapReconstructionDate = null; 
let selectedSwapErrorsDate = null; // Swap用
let maturityFilter = { min: null, max: null };

// 主成分のラベルと色
const PC_LABELS = ['Level', 'Slope', 'Curvature', 'PC4', 'PC5'];
const PC_COLORS = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6'];

// 初期化
document.addEventListener('DOMContentLoaded', function() {
    console.log('PCA.js loaded - version 2026-01-04');
    
    initializeEventListeners();
    updateParameterDisplays();
    updateSwapParameterDisplays(); // Swap用初期化

    // 保存された結果を復元
    loadSavedState('jgb');
    loadSavedState('swap');
});

// イベントリスナー設定
function initializeEventListeners() {
    console.log('Initializing Event Listeners');
    // JGB Params
    const lookbackDays = document.getElementById('lookbackDays');
    if (lookbackDays) lookbackDays.addEventListener('input', updateParameterDisplays);
    
    const nComponents = document.getElementById('nComponents');
    if (nComponents) nComponents.addEventListener('input', updateParameterDisplays);
    
    const analyzeBtn = document.getElementById('analyzeBtn');
    if (analyzeBtn) analyzeBtn.addEventListener('click', runPCAAnalysis);
    
    const reconDateSelect = document.getElementById('reconstructionDateSelect');
    if (reconDateSelect) reconDateSelect.addEventListener('change', onReconstructionDateChange);
    
    const errDateSelect = document.getElementById('errorsDateSelect');
    if (errDateSelect) errDateSelect.addEventListener('change', onErrorsDateChange);
    
    const applyFilt = document.getElementById('applyFilter');
    if (applyFilt) applyFilt.addEventListener('click', applyMaturityFilter);
    
    const clearFilt = document.getElementById('clearFilter');
    if (clearFilt) clearFilt.addEventListener('click', clearMaturityFilter);

    // Swap Params
    const swapLookbackDays = document.getElementById('swapLookbackDays');
    if (swapLookbackDays) swapLookbackDays.addEventListener('input', updateSwapParameterDisplays);
    
    const swapNComponents = document.getElementById('swapNComponents');
    if (swapNComponents) swapNComponents.addEventListener('input', updateSwapParameterDisplays);
    
    const analyzeSwapBtn = document.getElementById('analyzeSwapBtn');
    if (analyzeSwapBtn) {
        console.log('Found analyzeSwapBtn, attaching listener');
        analyzeSwapBtn.addEventListener('click', runSwapPCAAnalysis);
    } else {
        console.warn('analyzeSwapBtn not found');
    }
    
    const swapReconDateSelect = document.getElementById('swapReconstructionDateSelect');
    if (swapReconDateSelect) swapReconDateSelect.addEventListener('change', onSwapReconstructionDateChange);

    const swapErrDateSelect = document.getElementById('swapErrorsDateSelect');
    if (swapErrDateSelect) swapErrDateSelect.addEventListener('change', onSwapErrorsDateChange);
}

// パラメータ表示を更新 (JGB)
function updateParameterDisplays() {
    const daysInput = document.getElementById('lookbackDays');
    const compInput = document.getElementById('nComponents');
    if (!daysInput || !compInput) return;
    
    const days = daysInput.value;
    const components = compInput.value;
    document.getElementById('lookbackDaysValue').textContent = `${days}日`;
    document.getElementById('nComponentsValue').textContent = components;
}

// パラメータ表示を更新 (Swap)
function updateSwapParameterDisplays() {
    const daysInput = document.getElementById('swapLookbackDays');
    const compInput = document.getElementById('swapNComponents');
    if (!daysInput || !compInput) return;
    
    const days = daysInput.value;
    const components = compInput.value;
    document.getElementById('swapLookbackDaysValue').textContent = `${days}日`;
    document.getElementById('swapNComponentsValue').textContent = components;
}

// エラー表示
function showError(message) {
    const errorAlert = document.getElementById('errorAlert');
    const errorMessage = document.getElementById('errorMessage');
    errorMessage.textContent = message;
    errorAlert.style.display = 'block';
    setTimeout(() => { errorAlert.style.display = 'none'; }, 5000);
}

// ローディング表示制御
function showLoading(show) {
    const overlay = document.getElementById('loadingOverlay');
    if (show) overlay.classList.add('active');
    else overlay.classList.remove('active');
}

// ==================== JGB PCA Logic ====================

async function runPCAAnalysis() {
    const days = document.getElementById('lookbackDays').value;
    const components = document.getElementById('nComponents').value;
    showLoading(true);
    try {
        const response = await fetch(`/api/pca/analyze?days=${days}&components=${components}`);
        if (!response.ok) throw new Error(`APIエラー: ${response.status}`);
        pcaData = await response.json();
        displayResults();
        saveState('jgb');
    } catch (error) {
        console.error('PCA分析エラー:', error);
        showError(`分析中にエラーが発生しました: ${error.message}`);
    } finally {
        showLoading(false);
    }
}

function displayResults() {
    if (!pcaData) return;
    document.getElementById('resultsArea').style.display = 'block';
    selectedReconstructionDate = pcaData.reconstruction.latest_date;
    selectedErrorsDate = pcaData.reconstruction.latest_date;
    populateDateSelectors();
    
    // Summary
    document.getElementById('statVariance').textContent = `${(pcaData.pca_model.cumulative_variance_ratio * 100).toFixed(1)}%`;
    document.getElementById('statDays').textContent = pcaData.parameters.valid_dates_count;

    drawComponentsChart();
    drawScoresChart();
    drawErrorsChart();
    updateVarianceTable();
    updateReconstructionTable();
    updateErrorStatsTable();
}

function populateDateSelectors() {
    const dates = pcaData.reconstruction.dates;
    const rSelect = document.getElementById('reconstructionDateSelect');
    const eSelect = document.getElementById('errorsDateSelect');
    
    if (!rSelect || !eSelect) return;
    rSelect.innerHTML = ''; eSelect.innerHTML = '';
    
    dates.forEach(date => {
        const opt1 = new Option(date, date, date === selectedReconstructionDate, date === selectedReconstructionDate);
        rSelect.add(opt1);
        const opt2 = new Option(date, date, date === selectedErrorsDate, date === selectedErrorsDate);
        eSelect.add(opt2);
    });
}

function onReconstructionDateChange(e) {
    selectedReconstructionDate = e.target.value;
    updateReconstructionTable();
    updateErrorStatsTable();
}

function onErrorsDateChange(e) {
    selectedErrorsDate = e.target.value;
    drawErrorsChart();
}

function applyMaturityFilter() {
    const minVal = parseFloat(document.getElementById('minMaturity').value);
    const maxVal = parseFloat(document.getElementById('maxMaturity').value);
    maturityFilter.min = isNaN(minVal) ? null : minVal;
    maturityFilter.max = isNaN(maxVal) ? null : maxVal;
    drawErrorsChart();
}

function clearMaturityFilter() {
    maturityFilter.min = null; maturityFilter.max = null;
    document.getElementById('minMaturity').value = '';
    document.getElementById('maxMaturity').value = '';
    drawErrorsChart();
}

function drawComponentsChart() {
    const ctx = document.getElementById('componentsChart').getContext('2d');
    if (charts.components) charts.components.destroy();
    
    const commonGrid = pcaData.common_grid;
    const components = pcaData.pca_model.components;
    const datasets = components.map((comp, i) => ({
        label: `PC${i+1} (${PC_LABELS[i]})`,
        data: commonGrid.map((x, idx) => ({x: x, y: comp[idx]})),
        borderColor: PC_COLORS[i],
        backgroundColor: PC_COLORS[i] + '20',
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.4
    }));

    charts.components = new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { x: { type: 'linear', title: { display: true, text: '残存期間 (年)' } } }
        }
    });
}

function drawScoresChart() {
    const ctx = document.getElementById('scoresChart').getContext('2d');
    if (charts.scores) charts.scores.destroy();
    
    const dates = pcaData.principal_component_scores.dates;
    const scores = pcaData.principal_component_scores.scores;
    const datasets = scores[0].map((_, i) => ({
        label: `PC${i+1} (${PC_LABELS[i]})`,
        data: dates.map((d, idx) => ({x: d, y: scores[idx][i]})),
        borderColor: PC_COLORS[i],
        backgroundColor: PC_COLORS[i] + '40',
        borderWidth: 2,
        pointRadius: 3,
        tension: 0.1
    }));

    charts.scores = new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { x: { type: 'category', ticks: { maxRotation: 45, minRotation: 45 } } }
        }
    });
}

function drawErrorsChart() {
    const canvas = document.getElementById('errorsChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (charts.errors) charts.errors.destroy();

    const dateData = pcaData.reconstruction.all_dates[selectedErrorsDate];
    if (!dateData) return;
    let reconstructionData = dateData.data;

    // 年限フィルターを適用
    if (maturityFilter.min !== null || maturityFilter.max !== null) {
        reconstructionData = reconstructionData.filter(item => {
            const maturity = item.maturity;
            if (maturityFilter.min !== null && maturity < maturityFilter.min) return false;
            if (maturityFilter.max !== null && maturity > maturityFilter.max) return false;
            return true;
        });
    }

    // 銘柄コードの下4桁でグループ化し、年限ごとに統合
    const bondYearGroups = {};
    reconstructionData.forEach(item => {
        const bondCode = item.bond_code || '';
        const bondType = bondCode.slice(-4);

        let yearLabel = '';
        if (bondType === '0032' || bondType === '0042') yearLabel = '2年債';
        else if (bondType === '0045' || bondType === '0057') yearLabel = '5年債';
        else if (bondType === '0058' || bondType === '0067') yearLabel = '10年債';
        else if (bondType === '0069') yearLabel = '20年債';
        else if (bondType === '0068') yearLabel = '30年債';
        else if (bondType === '0054') yearLabel = '40年債';
        else yearLabel = 'その他';

        if (!bondYearGroups[yearLabel]) bondYearGroups[yearLabel] = [];
        bondYearGroups[yearLabel].push({
            x: item.maturity,
            y: item.error * 100, // bp
            bond_name: item.bond_name,
            bond_code: item.bond_code
        });
    });

    const bondYearColors = {
        '2年債': '#e74c3c', '5年債': '#3498db', '10年債': '#2ecc71',
        '20年債': '#f39c12', '30年債': '#9b59b6', '40年債': '#e67e22', 'その他': '#95a5a6'
    };

    const yearOrder = ['2年債', '5年債', '10年債', '20年債', '30年債', '40年債', 'その他'];
    const datasets = yearOrder
        .filter(year => bondYearGroups[year])
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
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const d = context.raw;
                            return [
                                `銘柄名: ${d.bond_name}`,
                                `銘柄コード: ${d.bond_code}`,
                                `残存期間: ${d.x.toFixed(3)}年`,
                                `誤差: ${d.y.toFixed(3)}bp`
                            ];
                        }
                    }
                }
            },
            scales: {
                x: { type: 'linear', title: { display: true, text: '残存期間 (年)' } },
                y: { title: { display: true, text: '誤差 (bp)' } }
            }
        }
    });
}

function updateVarianceTable() {
    const tbody = document.getElementById('varianceTable');
    if (!tbody) return;
    tbody.innerHTML = pcaData.pca_model.explained_variance_ratio.map((v, i) => `
        <tr><td><strong>PC${i+1}</strong></td><td>${PC_LABELS[i]}</td>
        <td><span class="badge" style="background-color: ${PC_COLORS[i]}">${(v * 100).toFixed(3)}%</span></td></tr>
    `).join('');
}

function updateReconstructionTable() {
    const tbody = document.getElementById('reconstructionTable');
    if (!tbody) return;
    const data = pcaData.reconstruction.all_dates[selectedReconstructionDate].data;
    tbody.innerHTML = data.map((d, i) => `
        <tr>
            <td>${i + 1}</td>
            <td>${d.maturity.toFixed(3)}</td>
            <td title="${d.bond_name}"><code>${d.bond_code}</code></td>
            <td>${d.original_yield.toFixed(3)}</td>
            <td>${d.reconstructed_yield.toFixed(3)}</td>
            <td class="${Math.abs(d.error) > 0.05 ? 'text-danger fw-bold' : ''}">${(d.error * 100).toFixed(3)}</td>
        </tr>
    `).join('');
}

function updateErrorStatsTable() {
    const statsElem = document.getElementById('errorStatsTable');
    if (!statsElem) return;
    const stats = pcaData.reconstruction.all_dates[selectedReconstructionDate].statistics;
    statsElem.textContent = 
        `最大誤差: ${(stats.max_error * 100).toFixed(3)} bp | 標準偏差: ${(stats.std * 100).toFixed(3)} bp`;
}


// ==================== Swap PCA Logic ====================

async function runSwapPCAAnalysis() {
    console.log('runSwapPCAAnalysis called');
    const days = document.getElementById('swapLookbackDays').value;
    const components = document.getElementById('swapNComponents').value;
    const product = document.getElementById('swapProductType').value;

    showLoading(true);
    try {
        const response = await fetch(`/api/pca/analyze_swap?days=${days}&components=${components}&product_type=${product}`);
        if (!response.ok) throw new Error(`APIエラー: ${response.status}`);
        
        swapPcaData = await response.json();
        console.log('Swap PCA Data received');
        displaySwapResults();
        saveState('swap');
    } catch (error) {
        console.error('Swap PCA分析エラー:', error);
        showError(`分析中にエラーが発生しました: ${error.message}`);
    } finally {
        showLoading(false);
    }
}

function displaySwapResults() {
    if (!swapPcaData) return;
    document.getElementById('swapResultsArea').style.display = 'block';
    
    selectedSwapReconstructionDate = swapPcaData.reconstruction.latest_date;
    selectedSwapErrorsDate = swapPcaData.reconstruction.latest_date;
    
    // Date Selector
    const dates = swapPcaData.reconstruction.dates;
    const select = document.getElementById('swapReconstructionDateSelect');
    if (select) {
        select.innerHTML = '';
        dates.forEach(date => {
            select.add(new Option(date, date, date === selectedSwapReconstructionDate, date === selectedSwapReconstructionDate));
        });
    }

    const errSelect = document.getElementById('swapErrorsDateSelect');
    if (errSelect) {
        errSelect.innerHTML = '';
        dates.forEach(date => {
            errSelect.add(new Option(date, date, date === selectedSwapErrorsDate, date === selectedSwapErrorsDate));
        });
    }

    // Summary
    document.getElementById('swapStatVariance').textContent = `${(swapPcaData.pca_model.cumulative_variance_ratio * 100).toFixed(1)}%`;
    document.getElementById('swapStatDays').textContent = swapPcaData.parameters.valid_dates_count;

    drawSwapComponentsChart();
    drawSwapScoresChart();
    drawSwapErrorsChart();
    updateSwapVarianceTable();
    updateSwapReconstructionTable();
}

function onSwapReconstructionDateChange(e) {
    selectedSwapReconstructionDate = e.target.value;
    updateSwapReconstructionTable();
}

function onSwapErrorsDateChange(e) {
    selectedSwapErrorsDate = e.target.value;
    drawSwapErrorsChart();
}

function drawSwapErrorsChart() {
    const canvas = document.getElementById('swapErrorsChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (charts.swapErrors) charts.swapErrors.destroy();

    const dateData = swapPcaData.reconstruction.all_dates[selectedSwapErrorsDate];
    if (!dateData) return;
    let reconstructionData = dateData.data;

    const datasets = [{
        label: 'Swap復元誤差',
        data: reconstructionData.map(d => ({ x: d.maturity, y: d.error * 100 })), // bp
        backgroundColor: '#2ecc7180',
        borderColor: '#2ecc71',
        pointRadius: 5,
        pointHoverRadius: 7
    }];

    charts.swapErrors = new Chart(ctx, {
        type: 'scatter',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const d = context.raw;
                            return [
                                `年限: ${d.x.toFixed(1)}Y`,
                                `誤差: ${d.y.toFixed(3)}bp`
                            ];
                        }
                    }
                }
            },
            scales: {
                x: { type: 'linear', title: { display: true, text: '年限 (年)' } },
                y: { title: { display: true, text: '誤差 (bp)' } }
            }
        }
    });
}

function drawSwapComponentsChart() {
    const ctx = document.getElementById('swapComponentsChart').getContext('2d');
    if (charts.swapComponents) charts.swapComponents.destroy();
    
    const commonGrid = swapPcaData.common_grid;
    const components = swapPcaData.pca_model.components;
    
    const datasets = components.map((comp, i) => ({
        label: `PC${i+1} (${PC_LABELS[i]})`,
        data: commonGrid.map((x, idx) => ({x: x, y: comp[idx]})),
        borderColor: PC_COLORS[i],
        backgroundColor: PC_COLORS[i] + '20',
        borderWidth: 2,
        tension: 0.4
    }));

    charts.swapComponents = new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { x: { type: 'linear', title: { display: true, text: '年限 (年)' } } }
        }
    });
}

function drawSwapScoresChart() {
    const ctx = document.getElementById('swapScoresChart').getContext('2d');
    if (charts.swapScores) charts.swapScores.destroy();
    
    const dates = swapPcaData.principal_component_scores.dates;
    const scores = swapPcaData.principal_component_scores.scores;
    
    const datasets = scores[0].map((_, i) => ({
        label: `PC${i+1} (${PC_LABELS[i]})`,
        data: dates.map((d, idx) => ({x: d, y: scores[idx][i]})),
        borderColor: PC_COLORS[i],
        backgroundColor: PC_COLORS[i] + '40',
        borderWidth: 2,
        tension: 0.1
    }));

    charts.swapScores = new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { x: { type: 'category', ticks: { maxRotation: 45, minRotation: 45 } } }
        }
    });
}

function updateSwapVarianceTable() {
    const tbody = document.getElementById('swapVarianceTable');
    if (!tbody) return;
    tbody.innerHTML = swapPcaData.pca_model.explained_variance_ratio.map((v, i) => `
        <tr><td><strong>PC${i+1}</strong></td><td>${PC_LABELS[i]}</td>
        <td><span class="badge" style="background-color: ${PC_COLORS[i]}">${(v * 100).toFixed(3)}%</span></td></tr>
    `).join('');
}

function updateSwapReconstructionTable() {
    const tbody = document.getElementById('swapReconstructionTable');
    if (!tbody) return;
    const data = swapPcaData.reconstruction.all_dates[selectedSwapReconstructionDate].data;
    const stats = swapPcaData.reconstruction.all_dates[selectedSwapReconstructionDate].statistics;
    
    tbody.innerHTML = data.map((d, i) => `
        <tr>
            <td>${i + 1}</td>
            <td>${d.maturity.toFixed(1)}Y</td>
            <td>${d.original_yield.toFixed(3)}</td>
            <td>${d.reconstructed_yield.toFixed(3)}</td>
            <td class="${Math.abs(d.error) > 0.05 ? 'text-danger fw-bold' : ''}">${(d.error * 100).toFixed(3)}</td>
        </tr>
    `).join('');

    const statsElem = document.getElementById('swapErrorStatsTable');
    if (statsElem) {
        statsElem.textContent = 
            `最大誤差: ${(stats.max_error * 100).toFixed(3)} bp | 標準偏差: ${(stats.std * 100).toFixed(3)} bp`;
    }
}

// ==================== State Management ====================

function saveState(type) {
    const key = type === 'swap' ? 'pcaAnalysisResult_swap' : 'pcaAnalysisResult';
    const data = type === 'swap' ? swapPcaData : pcaData;
    if (!data) return;

    // 軽量化して保存
    const savedData = {
        pca_model: data.pca_model,
        principal_component_scores: data.principal_component_scores,
        common_grid: data.common_grid,
        reconstruction: {
            latest_date: data.reconstruction.latest_date,
            dates: [data.reconstruction.latest_date], // Only keep latest for simple caching
            all_dates: { [data.reconstruction.latest_date]: data.reconstruction.all_dates[data.reconstruction.latest_date] }
        },
        parameters: data.parameters,
        timestamp: new Date().toISOString()
    };

    try {
        localStorage.setItem(key, JSON.stringify(savedData));
        console.log(`Saved ${type} PCA state.`);
    } catch (e) {
        console.error('Storage error:', e);
    }
}

function loadSavedState(type) {
    const key = type === 'swap' ? 'pcaAnalysisResult_swap' : 'pcaAnalysisResult';
    const saved = localStorage.getItem(key);
    if (!saved) return;

    try {
        const state = JSON.parse(saved);
        if (type === 'swap') {
            swapPcaData = state;
            const daysInput = document.getElementById('swapLookbackDays');
            if (daysInput) daysInput.value = state.parameters.lookback_days;
            const compInput = document.getElementById('swapNComponents');
            if (compInput) compInput.value = state.parameters.n_components;
            updateSwapParameterDisplays();
            displaySwapResults();
        } else {
            pcaData = state;
            const daysInput = document.getElementById('lookbackDays');
            if (daysInput) daysInput.value = state.parameters.lookback_days;
            const compInput = document.getElementById('nComponents');
            if (compInput) compInput.value = state.parameters.n_components;
            updateParameterDisplays();
            displayResults();
        }
        showRestoredMessage(state.timestamp, state.parameters, type);
    } catch (e) {
        console.error(`Error loading ${type} state:`, e);
        localStorage.removeItem(key);
    }
}

function clearSavedState(type) {
    const key = type === 'swap' ? 'pcaAnalysisResult_swap' : 'pcaAnalysisResult';
    localStorage.removeItem(key);
    location.reload();
}

function showRestoredMessage(timestamp, params, type) {
    const savedDate = new Date(timestamp);
    const timeText = savedDate.toLocaleString();
    const prefix = type === 'swap' ? 'Swap' : 'JGB';
    
    console.log(`${prefix} PCA data restored from ${timeText}`);
}
