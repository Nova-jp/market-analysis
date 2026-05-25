'use client';

import { useState, useEffect, useMemo } from 'react';
import Link from 'next/link';
import { Loader2, ArrowLeft, TrendingUp, Plus, X } from 'lucide-react';
import { fetcher } from '@/lib/api';
import type { IMMForwardHistoryResponse, IMMForwardSnapshot } from '@/lib/api';
import IMMForwardHeatmap from '@/components/IMMForwardHeatmap';
import type { ViewMode } from '@/components/IMMForwardHeatmap';
import IMMForwardCurve from '@/components/IMMForwardCurve';
import IMMForwardTimeseries from '@/components/IMMForwardTimeseries';
import type { SeriesConfig } from '@/components/IMMForwardTimeseries';

// ── 定数 ────────────────────────────────────────────────────────

const Z_WINDOWS = [20, 50, 100, 200] as const;
const TENOR_OPTIONS = [
  { months: 3,  label: '3M' },
  { months: 6,  label: '6M' },
  { months: 12, label: '1Y' },
  { months: 24, label: '2Y' },
  { months: 60, label: '5Y' },
];
const SERIES_COLORS = ['#2563eb', '#dc2626', '#059669', '#d97706', '#7c3aed', '#db2777'];
const SERIES_TYPES: { value: SeriesConfig['type']; label: string }[] = [
  { value: 'rate',   label: 'レート'    },
  { value: 'fly1y',  label: '1Yフライ'  },
  { value: 'fly2y',  label: '2Yフライ'  },
  { value: 'spread', label: 'スプレッド' },
];

type Tab = 'matrix' | 'curve' | 'timeseries' | 'fly1y' | 'fly2y';

// ── ユーティリティ ───────────────────────────────────────────────

function flatIdx(i: number, j: number, n: number) {
  return (n - 1) * i - Math.floor(i * (i - 1) / 2) + (j - i - 1);
}

function computeButterfly(snap: IMMForwardSnapshot, wings: number): (number | null)[] {
  const { codes, rates } = snap;
  const n = codes.length;
  const result: (number | null)[] = [];
  let k = 0;
  for (let i = 0; i < n; i++) {
    for (let j = i + 1; j < n; j++) {
      const nearJ = j - wings;
      const farJ  = j + wings;
      if (nearJ > i && farJ < n) {
        const b  = rates[k];
        const nr = rates[flatIdx(i, nearJ, n)];
        const fr = rates[flatIdx(i, farJ,  n)];
        result.push(b !== null && nr !== null && fr !== null ? (2 * b - nr - fr) * 100 : null);
      } else {
        result.push(null);
      }
      k++;
    }
  }
  return result;
}

function computeButterflyZScore(
  snap: IMMForwardSnapshot,
  wSnaps: IMMForwardSnapshot[],
  wMaps: Map<string, number>[],
  wings: number,
): (number | null)[] {
  const curr = computeButterfly(snap, wings);
  const { codes } = snap;
  const n = codes.length;
  const result: (number | null)[] = [];
  let k = 0;
  for (let i = 0; i < n; i++) {
    for (let j = i + 1; j < n; j++) {
      const cv = curr[k++];
      if (cv === null || wSnaps.length < 5) { result.push(null); continue; }
      const hist: number[] = [];
      for (let w = 0; w < wSnaps.length; w++) {
        const si = wMaps[w].get(codes[i]);
        const sj = wMaps[w].get(codes[j]);
        if (si === undefined || sj === undefined) continue;
        const nearJ = sj - wings; const farJ = sj + wings;
        const sn = wSnaps[w].codes.length;
        if (nearJ <= si || farJ >= sn) continue;
        const b  = wSnaps[w].rates[flatIdx(si, sj, sn)];
        const nr = wSnaps[w].rates[flatIdx(si, nearJ, sn)];
        const fr = wSnaps[w].rates[flatIdx(si, farJ, sn)];
        if (b !== null && nr !== null && fr !== null) hist.push((2 * b - nr - fr) * 100);
      }
      if (hist.length < 5) { result.push(null); continue; }
      const m   = hist.reduce((a, b) => a + b, 0) / hist.length;
      const std = Math.sqrt(hist.reduce((a, b) => a + (b - m) ** 2, 0) / hist.length);
      result.push(std < 1e-8 ? 0 : (cv - m) / std);
    }
  }
  return result;
}

// ── コンポーネント ────────────────────────────────────────────────

export default function IMMForwardMatrixPage() {
  const [historyData, setHistoryData] = useState<IMMForwardHistoryResponse | null>(null);
  const [loading,     setLoading]     = useState(true);
  const [error,       setError]       = useState<string | null>(null);

  // マトリックス共通
  const [dateIdx,     setDateIdx]     = useState(0);
  const [zWindow,     setZWindow]     = useState(50);
  const [viewMode,    setViewMode]    = useState<ViewMode>('zscore');
  const [activeTab,   setActiveTab]   = useState<Tab>('matrix');
  const [flyViewMode, setFlyViewMode] = useState<'bps' | 'zscore'>('zscore');

  // フォワードカーブ
  const [tenorMonths, setTenorMonths] = useState(3);

  // 時系列
  const [seriesList, setSeriesList] = useState<SeriesConfig[]>([]);
  const [tsZScore,   setTsZScore]   = useState(false);
  const [tsBands,    setTsBands]    = useState(true);
  // 追加フォーム
  const [addType,   setAddType]   = useState<SeriesConfig['type']>('rate');
  const [addStart,  setAddStart]  = useState('');
  const [addEnd,    setAddEnd]    = useState('');
  const [addStart2, setAddStart2] = useState('');
  const [addEnd2,   setAddEnd2]   = useState('');

  // ── 初期ロード ───────────────────────────────────────────────
  useEffect(() => {
    fetcher('/api/imm-forward-matrix/history')
      .then((data: IMMForwardHistoryResponse) => {
        setHistoryData(data);
        const last  = data.trade_dates.length - 1;
        setDateIdx(last);
        const codes = data.snapshots[last]?.codes ?? [];
        if (codes.length >= 5) {
          setAddStart(codes[0]);
          setAddEnd(codes[4]);
          setAddStart2(codes[0]);
          setAddEnd2(codes[8] ?? codes[4]);
          // デフォルト1系列
          setSeriesList([{
            id: 'init', type: 'rate',
            startCode: codes[0], endCode: codes[4],
            color: SERIES_COLORS[0],
            label: `${codes[0]}→${codes[4]}`,
          }]);
        }
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  // ── 共有ウィンドウスナップ ────────────────────────────────────
  const { windowSnaps, windowMaps } = useMemo(() => {
    if (!historyData || dateIdx < 0) return { windowSnaps: [], windowMaps: [] };
    const start = Math.max(0, dateIdx - zWindow);
    const snaps = historyData.snapshots.slice(start, dateIdx);
    return { windowSnaps: snaps, windowMaps: snaps.map(s => new Map(s.codes.map((c, i) => [c, i]))) };
  }, [historyData, dateIdx, zWindow]);

  // ── メインマトリックス表示値 ─────────────────────────────────
  const displayValues = useMemo(() => {
    if (!historyData || dateIdx < 0) return [];
    const snap  = historyData.snapshots[dateIdx];
    const { codes, rates } = snap;
    const n = codes.length;

    if (viewMode === 'rate') return rates;

    if (viewMode === 'delta') {
      if (dateIdx === 0) return rates.map(() => null as number | null);
      const prev = historyData.snapshots[dateIdx - 1];
      const pMap = new Map(prev.codes.map((c, i) => [c, i]));
      const pn   = prev.codes.length;
      const res: (number | null)[] = [];
      let k = 0;
      for (let i = 0; i < n; i++) {
        for (let j = i + 1; j < n; j++) {
          const curr = rates[k++];
          if (curr === null) { res.push(null); continue; }
          const pi = pMap.get(codes[i]); const pj = pMap.get(codes[j]);
          if (pi === undefined || pj === undefined || pj <= pi) { res.push(null); continue; }
          const pr = prev.rates[flatIdx(pi, pj, pn)];
          res.push(pr !== null ? (curr - pr) * 100 : null);
        }
      }
      return res;
    }

    if (windowSnaps.length < 5) return rates.map(() => null as number | null);

    // Z-score
    const zs: (number | null)[] = [];
    let k = 0;
    for (let i = 0; i < n; i++) {
      for (let j = i + 1; j < n; j++) {
        const curr = rates[k++];
        if (curr === null) { zs.push(null); continue; }
        const hist: number[] = [];
        for (let w = 0; w < windowSnaps.length; w++) {
          const si = windowMaps[w].get(codes[i]); const sj = windowMaps[w].get(codes[j]);
          if (si === undefined || sj === undefined || sj <= si) continue;
          const sn = windowSnaps[w].codes.length;
          const r  = windowSnaps[w].rates[flatIdx(si, sj, sn)];
          if (r !== null) hist.push(r);
        }
        if (hist.length < 5) { zs.push(null); continue; }
        const m   = hist.reduce((a, b) => a + b, 0) / hist.length;
        const std = Math.sqrt(hist.reduce((a, b) => a + (b - m) ** 2, 0) / hist.length);
        zs.push(std < 1e-10 ? 0 : (curr - m) / std);
      }
    }

    if (viewMode === 'zscore') return zs;

    // 相対Z: テナー内対角平均を引く
    const dSum = new Map<number, number>(); const dCnt = new Map<number, number>();
    let ki = 0;
    for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) {
      const z = zs[ki++]; if (z === null) continue;
      const t = j - i;
      dSum.set(t, (dSum.get(t) ?? 0) + z); dCnt.set(t, (dCnt.get(t) ?? 0) + 1);
    }
    const rel: (number | null)[] = [];
    ki = 0;
    for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) {
      const z = zs[ki++]; if (z === null) { rel.push(null); continue; }
      const t = j - i;
      rel.push(z - (dSum.get(t) ?? 0) / (dCnt.get(t) ?? 1));
    }
    return rel;
  }, [historyData, dateIdx, viewMode, windowSnaps, windowMaps]);

  // ── バタフライ表示値 ────────────────────────────────────────
  const fly1YValues = useMemo(() => {
    if (!historyData || dateIdx < 0) return [];
    const snap = historyData.snapshots[dateIdx];
    return flyViewMode === 'bps'
      ? computeButterfly(snap, 4)
      : computeButterflyZScore(snap, windowSnaps, windowMaps, 4);
  }, [historyData, dateIdx, flyViewMode, windowSnaps, windowMaps]);

  const fly2YValues = useMemo(() => {
    if (!historyData || dateIdx < 0) return [];
    const snap = historyData.snapshots[dateIdx];
    return flyViewMode === 'bps'
      ? computeButterfly(snap, 8)
      : computeButterflyZScore(snap, windowSnaps, windowMaps, 8);
  }, [historyData, dateIdx, flyViewMode, windowSnaps, windowMaps]);

  // ── 追加フォーム用コードリスト ────────────────────────────────
  const latestCodes = useMemo(
    () => historyData?.snapshots[historyData.snapshots.length - 1]?.codes ?? [],
    [historyData],
  );

  const addEndCodes = useMemo(() => {
    const si = latestCodes.indexOf(addStart);
    if (si < 0) return [];
    if (addType === 'fly1y') return latestCodes.filter((_, j) => j > si + 4 && j < latestCodes.length - 4);
    if (addType === 'fly2y') return latestCodes.filter((_, j) => j > si + 8 && j < latestCodes.length - 8);
    return latestCodes.slice(si + 1);
  }, [latestCodes, addStart, addType]);

  const addEnd2Codes = useMemo(() => {
    const si = latestCodes.indexOf(addStart2);
    return si < 0 ? [] : latestCodes.slice(si + 1);
  }, [latestCodes, addStart2]);

  const handleAddSeries = () => {
    if (!addStart || !addEnd) return;
    if (addType === 'spread' && (!addStart2 || !addEnd2)) return;
    const id    = `s_${Date.now()}`;
    const color = SERIES_COLORS[seriesList.length % SERIES_COLORS.length];
    const typeLabel = SERIES_TYPES.find(t => t.value === addType)?.label ?? addType;
    const label =
      addType === 'spread'
        ? `${addStart}→${addEnd} - ${addStart2}→${addEnd2}`
        : `[${typeLabel}] ${addStart}→${addEnd}`;
    setSeriesList(prev => [...prev, {
      id, type: addType, startCode: addStart, endCode: addEnd,
      startCode2: addStart2, endCode2: addEnd2, color, label,
    }]);
  };

  // ── ローディング / エラー ────────────────────────────────────
  if (loading) {
    return (
      <main className="min-h-screen bg-slate-50 flex items-center justify-center">
        <div className="flex flex-col items-center gap-3 text-slate-600">
          <Loader2 className="w-8 h-8 animate-spin text-indigo-500" />
          <span className="text-sm">200日分のIMM Forwardデータを読み込み中...</span>
        </div>
      </main>
    );
  }
  if (error || !historyData) {
    return (
      <main className="min-h-screen bg-slate-50 flex items-center justify-center">
        <div className="text-center space-y-3">
          <p className="text-red-500">{error ?? 'データが見つかりません'}</p>
          <Link href="/" className="text-blue-600 hover:underline text-sm">← ホームに戻る</Link>
        </div>
      </main>
    );
  }

  const { trade_dates, snapshots } = historyData;
  const N           = trade_dates.length;
  const currentSnap = snapshots[dateIdx];
  const currentDate = trade_dates[dateIdx];
  const isFlyTab    = activeTab === 'fly1y' || activeTab === 'fly2y';

  const TABS: { id: Tab; label: string }[] = [
    { id: 'matrix',     label: 'マトリックス'     },
    { id: 'fly1y',      label: '1Yフライ'         },
    { id: 'fly2y',      label: '2Yフライ'         },
    { id: 'curve',      label: 'フォワードカーブ'  },
    { id: 'timeseries', label: '時系列'           },
  ];

  return (
    <main className="min-h-screen bg-slate-50">
      {/* ヘッダー */}
      <div className="bg-white border-b border-slate-200 px-6 py-4 flex items-center gap-4">
        <Link href="/" className="text-slate-400 hover:text-slate-700 transition-colors">
          <ArrowLeft className="w-5 h-5" />
        </Link>
        <div className="flex items-center gap-2">
          <TrendingUp className="w-5 h-5 text-indigo-600" />
          <h1 className="text-lg font-bold text-slate-800">IMM Forward Matrix</h1>
        </div>
        <span className="ml-auto text-xs text-slate-400">{N} 営業日</span>
      </div>

      <div className="container mx-auto px-4 py-5 space-y-4 max-w-7xl">
        {/* コントロールパネル */}
        <div className="bg-white rounded-xl border border-slate-200 p-4 space-y-4">
          {/* 日付スライダー */}
          <div className="space-y-1.5">
            <div className="flex justify-between items-center">
              <span className="text-xs text-slate-400">{trade_dates[0]}</span>
              <span className="text-sm font-semibold text-indigo-600">{currentDate}</span>
              <span className="text-xs text-slate-400">{trade_dates[N - 1]}</span>
            </div>
            <input type="range" min={0} max={N - 1} value={dateIdx}
              onChange={e => setDateIdx(Number(e.target.value))}
              className="w-full accent-indigo-600 cursor-pointer" />
          </div>

          <div className="flex flex-wrap items-center gap-4">
            {/* Z-Score ウィンドウ */}
            <div className="flex items-center gap-2">
              <span className="text-xs text-slate-500 font-medium whitespace-nowrap">Z-Scoreウィンドウ:</span>
              <div className="flex gap-1">
                {Z_WINDOWS.map(w => (
                  <button key={w} onClick={() => setZWindow(w)}
                    className={`px-2.5 py-1 text-xs rounded-md font-medium transition-colors ${
                      zWindow === w ? 'bg-indigo-600 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                    }`}>{w}日</button>
                ))}
              </div>
            </div>

            {/* マトリックス表示モード */}
            {activeTab === 'matrix' && (
              <div className="flex items-center gap-2 ml-auto">
                <span className="text-xs text-slate-500 font-medium">表示:</span>
                <div className="flex gap-1">
                  {([['rate','レート'],['zscore','Z-Score'],['relative','相対Z'],['delta','Δ前日差']] as [ViewMode, string][]).map(
                    ([m, l]) => (
                      <button key={m} onClick={() => setViewMode(m)}
                        className={`px-2.5 py-1 text-xs rounded-md font-medium transition-colors ${
                          viewMode === m ? 'bg-indigo-600 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                        }`}>{l}</button>
                    ))}
                </div>
              </div>
            )}

            {/* フライ表示モード */}
            {isFlyTab && (
              <div className="flex items-center gap-2 ml-auto">
                <span className="text-xs text-slate-500 font-medium">表示:</span>
                <div className="flex gap-1">
                  {([['bps','bps'],['zscore','Z-Score']] as ['bps'|'zscore', string][]).map(
                    ([m, l]) => (
                      <button key={m} onClick={() => setFlyViewMode(m)}
                        className={`px-2.5 py-1 text-xs rounded-md font-medium transition-colors ${
                          flyViewMode === m ? 'bg-indigo-600 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                        }`}>{l}</button>
                    ))}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* タブ */}
        <div className="flex gap-1 bg-white rounded-xl border border-slate-200 p-1 w-fit flex-wrap">
          {TABS.map(({ id, label }) => (
            <button key={id} onClick={() => setActiveTab(id)}
              className={`px-4 py-1.5 text-sm rounded-lg font-medium transition-colors ${
                activeTab === id ? 'bg-indigo-600 text-white shadow-sm' : 'text-slate-600 hover:bg-slate-100'
              }`}>{label}</button>
          ))}
        </div>

        {/* タブコンテンツ */}
        <div className="bg-white rounded-xl border border-slate-200 p-4">

          {/* ── マトリックス ── */}
          {activeTab === 'matrix' && (
            <IMMForwardHeatmap snapshot={currentSnap} displayValues={displayValues}
              viewMode={viewMode} tradeDate={currentDate} />
          )}

          {/* ── 1Y / 2Y フライ ── */}
          {activeTab === 'fly1y' && (
            <IMMForwardHeatmap snapshot={currentSnap} displayValues={fly1YValues}
              viewMode={flyViewMode === 'zscore' ? 'zscore' : 'rate'} tradeDate={currentDate}
              chartTitle={`1Yフライ (固定スタート) — ${currentDate}`}
              colorbarLabel={flyViewMode === 'bps' ? '1Yフライ (bps)' : undefined} />
          )}
          {activeTab === 'fly2y' && (
            <IMMForwardHeatmap snapshot={currentSnap} displayValues={fly2YValues}
              viewMode={flyViewMode === 'zscore' ? 'zscore' : 'rate'} tradeDate={currentDate}
              chartTitle={`2Yフライ (固定スタート) — ${currentDate}`}
              colorbarLabel={flyViewMode === 'bps' ? '2Yフライ (bps)' : undefined} />
          )}

          {/* ── フォワードカーブ ── */}
          {activeTab === 'curve' && (
            <div className="space-y-4">
              <div className="flex items-center gap-2">
                <span className="text-sm text-slate-600 font-medium">テナー:</span>
                <div className="flex gap-1">
                  {TENOR_OPTIONS.map(opt => (
                    <button key={opt.months} onClick={() => setTenorMonths(opt.months)}
                      className={`px-3 py-1 text-xs rounded-md font-medium transition-colors ${
                        tenorMonths === opt.months ? 'bg-indigo-600 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                      }`}>{opt.label}</button>
                  ))}
                </div>
                <span className="ml-4 text-xs text-slate-400">青={currentDate}　赤=-20日　緑=-50日</span>
              </div>
              <IMMForwardCurve snapshots={snapshots} tradeDates={trade_dates}
                dateIdx={dateIdx} tenorMonths={tenorMonths} />
            </div>
          )}

          {/* ── 時系列 ── */}
          {activeTab === 'timeseries' && (
            <div className="space-y-4">

              {/* 系列追加フォーム */}
              <div className="bg-slate-50 rounded-lg border border-slate-200 p-3 space-y-3">
                <p className="text-xs font-semibold text-slate-600">系列を追加</p>
                <div className="flex flex-wrap items-center gap-2">
                  {/* タイプ */}
                  <select value={addType}
                    onChange={e => { setAddType(e.target.value as SeriesConfig['type']); setAddEnd(''); }}
                    className="border border-slate-200 rounded px-2 py-1 text-xs text-slate-700">
                    {SERIES_TYPES.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
                  </select>

                  {/* ペア1 */}
                  <select value={addStart}
                    onChange={e => { setAddStart(e.target.value); setAddEnd(''); }}
                    className="border border-slate-200 rounded px-2 py-1 text-xs text-slate-700">
                    {latestCodes.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                  <span className="text-xs text-slate-400">→</span>
                  <select value={addEnd} onChange={e => setAddEnd(e.target.value)}
                    className="border border-slate-200 rounded px-2 py-1 text-xs text-slate-700">
                    <option value="">エンド選択</option>
                    {addEndCodes.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>

                  {/* ペア2 (スプレッドのみ) */}
                  {addType === 'spread' && <>
                    <span className="text-xs text-slate-400 font-bold">−</span>
                    <select value={addStart2} onChange={e => { setAddStart2(e.target.value); setAddEnd2(''); }}
                      className="border border-slate-200 rounded px-2 py-1 text-xs text-slate-700">
                      {latestCodes.map(c => <option key={c} value={c}>{c}</option>)}
                    </select>
                    <span className="text-xs text-slate-400">→</span>
                    <select value={addEnd2} onChange={e => setAddEnd2(e.target.value)}
                      className="border border-slate-200 rounded px-2 py-1 text-xs text-slate-700">
                      <option value="">エンド選択</option>
                      {addEnd2Codes.map(c => <option key={c} value={c}>{c}</option>)}
                    </select>
                  </>}

                  <button onClick={handleAddSeries}
                    disabled={!addEnd || (addType === 'spread' && !addEnd2)}
                    className="flex items-center gap-1 px-3 py-1 text-xs bg-indigo-600 text-white rounded-md hover:bg-indigo-700 disabled:opacity-40 disabled:cursor-not-allowed font-medium">
                    <Plus className="w-3 h-3" /> 追加
                  </button>
                </div>

                {/* 追加済み系列リスト */}
                {seriesList.length > 0 && (
                  <div className="flex flex-wrap gap-2 pt-1">
                    {seriesList.map(s => (
                      <div key={s.id} className="flex items-center gap-1.5 bg-white border border-slate-200 rounded-full px-3 py-1 text-xs">
                        <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: s.color }} />
                        <span className="text-slate-700">{s.label}</span>
                        <button onClick={() => setSeriesList(prev => prev.filter(x => x.id !== s.id))}
                          className="text-slate-400 hover:text-red-500 ml-0.5">
                          <X className="w-3 h-3" />
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* オプション */}
              <div className="flex items-center gap-4">
                <label className="flex items-center gap-2 text-xs text-slate-600 cursor-pointer select-none">
                  <input type="checkbox" checked={tsZScore} onChange={e => setTsZScore(e.target.checked)}
                    className="accent-indigo-600" />
                  Z-Score表示
                </label>
                {!tsZScore && seriesList.length === 1 && (
                  <label className="flex items-center gap-2 text-xs text-slate-600 cursor-pointer select-none">
                    <input type="checkbox" checked={tsBands} onChange={e => setTsBands(e.target.checked)}
                      className="accent-indigo-600" />
                    σバンド表示
                  </label>
                )}
                <span className="text-xs text-slate-400 ml-auto">Z-Windowウィンドウ: {zWindow}日</span>
              </div>

              {seriesList.length === 0 ? (
                <p className="text-slate-500 text-center py-16 text-sm">上のフォームから系列を追加してください。</p>
              ) : (
                <IMMForwardTimeseries
                  snapshots={snapshots}
                  tradeDates={trade_dates}
                  series={seriesList}
                  zWindow={zWindow}
                  showZScore={tsZScore}
                  showBands={tsBands}
                />
              )}
            </div>
          )}
        </div>
      </div>
    </main>
  );
}
