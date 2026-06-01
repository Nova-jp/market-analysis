'use client';

import { useState, useEffect, useCallback } from 'react';
import Link from 'next/link';
import { fetcher, InstantaneousForwardResponse } from '@/lib/api';
import InstantaneousFwdChart, { FwdDataset } from '@/components/InstantaneousFwdChart';
import { Loader2, Plus, X, ArrowLeft } from 'lucide-react';

const PALETTE = [
  '#2563eb', '#dc2626', '#059669', '#d97706',
  '#7c3aed', '#db2777', '#0891b2', '#4f46e5',
  '#ea580c', '#1e293b',
];

interface QuickDates {
  latest?: string;
  previous?: string;
  five_days_ago?: string;
  month_ago?: string;
}

export default function InstantaneousForwardPage() {
  const [datasets, setDatasets] = useState<FwdDataset[]>([]);
  const [dateInput, setDateInput] = useState('');
  const [quickDates, setQuickDates] = useState<QuickDates | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [minMaturity, setMinMaturity] = useState(0);
  const [maxMaturity, setMaxMaturity] = useState(10);
  const [tempMin, setTempMin] = useState(0);
  const [tempMax, setTempMax] = useState(10);

  useEffect(() => {
    fetcher('/api/quick-dates')
      .then((qd: QuickDates) => {
        setQuickDates(qd);
        if (qd.latest) {
          setDateInput(qd.latest);
          addDate(qd.latest);
        }
      })
      .catch(() => setError('クイック日付の取得に失敗しました。'));
  }, []);

  const addDate = useCallback(
    async (date: string) => {
      if (!date) return;
      setLoading(true);
      setError(null);
      try {
        const resp: InstantaneousForwardResponse = await fetcher(
          `/api/instantaneous-forward/${date}`,
        );
        if (resp.error || !resp.data || resp.data.length === 0) {
          setError(resp.error ?? `${date} のデータが見つかりませんでした。`);
          return;
        }
        setDatasets((prev) => {
          if (prev.some((ds) => ds.date === date)) return prev;
          return [...prev, { date, data: resp.data, color: PALETTE[prev.length % PALETTE.length] }];
        });
      } catch (e: any) {
        setError(e.detail ?? 'データの取得に失敗しました。');
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  const removeDate = (date: string) => {
    setDatasets((prev) =>
      prev
        .filter((ds) => ds.date !== date)
        .map((ds, idx) => ({ ...ds, color: PALETTE[idx % PALETTE.length] })),
    );
  };

  const handleAdd = () => addDate(dateInput);

  const handleRangeChange = () => {
    setMinMaturity(tempMin);
    setMaxMaturity(tempMax);
  };

  const quickDateEntries: { label: string; value?: string }[] = [
    { label: '最新', value: quickDates?.latest },
    { label: '前日', value: quickDates?.previous },
    { label: '5営業日前', value: quickDates?.five_days_ago },
    { label: '1ヶ月前', value: quickDates?.month_ago },
  ];

  return (
    <main className="min-h-screen bg-slate-50">
      <div className="bg-white border-b border-slate-200 px-6 py-4 flex items-center gap-4">
        <Link href="/" className="text-slate-500 hover:text-slate-700">
          <ArrowLeft className="w-5 h-5" />
        </Link>
        <h1 className="text-xl font-bold text-slate-900">Instantaneous Forward Rate</h1>
        <span className="text-sm text-slate-500">OIS 瞬間フォワード・ゼロ曲線 (連続複利)</span>
      </div>

      <div className="container mx-auto px-6 py-8 max-w-6xl">
        {/* 日付コントロール */}
        <div className="bg-white rounded-2xl border border-slate-200 p-6 mb-6">
          <div className="flex flex-wrap gap-2 mb-4">
            {quickDateEntries.map(({ label, value }) =>
              value ? (
                <button
                  key={label}
                  onClick={() => {
                    setDateInput(value);
                    addDate(value);
                  }}
                  disabled={loading || datasets.some((ds) => ds.date === value)}
                  className="px-3 py-1.5 text-sm rounded-lg border border-slate-200 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed text-slate-700"
                >
                  {label} ({value})
                </button>
              ) : null,
            )}
          </div>

          <div className="flex gap-2">
            <input
              type="date"
              value={dateInput}
              onChange={(e) => setDateInput(e.target.value)}
              className="flex-1 border border-slate-200 rounded-lg px-3 py-2 text-sm text-slate-800 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <button
              onClick={handleAdd}
              disabled={loading || !dateInput}
              className="flex items-center gap-1.5 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Plus className="w-4 h-4" />}
              追加
            </button>
          </div>

          {error && (
            <p className="mt-3 text-sm text-red-600 bg-red-50 rounded-lg px-3 py-2">{error}</p>
          )}

          {/* 日付チップ */}
          {datasets.length > 0 && (
            <div className="flex flex-wrap gap-2 mt-4">
              {datasets.map((ds) => (
                <div
                  key={ds.date}
                  className="flex items-center gap-1.5 px-3 py-1 rounded-full text-sm font-medium text-white"
                  style={{ backgroundColor: ds.color }}
                >
                  {ds.date}
                  <button onClick={() => removeDate(ds.date)} className="hover:opacity-70">
                    <X className="w-3.5 h-3.5" />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Maturity range スライダー */}
        <div className="bg-white rounded-2xl border border-slate-200 p-6 mb-6">
          <p className="text-sm font-medium text-slate-700 mb-3">
            残存年数レンジ: {tempMin}Y 〜 {tempMax}Y
          </p>
          <div className="flex gap-6 items-center">
            <div className="flex-1">
              <label className="text-xs text-slate-500 mb-1 block">Min</label>
              <input
                type="range" min={0} max={39} step={1}
                value={tempMin}
                onChange={(e) => setTempMin(Math.min(Number(e.target.value), tempMax - 1))}
                onMouseUp={handleRangeChange}
                onTouchEnd={handleRangeChange}
                className="w-full accent-blue-600"
              />
            </div>
            <div className="flex-1">
              <label className="text-xs text-slate-500 mb-1 block">Max</label>
              <input
                type="range" min={1} max={40} step={1}
                value={tempMax}
                onChange={(e) => setTempMax(Math.max(Number(e.target.value), tempMin + 1))}
                onMouseUp={handleRangeChange}
                onTouchEnd={handleRangeChange}
                className="w-full accent-blue-600"
              />
            </div>
          </div>
          <p className="text-xs text-slate-400 mt-2">
            実線: 瞬間フォワードレート　破線: OIS ゼロレート（連続複利・Act/365）
          </p>
        </div>

        {/* チャート */}
        <div className="bg-white rounded-2xl border border-slate-200 p-6">
          <InstantaneousFwdChart
            datasets={datasets}
            minMaturity={minMaturity}
            maxMaturity={maxMaturity}
          />
        </div>
      </div>
    </main>
  );
}
