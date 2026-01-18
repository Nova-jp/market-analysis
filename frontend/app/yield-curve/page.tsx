'use client';

import React, { useState, useEffect } from 'react';
import { fetcher, YieldCurveResponse, QuickDatesResponse } from '@/lib/api';
import YieldCurveChart from '@/components/YieldCurveChart';
import { Loader2, Calendar, TrendingUp } from 'lucide-react';

export default function YieldCurvePage() {
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState<YieldCurveResponse | null>(null);
  const [quickDates, setQuickDates] = useState<QuickDatesResponse | null>(null);
  const [selectedDate, setSelectedDate] = useState<string>('');
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function init() {
      try {
        const qd = await fetcher('/api/quick-dates');
        setQuickDates(qd);
        if (qd.latest) {
          setSelectedDate(qd.latest);
          const yieldData = await fetcher(`/api/yield-data/${qd.latest}`);
          setData(yieldData);
        }
      } catch (err) {
        console.error('Failed to initialize yield curve page:', err);
        setError('データの取得に失敗しました。バックエンドサーバーが起動しているか確認してください。');
      } finally {
        setLoading(false);
      }
    }
    init();
  }, []);

  const handleDateChange = async (date: string) => {
    if (date === selectedDate) return;
    setSelectedDate(date);
    setLoading(true);
    try {
      const yieldData = await fetcher(`/api/yield-data/${date}`);
      setData(yieldData);
      setError(null);
    } catch (err) {
      console.error('Failed to update yield data:', err);
      setError('データの更新に失敗しました。');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-50/50">
      <div className="container mx-auto p-4 md:p-8 space-y-8">
        {/* Header Section */}
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="space-y-1">
            <h1 className="text-3xl font-bold tracking-tight text-slate-900 flex items-center gap-2">
              <TrendingUp className="w-8 h-8 text-blue-600" />
              Yield Curve Comparison
            </h1>
            <p className="text-slate-500">日本国債(JGB)のイールドカーブを視覚化し、市場動向を分析します。</p>
          </div>
          
          <div className="flex items-center gap-3 bg-white p-2 rounded-lg shadow-sm border border-slate-200">
            <Calendar className="w-5 h-5 text-slate-400 ml-2" />
            <select 
              value={selectedDate} 
              onChange={(e) => handleDateChange(e.target.value)}
              className="bg-transparent border-none focus:ring-0 text-slate-700 font-medium pr-8 cursor-pointer"
            >
              {quickDates?.latest && <option value={quickDates.latest}>Latest ({quickDates.latest})</option>}
              {quickDates?.previous && <option value={quickDates.previous}>Previous ({quickDates.previous})</option>}
              {quickDates?.five_days_ago && <option value={quickDates.five_days_ago}>5 Days Ago ({quickDates.five_days_ago})</option>}
              {quickDates?.month_ago && <option value={quickDates.month_ago}>1 Month Ago ({quickDates.month_ago})</option>}
            </select>
          </div>
        </div>

        {/* Main Content Card */}
        <div className="bg-white rounded-2xl shadow-md border border-slate-200 overflow-hidden relative">
          {loading && (
            <div className="absolute inset-0 bg-white/60 backdrop-blur-[1px] z-10 flex flex-col items-center justify-center gap-3">
              <Loader2 className="w-10 h-10 animate-spin text-blue-600" />
              <p className="text-slate-600 font-medium">データを読み込み中...</p>
            </div>
          )}

          <div className="p-6 md:p-8">
            <div className="flex items-center justify-between mb-8">
              <h2 className="text-xl font-bold text-slate-800">
                JGB Yield Curve <span className="text-blue-600 ml-2">@{selectedDate}</span>
              </h2>
              <div className="flex items-center gap-4 text-sm text-slate-500">
                <div className="flex items-center gap-1.5">
                  <span className="w-3 h-3 rounded-full bg-blue-600"></span>
                  <span>Compound Yield</span>
                </div>
              </div>
            </div>

            {error ? (
              <div className="h-[400px] flex items-center justify-center text-red-500 bg-red-50 rounded-xl border border-red-100 p-4">
                {error}
              </div>
            ) : data && data.data.length > 0 ? (
              <YieldCurveChart data={data.data} name={`JGB ${selectedDate}`} />
            ) : !loading && (
              <div className="h-[400px] flex items-center justify-center text-slate-400 bg-slate-50 rounded-xl border border-dashed border-slate-200">
                データが見つかりませんでした。
              </div>
            )}
          </div>
          
          <div className="bg-slate-50 border-t border-slate-100 px-8 py-4">
            <div className="flex flex-wrap gap-x-8 gap-y-2 text-xs text-slate-500">
              <p>Source: JSDA (日本証券業協会) 公表価格</p>
              <p>Type: Average Compound Yield (複利利回り)</p>
            </div>
          </div>
        </div>

        {/* Future feature placeholders */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm opacity-50 cursor-not-allowed">
            <h3 className="font-bold text-slate-700 mb-2">Comparison Mode</h3>
            <p className="text-sm text-slate-500">2つの異なる日付のカーブを重ねて表示します（開発中）</p>
          </div>
          <div className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm opacity-50 cursor-not-allowed">
            <h3 className="font-bold text-slate-700 mb-2">ASW Analysis</h3>
            <p className="text-sm text-slate-500">スワップスプレッドの推移を表示します（開発中）</p>
          </div>
          <div className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm opacity-50 cursor-not-allowed">
            <h3 className="font-bold text-slate-700 mb-2">Export Data</h3>
            <p className="text-sm text-slate-500">CSV/Excel形式でのデータダウンロード（開発中）</p>
          </div>
        </div>
      </div>
    </div>
  );
}
