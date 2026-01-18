'use client';

import { useState, useEffect } from 'react';
import React from 'react';
import Link from 'next/link';
import { fetcher, YieldCurveResponse, QuickDatesResponse } from '@/lib/api';
import YieldCurveChart from '@/components/YieldCurveChart';
import { Loader2, Plus, X, Calendar, TrendingUp, RefreshCw, ZoomIn, ArrowLeft } from 'lucide-react';

// カラーパレット
const COLORS = [
  '#2563eb', '#dc2626', '#059669', '#d97706', '#7c3aed', 
  '#db2777', '#0891b2', '#4f46e5', '#ea580c', '#1e293b'
];

interface Dataset {
  date: string;
  data: any[];
  color: string;
}

export default function YieldCurvePage() {
  const [loading, setLoading] = useState(false);
  const [quickDates, setQuickDates] = useState<QuickDatesResponse | null>(null);
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [dateInput, setDateInput] = useState<string>('');
  const [error, setError] = useState<string | null>(null);

  // Zoom State
  const [minMaturity, setMinMaturity] = useState(0);
  const [maxMaturity, setMaxMaturity] = useState(40);
  
  // スライダー操作用の一時ステート
  const [tempMin, setTempMin] = useState(0);
  const [tempMax, setTempMax] = useState(40);

  useEffect(() => {
    async function init() {
      try {
        setLoading(true);
        const qd = await fetcher('/api/quick-dates');
        setQuickDates(qd);
        if (qd.latest) {
          await addDateToChart(qd.latest);
        }
      } catch (err) {
        console.error('Initialization failed:', err);
        setError('初期データの読み込みに失敗しました。');
      } finally {
        setLoading(false);
      }
    }
    init();
  }, []);

  const addDateToChart = async (date: string) => {
    if (!date) return;
    if (datasets.some(ds => ds.date === date)) return;
    if (datasets.length >= 10) {
      setError('一度に表示できるのは最大10件までです。');
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const resp: YieldCurveResponse = await fetcher(`/api/yield-data/${date}`);
      if (!resp.data || resp.data.length === 0) {
        setError(`${date} のデータが見つかりませんでした。`);
        return;
      }
      const newColor = COLORS[datasets.length % COLORS.length];
      const newDataset: Dataset = {
        date: resp.date,
        data: resp.data,
        color: newColor
      };
      setDatasets(prev => [...prev, newDataset]);
      setDateInput('');
    } catch (err) {
      console.error('Failed to add date:', err);
      setError('データの取得に失敗しました。');
    } finally {
      setLoading(false);
    }
  };

  const removeDate = (dateToRemove: string) => {
    setDatasets(prev => {
      const filtered = prev.filter(ds => ds.date !== dateToRemove);
      return filtered.map((ds, idx) => ({
        ...ds,
        color: COLORS[idx % COLORS.length]
      }));
    });
  };

  const clearAll = () => {
    setDatasets([]);
    setError(null);
  };

  // スライダー変更時の処理（離したときに確定）
  const handleRangeChange = () => {
    if (tempMin >= tempMax) return;
    setMinMaturity(tempMin);
    setMaxMaturity(tempMax);
  };

  return (
    <div className="min-h-screen bg-slate-100">
      <div className="container mx-auto p-4 md:p-8 space-y-8">
        
        {/* Title Header */}
        <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 bg-white/50 p-6 rounded-2xl border border-white/20 shadow-sm">
          <div>
            <Link 
              href="/" 
              className="inline-flex items-center gap-2 text-slate-500 hover:text-blue-600 font-bold mb-2 transition-colors"
            >
              <ArrowLeft className="w-4 h-4" />
              Back to Home
            </Link>
            <h1 className="text-4xl font-extrabold text-black flex items-center gap-3 tracking-tight">
              <TrendingUp className="w-10 h-10 text-blue-600" />
              Yield Curve Comparison
            </h1>
            <p className="text-slate-800 font-medium text-lg mt-2">
              複数の日付を追加して、イールドカーブの変動を比較分析します。
            </p>
          </div>

          <div className="flex items-center gap-2">
             {datasets.length > 0 && (
               <button 
                 onClick={clearAll}
                 className="flex items-center gap-1.5 px-4 py-2 bg-white text-slate-700 font-bold border border-slate-200 hover:text-red-600 hover:border-red-200 hover:bg-red-50 rounded-xl transition-all shadow-sm"
               >
                 <RefreshCw className="w-4 h-4" />
                 Reset All
               </button>
             )}
          </div>
        </div>

        {/* Control Bar */}
        <div className="bg-white p-5 rounded-2xl shadow-md border border-slate-200 flex flex-col xl:flex-row items-center gap-6">
            <div className="flex items-center gap-2 overflow-x-auto w-full xl:w-auto pb-2 xl:pb-0 scrollbar-hide">
                <span className="text-sm font-black text-slate-500 uppercase tracking-widest mr-3 whitespace-nowrap">Quick Add:</span>
                {quickDates?.latest && (
                    <button onClick={() => addDateToChart(quickDates.latest!)} className="px-4 py-2 bg-blue-600 text-white text-sm font-bold rounded-full hover:bg-blue-700 whitespace-nowrap transition-all shadow-sm active:scale-95">Latest</button>
                )}
                {quickDates?.previous && (
                    <button onClick={() => addDateToChart(quickDates.previous!)} className="px-4 py-2 bg-slate-800 text-white text-sm font-bold rounded-full hover:bg-black whitespace-nowrap transition-all shadow-sm active:scale-95">Previous</button>
                )}
                {quickDates?.five_days_ago && (
                    <button onClick={() => addDateToChart(quickDates.five_days_ago!)} className="px-4 py-2 bg-slate-800 text-white text-sm font-bold rounded-full hover:bg-black whitespace-nowrap transition-all shadow-sm active:scale-95">-5 Days</button>
                )}
                {quickDates?.month_ago && (
                    <button onClick={() => addDateToChart(quickDates.month_ago!)} className="px-4 py-2 bg-slate-800 text-white text-sm font-bold rounded-full hover:bg-black whitespace-nowrap transition-all shadow-sm active:scale-95">-1 Month</button>
                )}
            </div>
            <div className="hidden xl:block w-px h-10 bg-slate-200"></div>
            <div className="flex items-center gap-3 w-full xl:w-auto">
                <div className="relative flex-1 xl:w-72">
                    <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-600" />
                    <input 
                        type="date"
                        value={dateInput}
                        onChange={(e) => setDateInput(e.target.value)}
                        className="w-full pl-10 pr-3 py-2.5 bg-white border-2 border-slate-300 rounded-xl text-black font-black text-base focus:outline-none focus:ring-4 focus:ring-blue-500/20 focus:border-blue-600 transition-all"
                    />
                </div>
                <button 
                    onClick={() => addDateToChart(dateInput)}
                    disabled={!dateInput || loading}
                    className="flex items-center gap-2 px-6 py-2.5 bg-blue-600 text-white text-base font-black rounded-xl hover:bg-blue-700 disabled:bg-slate-300 disabled:cursor-not-allowed transition-all shadow-lg active:scale-95 flex-shrink-0"
                >
                    <Plus className="w-5 h-5" />
                    Add Curve
                </button>
            </div>
        </div>

        {/* Active Chips */}
        {datasets.length > 0 && (
            <div className="flex flex-wrap gap-3 animate-in fade-in slide-in-from-top-4 duration-500">
                {datasets.map((ds) => (
                    <div 
                        key={ds.date} 
                        className="flex items-center gap-3 pl-4 pr-3 py-2 bg-white border-2 rounded-xl shadow-sm transition-all hover:shadow-md hover:-translate-y-0.5"
                        style={{ borderColor: ds.color }}
                    >
                        <span className="text-base font-black text-black">{ds.date}</span>
                        <button onClick={() => removeDate(ds.date)} className="p-1 hover:bg-slate-100 rounded-full text-slate-400 hover:text-red-500 transition-colors">
                            <X className="w-4 h-4" />
                        </button>
                    </div>
                ))}
            </div>
        )}

        {/* Main Chart Card */}
        <div className="bg-white rounded-3xl shadow-xl border border-slate-200 overflow-hidden relative min-h-[650px] pb-6">
            {loading && (
                <div className="absolute inset-0 bg-white/90 backdrop-blur-sm z-20 flex flex-col items-center justify-center gap-4">
                    <Loader2 className="w-12 h-12 animate-spin text-blue-600" />
                    <p className="text-xl font-black text-slate-800 animate-pulse">Fetching Data...</p>
                </div>
            )}

            <div className="p-4 md:p-8 pb-0">
                {error && (
                    <div className="mb-6 p-4 bg-red-50 text-red-700 font-bold rounded-xl border-2 border-red-100 flex items-center justify-between animate-shake">
                        <span>{error}</span>
                        <button onClick={() => setError(null)} className="hover:bg-red-100 p-1 rounded-full"><X className="w-5 h-5" /></button>
                    </div>
                )}
                
                {datasets.length > 0 ? (
                    <YieldCurveChart 
                      datasets={datasets} 
                      minMaturity={minMaturity}
                      maxMaturity={maxMaturity}
                    />
                ) : (
                    <div className="h-[500px] flex flex-col items-center justify-center text-slate-400 border-4 border-dashed border-slate-100 rounded-3xl bg-slate-50/50">
                        <TrendingUp className="w-20 h-20 mb-6 text-slate-200" />
                        <p className="text-2xl font-black text-slate-400">No data selected</p>
                        <p className="text-slate-400 mt-2 font-medium">Select dates from the menu above to start comparing.</p>
                    </div>
                )}
            </div>

            {/* Range Slider Control */}
            {datasets.length > 0 && (
                <div className="px-8 mt-2 mb-6">
                    <div className="bg-slate-50 p-6 rounded-xl border border-slate-200">
                        <div className="flex items-center gap-2 text-slate-700 font-bold mb-4 border-b border-slate-200 pb-2">
                            <ZoomIn className="w-5 h-5 text-blue-600" />
                            Maturity Zoom Control
                        </div>
                        
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                          {/* Min Slider */}
                          <div className="space-y-3">
                             <div className="flex justify-between items-center">
                               <label className="text-sm font-bold text-slate-600">Min Maturity</label>
                               <span className="text-sm font-black text-blue-600 bg-white px-2 py-1 rounded border border-blue-100">{tempMin} Years</span>
                             </div>
                             <input 
                               type="range" 
                               min="0" max="40" step="1"
                               value={tempMin}
                               onChange={(e) => {
                                 const val = Number(e.target.value);
                                 if (val < tempMax) { // Maxより小さければOK
                                    setTempMin(val);
                                 }
                               }}
                               onMouseUp={handleRangeChange}
                               onTouchEnd={handleRangeChange}
                               className="w-full h-2 bg-slate-200 rounded-lg appearance-none cursor-pointer accent-blue-600"
                             />
                             <div className="flex justify-between text-xs text-slate-400 font-medium">
                               <span>0Y</span>
                               <span>40Y</span>
                             </div>
                          </div>

                          {/* Max Slider */}
                          <div className="space-y-3">
                             <div className="flex justify-between items-center">
                               <label className="text-sm font-bold text-slate-600">Max Maturity</label>
                               <span className="text-sm font-black text-blue-600 bg-white px-2 py-1 rounded border border-blue-100">{tempMax} Years</span>
                             </div>
                             <input 
                               type="range" 
                               min="0" max="40" step="1"
                               value={tempMax}
                               onChange={(e) => {
                                 const val = Number(e.target.value);
                                 if (val > tempMin) { // Minより大きければOK
                                    setTempMax(val);
                                 }
                               }}
                               onMouseUp={handleRangeChange}
                               onTouchEnd={handleRangeChange}
                               className="w-full h-2 bg-slate-200 rounded-lg appearance-none cursor-pointer accent-blue-600"
                             />
                             <div className="flex justify-between text-xs text-slate-400 font-medium">
                               <span>0Y</span>
                               <span>40Y</span>
                             </div>
                          </div>
                        </div>
                    </div>
                </div>
            )}
            
            <div className="bg-slate-50 border-t border-slate-100 px-8 py-5">
                <div className="flex flex-wrap gap-x-10 gap-y-2 text-sm text-slate-600 font-semibold">
                    <p className="flex items-center gap-2">Source: JSDA</p>
                    <p className="flex items-center gap-2">Type: Average Compound Yield</p>
                </div>
            </div>
        </div>
      </div>
    </div>
  );
}
