'use client';

import { useState, useEffect } from 'react';
import React from 'react';
import Link from 'next/link';
import { fetcher, ForwardCurveResponse, QuickDatesResponse } from '@/lib/api';
import ForwardCurveChart from '@/components/ForwardCurveChart';
import { Loader2, Plus, X, Calendar, TrendingUp, RefreshCw, ZoomIn, ArrowLeft, Settings2 } from 'lucide-react';

const COLORS = [
  '#2563eb', '#dc2626', '#059669', '#d97706', '#7c3aed', 
  '#db2777', '#0891b2', '#4f46e5', '#ea580c', '#1e293b'
];

interface ForwardDataset {
  date: string;
  type: 'fixed-start' | 'fixed-tenor';
  parameter: string;
  data: any[];
  color: string;
}

export default function ForwardCurvePage() {
  const [loading, setLoading] = useState(false);
  const [quickDates, setQuickDates] = useState<QuickDatesResponse | null>(null);
  const [datasets, setDatasets] = useState<ForwardDataset[]>([]);
  const [dateInput, setDateInput] = useState<string>('');
  const [paramInput, setParamInput] = useState<string>('1Y');
  const [curveType, setCurveType] = useState<'fixed-start' | 'fixed-tenor'>('fixed-start');
  const [error, setError] = useState<string | null>(null);

  const [minMaturity, setMinMaturity] = useState(0);
  const [maxMaturity, setMaxMaturity] = useState(40);
  const [tempMin, setTempMin] = useState(0);
  const [tempMax, setTempMax] = useState(40);

  useEffect(() => {
    async function init() {
      try {
        setLoading(true);
        const qd = await fetcher('/api/quick-dates');
        setQuickDates(qd);
        if (qd.latest) {
          setDateInput(qd.latest);
          await addForwardCurve(qd.latest, 'fixed-start', '1Y');
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

  const addForwardCurve = async (date: string, type: 'fixed-start' | 'fixed-tenor', param: string) => {
    if (!date) return;
    const key = `${date}-${type}-${param}`;
    if (datasets.some(ds => `${ds.date}-${ds.type}-${ds.parameter}` === key)) return;
    if (datasets.length >= 10) {
      setError('一度に表示できるのは最大10件までです。');
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const endpoint = type === 'fixed-start' 
        ? `/api/forward-curve/fixed-start/${date}?n=${param}`
        : `/api/forward-curve/fixed-tenor/${date}?m=${param}`;
        
      const resp: ForwardCurveResponse = await fetcher(endpoint);
      if (!resp.data || resp.data.length === 0) {
        setError(`${date} (${param}) のデータが見つかりませんでした。`);
        return;
      }
      
      const newColor = COLORS[datasets.length % COLORS.length];
      const newDataset: ForwardDataset = {
        date: resp.date,
        type: resp.type,
        parameter: resp.parameter,
        data: resp.data,
        color: newColor
      };
      setDatasets(prev => [...prev, newDataset]);
    } catch (err: any) {
      console.error('Failed to add forward curve:', err);
      setError(err.detail || 'データの取得に失敗しました。');
    } finally {
      setLoading(false);
    }
  };

  const removeDataset = (index: number) => {
    setDatasets(prev => {
      const filtered = prev.filter((_, i) => i !== index);
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

  const handleRangeChange = () => {
    if (tempMin >= tempMax) return;
    setMinMaturity(tempMin);
    setMaxMaturity(tempMax);
  };

  return (
    <div className="min-h-screen bg-slate-100 text-slate-900">
      <div className="container mx-auto p-4 md:p-8 space-y-8">
        
        {/* Header */}
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
              <TrendingUp className="w-10 h-10 text-indigo-600" />
              Forward Curve Analysis
            </h1>
            <p className="text-slate-800 font-medium text-lg mt-2">
              スワップのフォワード・カーブを計算・比較します。
            </p>
          </div>

          <button 
            onClick={clearAll}
            className="flex items-center gap-1.5 px-4 py-2 bg-white text-slate-700 font-bold border border-slate-200 hover:text-red-600 hover:border-red-200 hover:bg-red-50 rounded-xl transition-all shadow-sm"
          >
            <RefreshCw className="w-4 h-4" />
            Reset All
          </button>
        </div>

        {/* Control Panel */}
        <div className="bg-white p-6 rounded-2xl shadow-md border border-slate-200 space-y-6">
          <div className="flex flex-col lg:flex-row gap-6">
            {/* Type Selector */}
            <div className="flex-shrink-0">
              <label className="block text-sm font-black text-slate-500 uppercase tracking-widest mb-3">Curve Type</label>
              <div className="flex p-1 bg-slate-100 rounded-xl w-fit">
                <button 
                  onClick={() => setCurveType('fixed-start')}
                  className={`px-4 py-2 rounded-lg text-sm font-bold transition-all ${curveType === 'fixed-start' ? 'bg-white text-indigo-600 shadow-sm' : 'text-slate-500 hover:text-slate-700'}`}
                >
                  n年先スタート (F(n, n+t))
                </button>
                <button 
                  onClick={() => setCurveType('fixed-tenor')}
                  className={`px-4 py-2 rounded-lg text-sm font-bold transition-all ${curveType === 'fixed-tenor' ? 'bg-white text-indigo-600 shadow-sm' : 'text-slate-500 hover:text-slate-700'}`}
                >
                  m年物フォワード (F(t, t+m))
                </button>
              </div>
            </div>

            <div className="hidden lg:block w-px h-16 bg-slate-200"></div>

            {/* Parameter Input */}
            <div className="flex-1 grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-black text-slate-500 uppercase tracking-widest mb-3">Date</label>
                <div className="relative">
                  <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-400" />
                  <input 
                    type="date"
                    value={dateInput}
                    onChange={(e) => setDateInput(e.target.value)}
                    className="w-full pl-10 pr-3 py-2.5 bg-slate-50 border-2 border-slate-200 rounded-xl text-black font-bold focus:outline-none focus:ring-4 focus:ring-indigo-500/20 focus:border-indigo-600 transition-all"
                  />
                </div>
              </div>
              <div>
                <label className="block text-sm font-black text-slate-500 uppercase tracking-widest mb-3">
                  {curveType === 'fixed-start' ? 'Start (n)' : 'Tenor (m)'}
                </label>
                <div className="relative">
                  <Settings2 className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-400" />
                  <input 
                    type="text"
                    value={paramInput}
                    onChange={(e) => setParamInput(e.target.value)}
                    placeholder="e.g. 1Y, 5Y"
                    className="w-full pl-10 pr-3 py-2.5 bg-slate-50 border-2 border-slate-200 rounded-xl text-black font-bold focus:outline-none focus:ring-4 focus:ring-indigo-500/20 focus:border-indigo-600 transition-all"
                  />
                </div>
              </div>
              <div className="flex items-end">
                <button 
                  onClick={() => addForwardCurve(dateInput, curveType, paramInput)}
                  disabled={!dateInput || !paramInput || loading}
                  className="w-full flex items-center justify-center gap-2 px-6 py-2.5 bg-indigo-600 text-white text-base font-black rounded-xl hover:bg-indigo-700 disabled:bg-slate-300 disabled:cursor-not-allowed transition-all shadow-lg active:scale-95"
                >
                  <Plus className="w-5 h-5" />
                  Add Forward Curve
                </button>
              </div>
            </div>
          </div>
        </div>

        {/* Active Chips */}
        {datasets.length > 0 && (
          <div className="flex flex-wrap gap-3">
            {datasets.map((ds, idx) => (
              <div 
                key={idx} 
                className="flex items-center gap-3 pl-4 pr-3 py-2 bg-white border-2 rounded-xl shadow-sm transition-all"
                style={{ borderColor: ds.color }}
              >
                <div className="flex flex-col leading-tight">
                  <span className="text-sm font-black text-black">{ds.date}</span>
                  <span className="text-[10px] font-bold text-slate-500 uppercase tracking-tighter">
                    {ds.type === 'fixed-start' ? `n=${ds.parameter}` : `m=${ds.parameter}`}
                  </span>
                </div>
                <button onClick={() => removeDataset(idx)} className="p-1 hover:bg-slate-100 rounded-full text-slate-400 hover:text-red-500 transition-colors">
                  <X className="w-4 h-4" />
                </button>
              </div>
            ))}
          </div>
        )}

        {/* Chart Section */}
        <div className="bg-white rounded-3xl shadow-xl border border-slate-200 overflow-hidden relative min-h-[650px] pb-6">
          {loading && (
            <div className="absolute inset-0 bg-white/90 backdrop-blur-sm z-20 flex flex-col items-center justify-center gap-4">
              <Loader2 className="w-12 h-12 animate-spin text-indigo-600" />
              <p className="text-xl font-black text-slate-800">Calculating Forward Curve...</p>
            </div>
          )}

          <div className="p-4 md:p-8 pb-0">
            {error && (
              <div className="mb-6 p-4 bg-red-50 text-red-700 font-bold rounded-xl border-2 border-red-100 flex items-center justify-between">
                <span>{error}</span>
                <button onClick={() => setError(null)} className="hover:bg-red-100 p-1 rounded-full"><X className="w-5 h-5" /></button>
              </div>
            )}
            
            {datasets.length > 0 ? (
              <>
                <div className="mb-4 text-sm font-bold text-slate-500 italic">
                  * X-Axis: {datasets[0].type === 'fixed-start' ? 'Swap Tenor (t)' : 'Maturity (t + m)'}
                </div>
                <ForwardCurveChart 
                  datasets={datasets} 
                  minMaturity={minMaturity}
                  maxMaturity={maxMaturity}
                />
              </>
            ) : (
              <div className="h-[500px] flex flex-col items-center justify-center text-slate-400 border-4 border-dashed border-slate-100 rounded-3xl bg-slate-50/50">
                <TrendingUp className="w-20 h-20 mb-6 text-slate-200" />
                <p className="text-2xl font-black text-slate-400">No data selected</p>
                <p className="text-slate-400 mt-2 font-medium">Add parameters above to generate forward curves.</p>
              </div>
            )}
          </div>

          {/* Zoom Controls */}
          {datasets.length > 0 && (
            <div className="px-8 mt-2 mb-6">
              <div className="bg-slate-50 p-6 rounded-xl border border-slate-200">
                <div className="flex items-center gap-2 text-slate-700 font-bold mb-4 border-b border-slate-200 pb-2">
                  <ZoomIn className="w-5 h-5 text-indigo-600" />
                  Zoom Control
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                  <div className="space-y-3">
                    <div className="flex justify-between items-center">
                      <label className="text-sm font-bold text-slate-600">Min</label>
                      <span className="text-sm font-black text-indigo-600">{tempMin}</span>
                    </div>
                    <input 
                      type="range" min="0" max="40" step="1" value={tempMin}
                      onChange={(e) => setTempMin(Number(e.target.value))}
                      onMouseUp={handleRangeChange} onTouchEnd={handleRangeChange}
                      className="w-full h-2 bg-slate-200 rounded-lg appearance-none cursor-pointer accent-indigo-600"
                    />
                  </div>
                  <div className="space-y-3">
                    <div className="flex justify-between items-center">
                      <label className="text-sm font-bold text-slate-600">Max</label>
                      <span className="text-sm font-black text-indigo-600">{tempMax}</span>
                    </div>
                    <input 
                      type="range" min="0" max="40" step="1" value={tempMax}
                      onChange={(e) => setTempMax(Number(e.target.value))}
                      onMouseUp={handleRangeChange} onTouchEnd={handleRangeChange}
                      className="w-full h-2 bg-slate-200 rounded-lg appearance-none cursor-pointer accent-indigo-600"
                    />
                  </div>
                </div>
              </div>
            </div>
          )}
          
          <div className="bg-slate-50 border-t border-slate-100 px-8 py-5">
            <div className="flex flex-wrap gap-x-10 gap-y-2 text-sm text-slate-600 font-semibold">
              <p>Data: TONA OIS (IRS)</p>
              <p>Calculation: QuantLib PiecewiseLogCubicDiscount</p>
              <p>Convention: Act/365 Fixed, Annual</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
