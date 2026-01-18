'use client';

import { useState } from 'react';
import React from 'react';
import Link from 'next/link';
import { fetcher, PCAResponse } from '@/lib/api';
import LoadingsChart from '@/components/pca/LoadingsChart';
import ScoreChart from '@/components/pca/ScoreChart';
import { 
  BarChart3, 
  Settings2, 
  Play, 
  Loader2, 
  Info, 
  ArrowLeft,
  Activity,
  Layers,
  CalendarDays
} from 'lucide-react';

export default function PCAPage() {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<PCAResponse | null>(null);
  const [days, setDays] = useState(100);
  const [components, setComponents] = useState(3);
  const [error, setError] = useState<string | null>(null);

  const runAnalysis = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetcher(`/api/pca/analyze?days=${days}&components=${components}`);
      setResult(data);
    } catch (err) {
      console.error('PCA Analysis failed:', err);
      setError('分析の実行に失敗しました。データ不足かサーバーエラーの可能性があります。');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-100">
      <div className="container mx-auto p-4 md:p-8 space-y-8">
        
        {/* Title Header */}
        <div className="flex flex-col items-start gap-4 bg-white/50 p-6 rounded-2xl border border-white/20 shadow-sm">
          <Link 
            href="/" 
            className="inline-flex items-center gap-2 text-slate-500 hover:text-indigo-600 font-bold mb-1 transition-colors"
          >
            <ArrowLeft className="w-4 h-4" />
            Back to Home
          </Link>
          <div>
            <h1 className="text-4xl font-extrabold text-black flex items-center gap-3 tracking-tight">
              <BarChart3 className="w-10 h-10 text-indigo-600" />
              PCA Analysis
            </h1>
            <p className="text-slate-800 font-medium text-lg mt-2">
              主成分分析を用いてイールドカーブの変動要因（Level, Slope, Curvature）を分解・可視化します。
            </p>
          </div>
        </div>

        {/* Control Panel */}
        <div className="bg-white p-6 rounded-2xl shadow-md border border-slate-200">
          <div className="flex items-center gap-2 text-slate-700 font-bold mb-6 border-b border-slate-100 pb-2">
            <Settings2 className="w-5 h-5 text-indigo-600" />
            <h2>Analysis Parameters</h2>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 items-end">
            {/* Lookback Period */}
            <div className="space-y-4">
              <div className="flex justify-between items-center">
                <label className="text-sm font-bold text-slate-600 flex items-center gap-2">
                  <CalendarDays className="w-4 h-4 text-slate-400" />
                  Lookback Period
                </label>
                <span className="text-sm font-black text-indigo-600 bg-indigo-50 px-3 py-1 rounded-lg border border-indigo-100">
                  {days} Days
                </span>
              </div>
              <input 
                type="range" 
                min="30" 
                max="200" 
                step="10" 
                value={days} 
                onChange={(e) => setDays(Number(e.target.value))}
                className="w-full h-2 bg-slate-200 rounded-lg appearance-none cursor-pointer accent-indigo-600"
              />
              <p className="text-xs text-slate-400 font-medium">分析に使用する過去の営業日数 (30-200)</p>
            </div>

            {/* Principal Components */}
            <div className="space-y-4">
              <div className="flex justify-between items-center">
                <label className="text-sm font-bold text-slate-600 flex items-center gap-2">
                  <Layers className="w-4 h-4 text-slate-400" />
                  Principal Components
                </label>
                <span className="text-sm font-black text-indigo-600 bg-indigo-50 px-3 py-1 rounded-lg border border-indigo-100">
                  {components} PCs
                </span>
              </div>
              <div className="flex gap-2">
                {[1, 2, 3, 4, 5].map((num) => (
                  <button
                    key={num}
                    onClick={() => setComponents(num)}
                    className={`flex-1 py-2.5 rounded-xl text-sm font-bold transition-all ${
                      components === num 
                        ? 'bg-indigo-600 text-white shadow-md scale-105' 
                        : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                    }`}
                  >
                    {num}
                  </button>
                ))}
              </div>
              <p className="text-xs text-slate-400 font-medium">抽出する主成分の数 (通常は3つで十分)</p>
            </div>

            {/* Run Button */}
            <div>
              <button
                onClick={runAnalysis}
                disabled={loading}
                className="w-full py-3.5 px-6 bg-indigo-600 hover:bg-indigo-700 disabled:bg-indigo-400 text-white rounded-xl font-black shadow-lg shadow-indigo-200 transition-all flex items-center justify-center gap-2 active:scale-95"
              >
                {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : <Play className="w-5 h-5" />}
                {loading ? 'Analyzing...' : 'Run Analysis'}
              </button>
            </div>
          </div>
        </div>

        {/* Error Display */}
        {error && (
          <div className="bg-red-50 text-red-700 p-4 rounded-xl border-2 border-red-100 flex items-center gap-3 font-bold animate-in fade-in slide-in-from-top-2">
            <Info className="w-5 h-5 flex-shrink-0" />
            {error}
          </div>
        )}

        {/* Results Section */}
        {result && (
          <div className="space-y-6 animate-in fade-in slide-in-from-bottom-8 duration-700">
            {/* Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {result.components.map((comp, idx) => (
                <div key={comp.pc_number} className="bg-white p-6 rounded-2xl shadow-sm border border-slate-200 relative overflow-hidden group hover:shadow-md transition-shadow">
                  <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-20 transition-opacity">
                    <span className="text-6xl font-black text-indigo-900">{idx + 1}</span>
                  </div>
                  <h3 className="text-sm text-slate-500 font-bold uppercase tracking-wider mb-2">
                    {idx === 0 ? 'Level' : idx === 1 ? 'Slope' : idx === 2 ? 'Curvature' : `PC${idx + 1}`} Contribution
                  </h3>
                  <div className="flex items-baseline gap-2">
                    <span className="text-3xl font-black text-slate-900">
                      {(comp.explained_variance_ratio * 100).toFixed(1)}%
                    </span>
                    <span className="text-sm font-bold text-slate-400">variance</span>
                  </div>
                  <div className="mt-4 space-y-2">
                    <div className="flex justify-between text-xs font-bold text-slate-500">
                      <span>Cumulative</span>
                      <span>{(comp.cumulative_variance_ratio * 100).toFixed(1)}%</span>
                    </div>
                    <div className="w-full bg-slate-100 h-2 rounded-full overflow-hidden">
                      <div 
                        className="h-full bg-indigo-500 rounded-full transition-all duration-1000 ease-out" 
                        style={{ width: `${comp.cumulative_variance_ratio * 100}%` }}
                      ></div>
                    </div>
                  </div>
                </div>
              ))}
            </div>

            {/* Charts Grid */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Factor Loadings */}
              <div className="bg-white p-6 rounded-3xl shadow-xl border border-slate-200">
                <h3 className="font-bold text-xl text-slate-800 mb-6 flex items-center gap-2">
                  <BarChart3 className="w-6 h-6 text-indigo-500" />
                  Factor Loadings
                </h3>
                <LoadingsChart components={result.components} maturities={result.maturities} />
                <div className="mt-6 bg-slate-50 p-4 rounded-xl border border-slate-100">
                  <p className="text-xs text-slate-500 font-medium leading-relaxed">
                    <strong>Factor Loadings (感応度):</strong> 各年限の利回りが主成分の変化に対してどれだけ反応するかを示します。<br/>
                    • <span className="text-blue-600 font-bold">PC1 (Level)</span>: 全期間でプラス。金利水準全体の変動。<br/>
                    • <span className="text-red-600 font-bold">PC2 (Slope)</span>: 短期と長期で逆符号。イールドカーブの傾きの変化。<br/>
                    • <span className="text-green-600 font-bold">PC3 (Curvature)</span>: 中期と短・長期で逆符号。カーブの曲率の変化。
                  </p>
                </div>
              </div>

              {/* PC Scores */}
              <div className="bg-white p-6 rounded-3xl shadow-xl border border-slate-200">
                <h3 className="font-bold text-xl text-slate-800 mb-6 flex items-center gap-2">
                  <Activity className="w-6 h-6 text-indigo-500" />
                  PC Scores
                </h3>
                <ScoreChart scores={result.scores} componentsCount={result.parameters.components} />
                <div className="mt-6 bg-slate-50 p-4 rounded-xl border border-slate-100">
                  <p className="text-xs text-slate-500 font-medium leading-relaxed">
                    <strong>PC Scores (時系列推移):</strong> 過去{result.parameters.days}営業日における各要因の強さの推移です。<br/>
                    スコアがプラスの時はその要因が金利を押し上げる方向に働き、マイナスの時は押し下げる方向に働いています。市場イベント時のカーブ形状の変化を追跡できます。
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}