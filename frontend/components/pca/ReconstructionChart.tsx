'use client';

import React, { useState, useMemo } from 'react';
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  Cell,
} from 'recharts';
import { ReconstructionDataPoint } from '@/lib/api';
import { Filter } from 'lucide-react';

interface ReconstructionChartProps {
  data: ReconstructionDataPoint[];
}

// 期間に応じた色を生成する関数 (Fallback)
const getColorForMaturity = (maturity: number): string => {
  if (maturity < 2) return '#3b82f6'; // blue-500
  if (maturity < 5) return '#0ea5e9'; // sky-500
  if (maturity < 10) return '#10b981'; // emerald-500
  if (maturity < 20) return '#eab308'; // yellow-500
  if (maturity < 30) return '#f97316'; // orange-500
  return '#ef4444'; // red-500
};

// 銘柄コード（下4桁）に応じた色を生成する関数
const getColorForBondType = (bondCode: string, maturity: number): string => {
  if (!bondCode) return getColorForMaturity(maturity);
  
  // bondCodeの下4桁を取得
  const suffix = bondCode.slice(-4);
  
  switch (suffix) {
    case '0054': // 40年債
      return '#9333ea'; // purple-600
    case '0068': // 30年債
      return '#dc2626'; // red-600
    case '0069': // 20年債
      return '#ea580c'; // orange-600
    case '0067': // 10年債
    case '0058': // GX 10年債
      return '#16a34a'; // green-600
    case '0045': // 5年債
    case '0057': // GX 5年債
    case '0027': // GX 5年債 (WI)
      return '#0284c7'; // sky-600
    case '0042': // 2年債
      return '#2563eb'; // blue-600
    case '0074': // 短期証券
      return '#64748b'; // slate-500
    default:
      return getColorForMaturity(maturity);
  }
};

const ReconstructionChart = ({ data }: ReconstructionChartProps) => {
  // フィルタリング用のState
  const [minMaturity, setMinMaturity] = useState<number>(0);
  const [maxMaturity, setMaxMaturity] = useState<number>(45);

  const filteredData = useMemo(() => {
    return data
      .filter(d => d.maturity >= minMaturity && d.maturity <= maxMaturity)
      .map(d => ({
        ...d,
        errorBps: d.error * 100, // % -> bps
        fillColor: getColorForBondType(d.bond_code, d.maturity)
      }));
  }, [data, minMaturity, maxMaturity]);

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="flex flex-wrap items-center gap-4 bg-slate-50 p-4 rounded-xl border border-slate-100">
        <div className="flex items-center gap-2 text-slate-500 font-bold text-sm">
          <Filter className="w-4 h-4" />
          <span>Maturity Range:</span>
        </div>
        
        <div className="flex items-center gap-3">
          <div className="relative">
            <input
              type="number"
              value={minMaturity}
              onChange={(e) => setMinMaturity(Number(e.target.value))}
              className="w-20 px-2 py-1 text-right bg-white border border-slate-300 rounded-lg text-sm font-mono font-bold focus:ring-2 focus:ring-indigo-500 outline-none"
              step="1"
              min="0"
            />
            <span className="absolute right-7 top-1/2 -translate-y-1/2 text-xs text-slate-400 pointer-events-none">Y</span>
          </div>
          <span className="text-slate-400 font-bold">to</span>
          <div className="relative">
            <input
              type="number"
              value={maxMaturity}
              onChange={(e) => setMaxMaturity(Number(e.target.value))}
              className="w-20 px-2 py-1 text-right bg-white border border-slate-300 rounded-lg text-sm font-mono font-bold focus:ring-2 focus:ring-indigo-500 outline-none"
              step="1"
              min="0"
            />
            <span className="absolute right-7 top-1/2 -translate-y-1/2 text-xs text-slate-400 pointer-events-none">Y</span>
          </div>
        </div>

        {/* Legend */}
        <div className="flex flex-wrap gap-2 ml-auto text-xs font-bold text-slate-600">
          <div className="flex items-center gap-1"><div className="w-3 h-3 rounded-full bg-[#2563eb]"></div>2Y</div>
          <div className="flex items-center gap-1"><div className="w-3 h-3 rounded-full bg-[#0284c7]"></div>5Y</div>
          <div className="flex items-center gap-1"><div className="w-3 h-3 rounded-full bg-[#16a34a]"></div>10Y</div>
          <div className="flex items-center gap-1"><div className="w-3 h-3 rounded-full bg-[#ea580c]"></div>20Y</div>
          <div className="flex items-center gap-1"><div className="w-3 h-3 rounded-full bg-[#dc2626]"></div>30Y</div>
          <div className="flex items-center gap-1"><div className="w-3 h-3 rounded-full bg-[#9333ea]"></div>40Y</div>
        </div>
      </div>

      <div className="h-[500px] w-full bg-white rounded-xl border border-slate-100 p-2">
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 10 }}>
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
            <XAxis 
              type="number" 
              dataKey="maturity" 
              name="Maturity" 
              unit="Y" 
              domain={[minMaturity, maxMaturity]}
              allowDataOverflow={true}
              label={{ value: 'Maturity (Years)', position: 'insideBottom', offset: -10, fill: '#64748b' }}
              tick={{ fontSize: 12, fill: '#64748b' }}
              tickCount={10}
            />
            <YAxis 
              type="number" 
              dataKey="errorBps" 
              name="Error" 
              unit=" bps" 
              label={{ value: 'Reconstruction Error (bps)', angle: -90, position: 'insideLeft', offset: 0, fill: '#64748b' }}
              tick={{ fontSize: 12, fill: '#64748b' }}
              domain={['auto', 'auto']}
            />
            <Tooltip 
              cursor={{ strokeDasharray: '3 3' }}
              content={({ active, payload }) => {
                if (active && payload && payload.length) {
                  const d = payload[0].payload;
                  return (
                    <div className="bg-white/95 backdrop-blur-sm p-4 border border-slate-200 rounded-xl shadow-2xl text-sm min-w-[200px]">
                      <p className="font-bold text-slate-800 border-b border-slate-100 pb-2 mb-2 text-base">
                        {d.bond_name || d.bond_code}
                      </p>
                      <div className="space-y-2">
                        <div className="flex justify-between gap-6">
                          <span className="text-slate-500 font-medium">Maturity</span>
                          <span className="font-mono font-bold text-slate-700">{d.maturity.toFixed(2)}Y</span>
                        </div>
                        <div className="flex justify-between gap-6">
                          <span className="text-slate-500 font-medium">Error</span>
                          <span className={`font-mono font-bold ${d.errorBps > 0 ? 'text-red-500' : 'text-blue-500'}`}>
                            {d.errorBps.toFixed(2)} bps
                          </span>
                        </div>
                        <div className="grid grid-cols-2 gap-2 mt-2 pt-2 border-t border-slate-100">
                          <div>
                            <span className="text-xs text-slate-400 block">Original</span>
                            <span className="font-mono font-bold text-slate-600">{d.original_yield.toFixed(3)}%</span>
                          </div>
                          <div className="text-right">
                            <span className="text-xs text-slate-400 block">Fitted</span>
                            <span className="font-mono font-bold text-slate-600">{d.reconstructed_yield.toFixed(3)}%</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  );
                }
                return null;
              }}
            />
            <ReferenceLine y={0} stroke="#94a3b8" strokeWidth={2} />
            <Scatter name="Reconstruction Error" data={filteredData} shape="circle">
              {filteredData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.fillColor} stroke={entry.fillColor} fillOpacity={0.6} />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default ReconstructionChart;
