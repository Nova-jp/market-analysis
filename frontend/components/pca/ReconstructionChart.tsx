'use client';

import React from 'react';
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

interface ReconstructionChartProps {
  data: ReconstructionDataPoint[];
}

// 期間に応じた色を生成する関数
const getColorForMaturity = (maturity: number): string => {
  if (maturity < 2) return '#3b82f6'; // blue-500
  if (maturity < 5) return '#0ea5e9'; // sky-500
  if (maturity < 10) return '#10b981'; // emerald-500
  if (maturity < 20) return '#eab308'; // yellow-500
  if (maturity < 30) return '#f97316'; // orange-500
  return '#ef4444'; // red-500
};

const ReconstructionChart = ({ data }: ReconstructionChartProps) => {
  // bps単位に変換して表示する場合
  const chartData = data.map(d => ({
    ...d,
    errorBps: d.error * 100 // % -> bps (%単位の数値 0.01 = 1bp の場合)
  }));

  return (
    <div className="h-[400px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 10 }}>
          <CartesianGrid strokeDasharray="3 3" vertical={false} />
          <XAxis 
            type="number" 
            dataKey="maturity" 
            name="Maturity" 
            unit="Y" 
            label={{ value: 'Maturity (Years)', position: 'insideBottom', offset: -10 }}
            tick={{ fontSize: 12 }}
          />
          <YAxis 
            type="number" 
            dataKey="errorBps" 
            name="Error" 
            unit=" bps" 
            label={{ value: 'Reconstruction Error (bps)', angle: -90, position: 'insideLeft', offset: 0 }}
            tick={{ fontSize: 12 }}
          />
          <Tooltip 
            cursor={{ strokeDasharray: '3 3' }}
            content={({ active, payload }) => {
              if (active && payload && payload.length) {
                const d = payload[0].payload;
                return (
                  <div className="bg-white/95 backdrop-blur-sm p-3 border border-slate-200 rounded-xl shadow-xl text-sm">
                    <p className="font-bold text-slate-700 border-b border-slate-100 pb-1 mb-2">
                      {d.bond_name || d.bond_code}
                    </p>
                    <div className="space-y-1">
                      <div className="flex justify-between gap-4">
                        <span className="text-slate-500">Maturity:</span>
                        <span className="font-mono font-bold">{d.maturity.toFixed(2)}Y</span>
                      </div>
                      <div className="flex justify-between gap-4">
                        <span className="text-slate-500">Error:</span>
                        <span className={`font-mono font-bold ${d.errorBps > 0 ? 'text-red-500' : 'text-blue-500'}`}>
                          {d.errorBps.toFixed(2)} bps
                        </span>
                      </div>
                      <div className="flex justify-between gap-4 text-xs text-slate-400 mt-1 pt-1 border-t border-slate-100">
                        <span>Original: {d.original_yield.toFixed(3)}%</span>
                        <span>Fitted: {d.reconstructed_yield.toFixed(3)}%</span>
                      </div>
                    </div>
                  </div>
                );
              }
              return null;
            }}
          />
          <ReferenceLine y={0} stroke="#64748b" />
          <Scatter name="Reconstruction Error" data={chartData} fill="#8884d8">
            {chartData.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={getColorForMaturity(entry.maturity)} />
            ))}
          </Scatter>
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
};

export default ReconstructionChart;
