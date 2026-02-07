'use client';

import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts';
import { PCAComponent } from '@/lib/api';

interface LoadingsChartProps {
  components: PCAComponent[];
  maturities: number[];
}

const COLORS = ['#2563eb', '#dc2626', '#16a34a', '#d97706', '#9333ea'];
const NAMES = ['Level (PC1)', 'Slope (PC2)', 'Curvature (PC3)', 'PC4', 'PC5'];

const LoadingsChart = ({ components, maturities }: LoadingsChartProps) => {
  // データをRecharts用に変換
  const data = maturities.map((maturity, idx) => {
    const point: any = { maturity };
    components.forEach((comp, compIdx) => {
      point[`pc${comp.pc_number}`] = comp.loadings[idx];
    });
    return point;
  });

  return (
    <div className="h-[350px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 5, right: 30, left: 20, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" vertical={false} />
          <XAxis 
            dataKey="maturity" 
            label={{ value: 'Maturity (Years)', position: 'insideBottomRight', offset: -10 }}
            type="number"
          />
          <YAxis 
            label={{ value: 'Loading', angle: -90, position: 'insideLeft' }} 
            tick={{ fontSize: 12 }} 
            stroke="#64748b" 
            domain={['auto', 'auto']}
          />
          <Tooltip 
            content={({ active, payload, label }) => {
              if (active && payload && payload.length) {
                return (
                  <div className="bg-white/95 backdrop-blur-sm p-4 border border-slate-200 rounded-xl shadow-xl text-sm">
                    <p className="font-bold text-slate-700 mb-2 border-b border-slate-100 pb-1">
                      Maturity: {label} Years
                    </p>
                    <div className="space-y-1">
                      {payload.map((entry: any, index: number) => (
                        <div key={index} className="flex items-center justify-between gap-4 font-bold" style={{ color: entry.color }}>
                          <span>{entry.name}</span>
                          <span>{entry.value?.toFixed(4)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                );
              }
              return null;
            }}
          />
          <Legend verticalAlign="top" height={36} iconType="circle" />
          <ReferenceLine y={0} stroke="#666" strokeDasharray="3 3" />
          
          {components.map((comp, idx) => (
            <Line
              key={comp.pc_number}
              type="monotone"
              dataKey={`pc${comp.pc_number}`}
              name={NAMES[idx] || `PC${comp.pc_number}`}
              stroke={COLORS[idx % COLORS.length]}
              dot={false}
              strokeWidth={2}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export default LoadingsChart;
