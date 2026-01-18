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
import { PCAScore } from '@/lib/api';

interface ScoreChartProps {
  scores: PCAScore[];
  componentsCount: number;
}

const COLORS = ['#2563eb', '#dc2626', '#16a34a', '#d97706', '#9333ea'];
const NAMES = ['Level (PC1)', 'Slope (PC2)', 'Curvature (PC3)', 'PC4', 'PC5'];

const ScoreChart = ({ scores, componentsCount }: ScoreChartProps) => {
  return (
    <div className="h-[350px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={scores} margin={{ top: 5, right: 30, left: 20, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" vertical={false} />
          <XAxis 
            dataKey="date" 
            tickFormatter={(date) => date.slice(5)} // MM-DD
            minTickGap={30}
          />
          <YAxis label={{ value: 'Score', angle: -90, position: 'insideLeft' }} tick={{ fontSize: 12 }} stroke="#64748b" />
          <Tooltip 
            content={({ active, payload, label }) => {
              if (active && payload && payload.length) {
                return (
                  <div className="bg-white/95 backdrop-blur-sm p-4 border border-slate-200 rounded-xl shadow-xl text-sm">
                    <p className="font-bold text-slate-700 mb-2 border-b border-slate-100 pb-1">
                      Date: {label}
                    </p>
                    <div className="space-y-1">
                      {payload.map((entry: any, index: number) => (
                        <div key={index} className="flex items-center justify-between gap-4 font-bold" style={{ color: entry.color }}>
                          <span>{entry.name}</span>
                          <span>{entry.value?.toFixed(2)}</span>
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
          
          {Array.from({ length: componentsCount }).map((_, idx) => (
            <Line
              key={idx}
              type="monotone"
              dataKey={`pc${idx + 1}`}
              name={NAMES[idx] || `PC${idx + 1}`}
              stroke={COLORS[idx % COLORS.length]}
              dot={false}
              strokeWidth={1.5}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export default ScoreChart;
