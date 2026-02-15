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
} from 'recharts';
import { ForwardRateData } from '@/lib/api';

interface ForwardDataset {
  date: string;
  type: string;
  parameter: string;
  data: ForwardRateData[];
  color: string;
}

interface ForwardCurveChartProps {
  datasets: ForwardDataset[];
  minMaturity?: number;
  maxMaturity?: number;
}

const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-white/95 backdrop-blur-sm p-4 border border-slate-200 rounded-xl shadow-xl text-sm max-w-[300px]">
        <p className="font-bold text-slate-700 mb-2 border-b border-slate-100 pb-1">
          X-Axis Value: {Number(label).toFixed(2)}
        </p>
        <div className="space-y-3">
          {payload.map((entry: any, index: number) => {
            const dataPoint = entry.payload;
            const startTenor = dataPoint[`${entry.dataKey}_start`];
            const swapTenor = dataPoint[`${entry.dataKey}_swap`];
            
            return (
              <div key={index} className="flex flex-col gap-0.5">
                <div className="flex items-center justify-between gap-4 font-bold" style={{ color: entry.color }}>
                  <span>{entry.name}</span>
                  <span>{entry.value?.toFixed(4)}%</span>
                </div>
                <div className="text-xs text-slate-500 font-medium">
                  {startTenor} x {swapTenor} Forward
                </div>
              </div>
            );
          })}
        </div>
      </div>
    );
  }
  return null;
};

const ForwardCurveChart = ({ datasets, minMaturity = 0, maxMaturity = 40 }: ForwardCurveChartProps) => {
  if (!datasets || datasets.length === 0) return null;

  // 全てのユニークなMaturityを収集
  const allMaturities = new Set<number>();
  datasets.forEach(ds => {
    ds.data.forEach(point => {
        allMaturities.add(point.maturity);
    });
  });
  
  const sortedMaturities = Array.from(allMaturities)
    .filter(mat => mat >= minMaturity && mat <= maxMaturity)
    .sort((a, b) => a - b);

  // Recharts用のデータ構造に変換
  const chartData = sortedMaturities.map(mat => {
    const point: any = { maturity: mat };
    
    datasets.forEach(ds => {
      const match = ds.data.find(d => Math.abs(d.maturity - mat) < 0.001);
      if (match) {
        point[ds.date + ds.parameter] = match.rate;
        point[`${ds.date + ds.parameter}_start`] = match.start_tenor;
        point[`${ds.date + ds.parameter}_swap`] = match.swap_tenor;
      } else {
        point[ds.date + ds.parameter] = null;
      }
    });
    
    return point;
  });

  const yValues = chartData.flatMap(point => 
    datasets.map(ds => point[ds.date + ds.parameter]).filter(v => v !== null && typeof v === 'number')
  );
  
  let yDomain: [number | string, number | string] = ['auto', 'auto'];
  if (yValues.length > 0) {
    const min = Math.min(...yValues);
    const max = Math.max(...yValues);
    const padding = (max - min) * 0.1 || 0.01;
    yDomain = [Number((min - padding).toFixed(4)), Number((max + padding).toFixed(4))];
  }

  return (
    <div className="h-[550px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart
          data={chartData}
          margin={{ top: 10, right: 30, left: 20, bottom: 25 }}
        >
          <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
          <XAxis 
            dataKey="maturity" 
            type="number"
            domain={[minMaturity, maxMaturity]}
            allowDataOverflow={true}
            tick={{ fontSize: 12 }}
            stroke="#64748b"
            tickCount={10}
          />
          <YAxis 
            label={{ value: 'Rate (%)', angle: -90, position: 'insideLeft', offset: 0 }}
            tick={{ fontSize: 12 }}
            domain={yDomain}
            stroke="#64748b"
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend 
            verticalAlign="top" 
            height={40}
            wrapperStyle={{ paddingBottom: '20px' }}
          />
          
          {datasets.map((ds) => (
            <Line
              key={ds.date + ds.parameter}
              type="monotone"
              dataKey={ds.date + ds.parameter}
              name={`${ds.date} (${ds.parameter})`}
              stroke={ds.color}
              strokeWidth={2}
              dot={{ r: 4, fill: ds.color }}
              activeDot={{ r: 7, strokeWidth: 0 }}
              connectNulls
              animationDuration={500}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export default ForwardCurveChart;
