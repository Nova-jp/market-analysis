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
  TooltipProps,
} from 'recharts';
import { BondYieldData } from '@/lib/api';

interface YieldDataset {
  date: string;
  data: BondYieldData[];
  color: string;
}

interface YieldCurveChartProps {
  datasets: YieldDataset[];
  minMaturity?: number;
  maxMaturity?: number;
}

// カスタムツールチップ
const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-white/95 backdrop-blur-sm p-4 border border-slate-200 rounded-xl shadow-xl text-sm max-w-[300px]">
        <p className="font-bold text-slate-700 mb-2 border-b border-slate-100 pb-1">
          Maturity: {Number(label).toFixed(3)} Years
        </p>
        <div className="space-y-3">
          {payload.map((entry: any, index: number) => {
            const dateKey = entry.dataKey;
            const bondName = entry.payload[`${dateKey}_name`];
            
            return (
              <div key={index} className="flex flex-col gap-0.5">
                <div className="flex items-center justify-between gap-4 font-bold" style={{ color: entry.color }}>
                  <span>{bondName || entry.name}</span>
                  <span>{entry.value?.toFixed(3)}%</span>
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

const YieldCurveChart = ({ datasets, minMaturity = 0, maxMaturity = 40 }: YieldCurveChartProps) => {
  if (!datasets || datasets.length === 0) return null;

  // 1. 全てのユニークなMaturityを収集
  const allMaturities = new Set<number>();
  datasets.forEach(ds => {
    ds.data.forEach(point => {
        // 表示範囲内のデータのみをチャートデータに含める（またはXAxis domainで制御も可だが、データ量削減のためここでフィルタも有効）
        // ここではRechartsのdomain制御に任せるため全データを入れるが、あまりに遠いデータはノイズになるので本来はフィルタ推奨
        allMaturities.add(point.maturity);
    });
  });
  
  const sortedMaturities = Array.from(allMaturities)
    .filter(mat => mat >= minMaturity && mat <= maxMaturity)
    .sort((a, b) => a - b);

  // 2. Recharts用のデータ構造に変換
  const chartData = sortedMaturities.map(mat => {
    const point: any = { maturity: mat };
    
    datasets.forEach(ds => {
      const match = ds.data.find(d => Math.abs(d.maturity - mat) < 0.001);
      if (match) {
        point[ds.date] = match.yield;
        point[`${ds.date}_name`] = match.bond_name;
      } else {
        point[ds.date] = null;
        point[`${ds.date}_name`] = null;
      }
    });
    
    return point;
  });

  // 3. 表示データの範囲に合わせてY軸のスケールを手動計算
  const yValues = chartData.flatMap(point => 
    datasets.map(ds => point[ds.date]).filter(v => v !== null && typeof v === 'number')
  );
  
  let yDomain: [number | string, number | string] = ['auto', 'auto'];
  if (yValues.length > 0) {
    const min = Math.min(...yValues);
    const max = Math.max(...yValues);
    const padding = (max - min) * 0.1 || 0.01; // 10%のバッファ
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
            label={{ value: 'Maturity (Years)', position: 'insideBottomRight', offset: -10 }}
            type="number"
            domain={[minMaturity, maxMaturity]} // ここでズーム制御
            allowDataOverflow={true} // 範囲外のデータを隠す
            tick={{ fontSize: 12 }}
            stroke="#64748b"
            tickCount={10} // 目盛りの数を適度に維持
          />
          <YAxis 
            label={{ value: 'Yield (%)', angle: -90, position: 'insideLeft', offset: 0 }}
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
              key={ds.date}
              type="monotone"
              dataKey={ds.date}
              name={`JGB ${ds.date}`}
              stroke={ds.color}
              strokeWidth={2}
              dot={{ r: 3, fill: ds.color }}
              activeDot={{ r: 6, strokeWidth: 0 }}
              connectNulls
              animationDuration={500} // アニメーションを少し速くしてズーム時のレスポンス向上
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export default YieldCurveChart;
