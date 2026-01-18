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
import { BondYieldData } from '@/lib/api';

interface YieldCurveChartProps {
  data: BondYieldData[];
  name: string;
  color?: string;
}

const YieldCurveChart = ({ data, name, color = "#2563eb" }: YieldCurveChartProps) => {
  return (
    <div className="h-[400px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart
          data={data}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 20,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" vertical={false} />
          <XAxis 
            dataKey="maturity" 
            label={{ value: 'Maturity (Years)', position: 'insideBottomRight', offset: -10 }}
            type="number"
            domain={[0, 'dataMax']}
            tick={{ fontSize: 12 }}
          />
          <YAxis 
            label={{ value: 'Yield (%)', angle: -90, position: 'insideLeft', offset: 0 }}
            tick={{ fontSize: 12 }}
            domain={['auto', 'auto']}
          />
          <Tooltip 
            formatter={(value: number) => [value.toFixed(3) + '%', 'Yield']}
            labelFormatter={(label) => `Maturity: ${label}Y`}
            contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
          />
          <Legend verticalAlign="top" height={36}/>
          <Line
            type="monotone"
            dataKey="yield_rate"
            stroke={color}
            name={name}
            dot={{ r: 3, fill: color }}
            activeDot={{ r: 5 }}
            strokeWidth={2}
            animationDuration={1000}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export default YieldCurveChart;
