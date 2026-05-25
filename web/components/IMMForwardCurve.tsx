'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  Legend, ResponsiveContainer,
} from 'recharts';
import type { IMMForwardSnapshot } from '@/lib/api';

interface Props {
  snapshots: IMMForwardSnapshot[];
  tradeDates: string[];
  dateIdx: number;
  tenorMonths: number;
}

const SERIES_CONFIGS = [
  { offset: 0,  color: '#2563eb', width: 2.5, suffix: '' },
  { offset: 20, color: '#dc2626', width: 1.5, suffix: ' (-20D)' },
  { offset: 50, color: '#059669', width: 1.5, suffix: ' (-50D)' },
];

// 0.5bp の倍数ステップのうち約 maxTicks 本になるものを選ぶ
const BP_STEPS = [0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0];
function bpTicks(min: number, max: number, maxTicks = 12): number[] {
  const rawStep = (max - min) / maxTicks;
  const step = BP_STEPS.find(s => s >= rawStep) ?? 1.0;
  const first = Math.ceil(min / step) * step;
  const ticks: number[] = [];
  for (let t = first; t <= max + step * 0.01; t += step) {
    ticks.push(+t.toFixed(4));
  }
  return ticks;
}

function getRate(snap: IMMForwardSnapshot, startCode: string, steps: number): number | null {
  const n = snap.codes.length;
  const i = snap.codes.indexOf(startCode);
  if (i < 0) return null;
  const j = i + steps;
  if (j >= n) return null;
  const k = (n - 1) * i - Math.floor(i * (i - 1) / 2) + (j - i - 1);
  return snap.rates[k];
}

export default function IMMForwardCurve({ snapshots, tradeDates, dateIdx, tenorMonths }: Props) {
  const { data, series } = useMemo(() => {
    const steps = tenorMonths / 3;

    const series = SERIES_CONFIGS
      .map(cfg => ({ ...cfg, idx: dateIdx - cfg.offset }))
      .filter(s => s.idx >= 0 && s.idx < snapshots.length)
      .map(s => ({
        ...s,
        label: tradeDates[s.idx] + s.suffix,
      }));

    const currentSnap = snapshots[dateIdx];
    if (!currentSnap) return { data: [], series };

    const n = currentSnap.codes.length;
    const data = currentSnap.codes.slice(0, n - steps).map(code => {
      const entry: Record<string, string | number | null> = { code };
      for (const s of series) {
        entry[s.label] = getRate(snapshots[s.idx], code, steps);
      }
      return entry;
    });

    return { data, series };
  }, [snapshots, tradeDates, dateIdx, tenorMonths]);

  if (!data.length) return null;

  const allRates = data.flatMap(d => series.map(s => d[s.label])).filter((v): v is number => v !== null);
  const yMin = allRates.length ? Math.min(...allRates) : 0;
  const yMax = allRates.length ? Math.max(...allRates) : 5;
  const pad = Math.max((yMax - yMin) * 0.1, 0.05);
  const ticks = bpTicks(yMin - pad, yMax + pad);

  return (
    <div className="h-[460px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 10, right: 30, left: 20, bottom: 65 }}>
          <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
          <XAxis
            dataKey="code"
            tick={{ fontSize: 10 }}
            angle={-50}
            textAnchor="end"
            interval={1}
            height={70}
            stroke="#64748b"
          />
          <YAxis
            ticks={ticks}
            tickFormatter={v => v.toFixed(3)}
            label={{ value: 'Rate (%)', angle: -90, position: 'insideLeft', offset: 10 }}
            tick={{ fontSize: 10 }}
            domain={[ticks[0] ?? yMin - pad, ticks[ticks.length - 1] ?? yMax + pad]}
            stroke="#64748b"
          />
          <Tooltip
            formatter={(v: any) => [v != null ? `${(v as number).toFixed(4)}%` : '—']}
            labelFormatter={(code: string) => `Start: ${code}`}
          />
          <Legend verticalAlign="top" height={36} />
          {series.map(s => (
            <Line
              key={s.idx}
              type="monotone"
              dataKey={s.label}
              stroke={s.color}
              strokeWidth={s.width}
              dot={false}
              connectNulls={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
