'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from 'recharts';
import type { IMMForwardSnapshot } from '@/lib/api';

// ── 公開型 ──────────────────────────────────────────────────────

export interface SeriesConfig {
  id: string;
  /** rate: フォワードレート(%)  fly1y/fly2y: バタフライ(bps)  spread: レート差(bps) */
  type: 'rate' | 'fly1y' | 'fly2y' | 'spread';
  startCode: string;
  endCode: string;
  startCode2?: string; // spread 用
  endCode2?: string;   // spread 用
  color: string;
  label: string;
}

interface Props {
  snapshots: IMMForwardSnapshot[];
  tradeDates: string[];
  series: SeriesConfig[];
  zWindow: number;
  showZScore: boolean;
  showBands: boolean;
}

// ── ユーティリティ ───────────────────────────────────────────────

function flatIdx(i: number, j: number, n: number) {
  return (n - 1) * i - Math.floor(i * (i - 1) / 2) + (j - i - 1);
}

function lookupRate(snap: IMMForwardSnapshot, s: string, e: string): number | null {
  const n = snap.codes.length;
  const i = snap.codes.indexOf(s);
  const j = snap.codes.indexOf(e);
  if (i < 0 || j < 0 || j <= i) return null;
  return snap.rates[flatIdx(i, j, n)];
}

function lookupFly(snap: IMMForwardSnapshot, s: string, e: string, wings: number): number | null {
  const n = snap.codes.length;
  const i = snap.codes.indexOf(s);
  const j = snap.codes.indexOf(e);
  if (i < 0 || j < 0 || j <= i) return null;
  const nearJ = j - wings;
  const farJ  = j + wings;
  if (nearJ <= i || farJ >= n) return null;
  const b  = snap.rates[flatIdx(i, j,     n)];
  const nr = snap.rates[flatIdx(i, nearJ, n)];
  const fr = snap.rates[flatIdx(i, farJ,  n)];
  return b !== null && nr !== null && fr !== null ? (2 * b - nr - fr) * 100 : null;
}

function getValue(snap: IMMForwardSnapshot, s: SeriesConfig): number | null {
  if (s.type === 'rate')  return lookupRate(snap, s.startCode, s.endCode);
  if (s.type === 'fly1y') return lookupFly(snap, s.startCode, s.endCode, 4);
  if (s.type === 'fly2y') return lookupFly(snap, s.startCode, s.endCode, 8);
  if (s.type === 'spread' && s.startCode2 && s.endCode2) {
    const r1 = lookupRate(snap, s.startCode,  s.endCode);
    const r2 = lookupRate(snap, s.startCode2, s.endCode2);
    return r1 !== null && r2 !== null ? (r1 - r2) * 100 : null;
  }
  return null;
}

// 0.5bp の倍数刻み (% 単位入力)
const PCT_STEPS  = [0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0];
// 0.5bps 刻み (bps 単位入力)
const BPS_STEPS  = [0.5, 1, 2, 5, 10, 20, 50, 100];

function makeTicks(steps: number[], min: number, max: number, maxTicks = 12): number[] {
  if (min >= max) return [min];
  const rawStep = (max - min) / maxTicks;
  const step = steps.find(s => s >= rawStep) ?? steps[steps.length - 1];
  const first = Math.ceil(min / step) * step;
  const ticks: number[] = [];
  for (let t = first; t <= max + step * 0.01; t += step) {
    ticks.push(+t.toFixed(5));
  }
  return ticks;
}

// ── コンポーネント ────────────────────────────────────────────────

export default function IMMForwardTimeseries({
  snapshots, tradeDates, series, zWindow, showZScore, showBands,
}: Props) {
  // Step1: 全期間の生値を先に計算（Z-score の分母用）
  const rawMatrix = useMemo(
    () => series.map(s => snapshots.map(snap => getValue(snap, s))),
    [series, snapshots],
  );

  const chartData = useMemo(() => {
    return tradeDates.map((date, idx) => {
      const pt: Record<string, string | number | null> = { date };

      for (let si = 0; si < series.length; si++) {
        const raw = rawMatrix[si][idx];

        if (!showZScore) {
          pt[series[si].id] = raw;
        } else {
          const hist = rawMatrix[si]
            .slice(Math.max(0, idx - zWindow), idx)
            .filter((v): v is number => v !== null);
          if (hist.length >= 5 && raw !== null) {
            const m   = hist.reduce((a, b) => a + b, 0) / hist.length;
            const std = Math.sqrt(hist.reduce((a, b) => a + (b - m) ** 2, 0) / hist.length);
            pt[series[si].id] = std < 1e-8 ? 0 : (raw - m) / std;
          } else {
            pt[series[si].id] = null;
          }
        }
      }

      // σ バンド: 1系列 & Raw モードのみ
      if (showBands && !showZScore && series.length === 1) {
        const raw  = rawMatrix[0][idx];
        const hist = rawMatrix[0]
          .slice(Math.max(0, idx - zWindow), idx)
          .filter((v): v is number => v !== null);
        if (hist.length >= 5 && raw !== null) {
          const m   = hist.reduce((a, b) => a + b, 0) / hist.length;
          const std = Math.sqrt(hist.reduce((a, b) => a + (b - m) ** 2, 0) / hist.length);
          pt['__mean'] = m;
          pt['__hi1']  = m + std;
          pt['__lo1']  = m - std;
          pt['__hi2']  = m + 2 * std;
          pt['__lo2']  = m - 2 * std;
        }
      }

      return pt;
    });
  }, [tradeDates, series, rawMatrix, zWindow, showZScore, showBands]);

  // 表示値が全て null かチェック
  const hasData = rawMatrix.some(row => row.some(v => v !== null));
  if (!hasData) {
    return <p className="text-center text-slate-500 py-16">このペアのデータがありません。</p>;
  }

  // Y 軸の型判定
  const allRate = !showZScore && series.every(s => s.type === 'rate');
  const allBps  = !showZScore && series.every(s => s.type !== 'rate');

  // Y 軸の全有効値
  const allVals = chartData.flatMap(pt =>
    [...series.map(s => pt[s.id]), pt['__hi2'], pt['__lo2']],
  ).filter((v): v is number => v !== null);

  const yMin = allVals.length ? Math.min(...allVals) : -3;
  const yMax = allVals.length ? Math.max(...allVals) :  3;
  const pad  = Math.max((yMax - yMin) * 0.08, allRate ? 0.005 : allBps ? 0.5 : 0.1);

  const ticks =
    showZScore ? [-3, -2, -1, 0, 1, 2, 3].filter(v => v >= yMin - 0.5 && v <= yMax + 0.5) :
    allRate    ? makeTicks(PCT_STEPS, yMin - pad, yMax + pad) :
    allBps     ? makeTicks(BPS_STEPS, yMin - pad, yMax + pad) :
                 makeTicks(PCT_STEPS, yMin - pad, yMax + pad);

  const yUnit =
    showZScore ? 'σ' :
    allRate    ? '%' :
    allBps     ? 'bps' :
                 '';

  const fmt = showZScore ? (v: number) => v.toFixed(2) :
              allBps      ? (v: number) => v.toFixed(2) :
                            (v: number) => v.toFixed(3);

  const tooltipFmt = (v: any, name: string): [string, string] => {
    if (v == null) return ['—', name];
    const s = series.find(s => s.id === name);
    const unit = showZScore ? 'σ' : (s?.type === 'rate' ? '%' : 'bps');
    return [`${(v as number).toFixed(showZScore ? 2 : s?.type === 'rate' ? 4 : 3)}${unit}`, name];
  };

  const xInterval = Math.max(1, Math.floor(tradeDates.length / 8));

  return (
    <div className="h-[480px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData} margin={{ top: 10, right: 30, left: 20, bottom: 65 }}>
          <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 10 }}
            angle={-45}
            textAnchor="end"
            interval={xInterval}
            height={70}
            stroke="#64748b"
          />
          <YAxis
            ticks={ticks}
            tickFormatter={fmt}
            label={{ value: yUnit, angle: -90, position: 'insideLeft', offset: 10 }}
            tick={{ fontSize: 10 }}
            domain={[ticks[0] ?? yMin - pad, ticks[ticks.length - 1] ?? yMax + pad]}
            stroke="#64748b"
          />
          <Tooltip formatter={tooltipFmt as any} />
          <Legend verticalAlign="top" height={40} />

          {/* σ バンド (1 系列 Raw モード時) */}
          {showBands && !showZScore && series.length === 1 && <>
            <Line type="monotone" dataKey="__hi2"  stroke="#c4b5fd" strokeWidth={0.8} dot={false} strokeDasharray="4 4" name="+2σ" connectNulls legendType="none" />
            <Line type="monotone" dataKey="__hi1"  stroke="#818cf8" strokeWidth={0.8} dot={false} strokeDasharray="4 4" name="+1σ" connectNulls legendType="none" />
            <Line type="monotone" dataKey="__mean" stroke="#94a3b8" strokeWidth={1}   dot={false} strokeDasharray="6 3" name="Mean" connectNulls legendType="none" />
            <Line type="monotone" dataKey="__lo1"  stroke="#818cf8" strokeWidth={0.8} dot={false} strokeDasharray="4 4" name="-1σ" connectNulls legendType="none" />
            <Line type="monotone" dataKey="__lo2"  stroke="#c4b5fd" strokeWidth={0.8} dot={false} strokeDasharray="4 4" name="-2σ" connectNulls legendType="none" />
          </>}

          {/* データ系列 */}
          {series.map(s => (
            <Line
              key={s.id}
              type="monotone"
              dataKey={s.id}
              name={s.label}
              stroke={s.color}
              strokeWidth={2}
              dot={false}
              connectNulls={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
