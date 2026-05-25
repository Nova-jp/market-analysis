'use client';

import dynamic from 'next/dynamic';
import { useMemo } from 'react';
import { Loader2 } from 'lucide-react';
import type { IMMForwardSnapshot } from '@/lib/api';

const Plot = dynamic(
  () =>
    Promise.all([
      import('plotly.js-dist-min'),
      import('react-plotly.js/factory'),
    ]).then(([Plotly, { default: createPlotlyComponent }]) =>
      createPlotlyComponent(Plotly as any),
    ),
  {
    ssr: false,
    loading: () => (
      <div className="flex items-center justify-center h-[560px]">
        <Loader2 className="w-8 h-8 animate-spin text-indigo-500" />
      </div>
    ),
  },
);

export type ViewMode = 'rate' | 'zscore' | 'delta' | 'relative';

interface Props {
  snapshot: IMMForwardSnapshot;
  displayValues: (number | null)[];
  viewMode: ViewMode;
  tradeDate: string;
  chartTitle?: string;
  colorbarLabel?: string;
}

// Excel 配色: 赤(低) → 白(中) → 青(高)
const COLORSCALE: [number, string][] = [
  [0,    '#d73027'],
  [0.25, '#fc8d59'],
  [0.5,  '#ffffff'],
  [0.75, '#74add1'],
  [1,    '#0070c0'],
];

const BP_STEPS = [0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0];

function pickDtick(range: number, maxTicks = 15): number {
  const rawStep = range / maxTicks;
  return BP_STEPS.find(s => s >= rawStep) ?? 1.0;
}

export default function IMMForwardHeatmap({
  snapshot, displayValues, viewMode, tradeDate, chartTitle, colorbarLabel,
}: Props) {
  const { codes, imm_dates, rates } = snapshot;
  const n = codes.length;

  const { z, text, zmin, zmax, dtick } = useMemo(() => {
    const z: (number | null)[][] = Array.from({ length: n }, () => new Array(n).fill(null));
    const text: string[][] = Array.from({ length: n }, () => new Array(n).fill(''));

    let k = 0;
    for (let i = 0; i < n; i++) {
      for (let j = i + 1; j < n; j++) {
        const dv = displayValues[k];
        const rv = rates[k];
        z[i][j] = dv;

        const tenor = (j - i) * 3;
        const tenorLabel = tenor >= 12 ? `${tenor / 12}Y` : `${tenor}M`;
        const rateStr = rv !== null ? `Rate: ${rv.toFixed(4)}%` : '';
        const displayStr =
          dv !== null
            ? viewMode === 'zscore' || viewMode === 'relative'
              ? `Z-score: ${dv.toFixed(2)}σ`
              : viewMode === 'delta'
              ? `Δ: ${dv > 0 ? '+' : ''}${dv.toFixed(2)}bps`
              : colorbarLabel
              ? `Value: ${dv > 0 ? '+' : ''}${dv.toFixed(3)}bps`
              : `Rate: ${dv.toFixed(4)}%`
            : '—';

        text[i][j] = [
          `<b>${codes[i]} → ${codes[j]}</b>`,
          `Start: ${imm_dates[i]}`,
          `End:   ${imm_dates[j]}`,
          `Tenor: ${tenorLabel}`,
          viewMode !== 'rate' ? rateStr : '',
          displayStr,
        ]
          .filter(Boolean)
          .join('<br>');
        k++;
      }
    }

    const vals = displayValues.filter((v): v is number => v !== null);

    let zmin: number | undefined;
    let zmax: number | undefined;
    let dtick: number | undefined;

    if (viewMode === 'zscore' || viewMode === 'relative') {
      zmin = -3; zmax = 3; dtick = 0.5;
    } else if (viewMode === 'delta') {
      const maxAbs = vals.length ? Math.max(...vals.map(Math.abs)) : 10;
      zmin = -maxAbs; zmax = maxAbs; dtick = 0.5;
    } else {
      // rate or bps (butterfly)
      if (vals.length) {
        const range = Math.max(...vals) - Math.min(...vals);
        dtick = pickDtick(range);
      }
    }

    return { z, text, zmin, zmax, dtick };
  }, [displayValues, viewMode, codes, imm_dates, rates, n, colorbarLabel]);

  const cbTitle =
    colorbarLabel ??
    (viewMode === 'zscore'   ? 'Z-score (σ)' :
     viewMode === 'relative' ? '相対Z (σ)'   :
     viewMode === 'delta'    ? 'Δ (bps)'      :
                               'Rate (%)');

  const title = chartTitle ?? `IMM Forward Matrix — ${tradeDate}`;

  return (
    <Plot
      data={[
        {
          type: 'heatmap',
          x: codes,
          y: codes,
          z: z as any,
          text: text as any,
          hovertemplate: '%{text}<extra></extra>',
          colorscale: COLORSCALE,
          zmin,
          zmax,
          colorbar: {
            title: { text: cbTitle, side: 'right' },
            thickness: 14,
            len: 0.85,
            dtick,
          },
          xgap: 1,
          ygap: 1,
        },
      ]}
      layout={{
        title: { text: title, font: { size: 14, color: '#1e293b' }, x: 0.5 },
        height: 580,
        margin: { t: 50, b: 110, l: 80, r: 80 },
        xaxis: {
          title: { text: 'End IMM', standoff: 10 },
          tickangle: -50,
          tickfont: { size: 9 },
          side: 'bottom',
        },
        yaxis: {
          title: { text: 'Start IMM', standoff: 10 },
          tickfont: { size: 9 },
          autorange: 'reversed',
        },
        plot_bgcolor: '#f8fafc',
        paper_bgcolor: 'white',
      }}
      config={{ responsive: true, displayModeBar: true, displaylogo: false }}
      style={{ width: '100%' }}
    />
  );
}
