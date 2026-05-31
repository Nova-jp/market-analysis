'use client';

import dynamic from 'next/dynamic';
import { useMemo } from 'react';
import { Loader2 } from 'lucide-react';
import type { InstantaneousForwardPoint } from '@/lib/api';

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
      <div className="flex items-center justify-center h-[500px]">
        <Loader2 className="w-8 h-8 animate-spin text-indigo-500" />
      </div>
    ),
  },
);

const PALETTE = [
  '#2563eb', '#dc2626', '#059669', '#d97706',
  '#7c3aed', '#db2777', '#0891b2', '#4f46e5',
  '#ea580c', '#1e293b',
];

export interface FwdDataset {
  date: string;
  data: InstantaneousForwardPoint[];
  color: string;
}

interface Props {
  datasets: FwdDataset[];
  minMaturity: number;
  maxMaturity: number;
}

export default function InstantaneousFwdChart({ datasets, minMaturity, maxMaturity }: Props) {
  const traces = useMemo(() => {
    const result: any[] = [];
    datasets.forEach((ds, idx) => {
      const color = PALETTE[idx % PALETTE.length];
      const filtered = ds.data.filter(
        (p) => p.maturity_years >= minMaturity && p.maturity_years <= maxMaturity,
      );
      const x = filtered.map((p) => p.maturity_years);

      result.push({
        x,
        y: filtered.map((p) => p.forward_rate),
        type: 'scatter',
        mode: 'lines',
        name: `${ds.date} Inst.Fwd`,
        line: { color, width: 2, dash: 'solid' },
        hovertemplate: '%{x:.2f}Y  %{y:.4f}%<extra>%{fullData.name}</extra>',
      });

      result.push({
        x,
        y: filtered.map((p) => p.zero_rate),
        type: 'scatter',
        mode: 'lines',
        name: `${ds.date} Zero`,
        line: { color, width: 1.5, dash: 'dash' },
        hovertemplate: '%{x:.2f}Y  %{y:.4f}%<extra>%{fullData.name}</extra>',
      });
    });
    return result;
  }, [datasets, minMaturity, maxMaturity]);

  const layout = useMemo(
    () => ({
      margin: { t: 24, r: 24, b: 48, l: 56 },
      xaxis: {
        title: { text: '残存年数 (Y)', font: { size: 12 } },
        range: [minMaturity, maxMaturity],
        gridcolor: '#e2e8f0',
        zeroline: false,
      },
      yaxis: {
        title: { text: 'レート (%)', font: { size: 12 } },
        gridcolor: '#e2e8f0',
        zeroline: false,
        tickformat: '.3f',
      },
      legend: {
        orientation: 'h' as const,
        y: -0.2,
        x: 0,
        font: { size: 11 },
      },
      plot_bgcolor: '#ffffff',
      paper_bgcolor: '#ffffff',
      hovermode: 'x unified' as const,
      font: { family: 'Inter, sans-serif', size: 12, color: '#334155' },
    }),
    [minMaturity, maxMaturity],
  );

  if (datasets.length === 0) {
    return (
      <div className="flex items-center justify-center h-[500px] text-slate-400 text-sm">
        日付を追加するとグラフが表示されます
      </div>
    );
  }

  return (
    <Plot
      data={traces}
      layout={layout}
      config={{ displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d'], responsive: true }}
      style={{ width: '100%', height: '500px' }}
    />
  );
}
