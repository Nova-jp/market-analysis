'use client';

import { useState } from 'react';
import Link from 'next/link';
import { ArrowLeft, FileSpreadsheet, Download, Loader2, CheckCircle, AlertCircle } from 'lucide-react';

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? '';

type DownloadState = 'idle' | 'loading' | 'done' | 'error';

function DownloadButton({
  label,
  url,
  filename,
}: {
  label: string;
  url: string;
  filename: string;
}) {
  const [state, setState] = useState<DownloadState>('idle');
  const [errorMsg, setErrorMsg] = useState('');

  async function handleDownload() {
    setState('loading');
    setErrorMsg('');
    try {
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`${res.status}: ${text}`);
      }
      const blob = await res.blob();
      const href = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = href;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(href);
      setState('done');
      setTimeout(() => setState('idle'), 3000);
    } catch (e) {
      setErrorMsg(e instanceof Error ? e.message : String(e));
      setState('error');
    }
  }

  return (
    <div className="flex flex-col gap-2">
      <button
        onClick={handleDownload}
        disabled={state === 'loading'}
        className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-semibold
          bg-teal-600 text-white hover:bg-teal-700 disabled:opacity-60 disabled:cursor-not-allowed
          transition-colors shadow-sm"
      >
        {state === 'loading' ? (
          <Loader2 className="w-4 h-4 animate-spin" />
        ) : state === 'done' ? (
          <CheckCircle className="w-4 h-4" />
        ) : (
          <Download className="w-4 h-4" />
        )}
        {state === 'loading' ? '生成中...' : state === 'done' ? 'ダウンロード完了' : label}
      </button>
      {state === 'error' && (
        <div className="flex items-start gap-1.5 text-xs text-red-600">
          <AlertCircle className="w-3.5 h-3.5 mt-0.5 shrink-0" />
          <span>{errorMsg}</span>
        </div>
      )}
    </div>
  );
}

interface ExportCard {
  title: string;
  description: string;
  note: string;
  sheets: { name: string; description: string }[];
  endpoint: string;
  defaultLookback: number;
  filenamePrefix: string;
}

const EXPORT_CARDS: ExportCard[] = [
  {
    title: 'ASW Matrix',
    description: 'ASW・TONA OIS・フォワードレートの統合分析シート',
    note: '計算量が多いため生成に数十秒かかる場合があります（QuantLib）',
    sheets: [
      { name: 'ASW_SASA_5Y',   description: '残存5年以下クーポン国債の SASA ASW [bps]' },
      { name: 'TONA_HIST_1Y',  description: 'TONA OIS スワップ金利履歴 [%]' },
      { name: 'TONA_FWD_3M',   description: 'キャリーロール: 3M×(N-3M) fwd − Spot(N) [bps]' },
      { name: 'TONA_INST_FWD', description: '瞬間フォワード: (N-3M)×3M fwd レート [%]' },
      { name: 'TONA_IMM_3M',   description: 'IMMストリップ: 各IMM日付開始 3M OIS fwd [%]' },
      { name: 'TONA_IMM_1Y',   description: 'IMMストリップ: 各IMM日付開始 1Y OIS fwd [%]' },
    ],
    endpoint: '/api/export/asw-matrix',
    defaultLookback: 365,
    filenamePrefix: 'market_data',
  },
  {
    title: 'IMM Rates',
    description: 'IMM日付ストリップの OIS フォワードレート（40年分 ≈ 160列）',
    note: '列: IMMコード（U26, Z26, H27...）、行2: 実際の第3水曜日の日付',
    sheets: [
      { name: 'IMM_SPOT_OIS', description: 'スポット日から各IMM日付までの OIS パースワップレート [%]' },
    ],
    endpoint: '/api/export/imm-rates',
    defaultLookback: 200,
    filenamePrefix: 'imm_rates',
  },
];

export default function ExportPage() {
  const [lookbacks, setLookbacks] = useState<Record<string, number>>(
    Object.fromEntries(EXPORT_CARDS.map((c) => [c.title, c.defaultLookback]))
  );

  const today = new Date().toISOString().slice(0, 10).replace(/-/g, '');

  return (
    <main className="min-h-screen bg-slate-50">
      {/* Header */}
      <div className="bg-white border-b border-slate-200">
        <div className="container mx-auto px-6 py-5 flex items-center gap-4">
          <Link
            href="/"
            className="p-2 rounded-lg text-slate-500 hover:bg-slate-100 hover:text-slate-800 transition-colors"
          >
            <ArrowLeft className="w-5 h-5" />
          </Link>
          <div className="flex items-center gap-3">
            <div className="p-2 bg-teal-50 rounded-xl">
              <FileSpreadsheet className="w-6 h-6 text-teal-600" />
            </div>
            <div>
              <h1 className="text-xl font-bold text-slate-800">Export / Analysis Sheets</h1>
              <p className="text-sm text-slate-500">Excel形式で分析データをダウンロード</p>
            </div>
          </div>
        </div>
      </div>

      {/* Cards */}
      <div className="container mx-auto px-6 py-10 flex flex-col gap-8 max-w-4xl">
        {EXPORT_CARDS.map((card) => {
          const lb = lookbacks[card.title];
          const url = `${API_BASE}${card.endpoint}?lookback=${lb}`;
          const filename = `${card.filenamePrefix}_${today}.xlsx`;

          return (
            <div
              key={card.title}
              className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden"
            >
              {/* Card Header */}
              <div className="px-8 py-5 border-b border-slate-100 bg-slate-50 flex items-start justify-between gap-4">
                <div>
                  <h2 className="text-lg font-bold text-slate-800 mb-0.5">{card.title}</h2>
                  <p className="text-sm text-slate-600">{card.description}</p>
                </div>
                <span className="shrink-0 text-xs bg-teal-50 text-teal-700 border border-teal-200 rounded-full px-2.5 py-1 font-medium">
                  .xlsx
                </span>
              </div>

              <div className="px-8 py-6 flex flex-col gap-6">
                {/* Sheet list */}
                <div>
                  <p className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
                    シート構成
                  </p>
                  <div className="divide-y divide-slate-100 rounded-xl border border-slate-100 overflow-hidden">
                    {card.sheets.map((sheet) => (
                      <div key={sheet.name} className="flex items-baseline gap-3 px-4 py-2.5 bg-white">
                        <span className="font-mono text-xs font-semibold text-teal-700 bg-teal-50 px-2 py-0.5 rounded shrink-0">
                          {sheet.name}
                        </span>
                        <span className="text-sm text-slate-600">{sheet.description}</span>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Layout note */}
                <div className="text-xs text-slate-500 bg-slate-50 rounded-lg px-4 py-2.5">
                  <span className="font-semibold text-slate-600">フォーマット: </span>
                  行1=列ヘッダー、行2=参照情報（日付/単位）、行3-7=Z-score数式（10D/20D/50D/100D/200D）、行13-=データ降順
                </div>

                {/* Lookback & Download */}
                <div className="flex items-end gap-4 flex-wrap">
                  <div className="flex flex-col gap-1">
                    <label className="text-xs font-semibold text-slate-500">
                      取得期間（暦日）
                    </label>
                    <div className="flex items-center gap-2">
                      <input
                        type="number"
                        min={30}
                        max={1000}
                        value={lb}
                        onChange={(e) =>
                          setLookbacks((prev) => ({
                            ...prev,
                            [card.title]: Math.min(1000, Math.max(30, Number(e.target.value))),
                          }))
                        }
                        className="w-24 px-3 py-2 rounded-lg border border-slate-300 text-sm
                          focus:outline-none focus:ring-2 focus:ring-teal-400 focus:border-transparent"
                      />
                      <span className="text-sm text-slate-500">日</span>
                    </div>
                  </div>

                  <DownloadButton label={`${card.title} をダウンロード`} url={url} filename={filename} />
                </div>

                {/* Note */}
                {card.note && (
                  <p className="text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded-lg px-4 py-2.5">
                    {card.note}
                  </p>
                )}
              </div>
            </div>
          );
        })}
      </div>

      <footer className="container mx-auto px-6 py-8 text-center text-slate-400 text-sm border-t border-slate-200/60 mt-4">
        <p>© 2026 Market Analytics System</p>
      </footer>
    </main>
  );
}
