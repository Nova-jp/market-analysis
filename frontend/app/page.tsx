import Link from 'next/link';
import { TrendingUp, BarChart3, Database, ShieldCheck, Activity } from 'lucide-react';

export default function Home() {
  const tools = [
    {
      title: 'Yield Curve Analysis',
      description: '日本国債(JGB)のイールドカーブ比較と推移分析。',
      href: '/yield-curve',
      icon: <TrendingUp className="w-6 h-6" />,
      color: 'bg-blue-500',
    },
    {
      title: 'PCA Analysis',
      description: '主成分分析(Level, Slope, Curvature)による変動要因の分解。',
      href: '/pca',
      icon: <BarChart3 className="w-6 h-6" />,
      color: 'bg-indigo-500',
      status: 'Coming Soon'
    },
    {
      title: 'Market Amount',
      description: '国債の市中残存額と日銀保有状況の可視化。',
      href: '/market-amount',
      icon: <Database className="w-6 h-6" />,
      color: 'bg-emerald-500',
      status: 'Coming Soon'
    },
    {
      title: 'Private Analytics',
      description: 'フォワードカーブ計算と詳細なPCAバストレポ。',
      href: '/private',
      icon: <ShieldCheck className="w-6 h-6" />,
      color: 'bg-amber-500',
      status: 'Coming Soon'
    },
  ];

  return (
    <main className="min-h-screen bg-slate-50">
      {/* Hero Section */}
      <div className="bg-white border-b border-slate-200">
        <div className="container mx-auto px-6 py-16 text-center">
          <div className="inline-flex items-center justify-center p-2 bg-blue-50 rounded-2xl mb-6">
            <Activity className="w-8 h-8 text-blue-600" />
          </div>
          <h1 className="text-4xl md:text-5xl font-extrabold text-slate-900 mb-4 tracking-tight">
            Market Analytics <span className="text-blue-600 text-3xl align-top">v2.0</span>
          </h1>
          <p className="text-xl text-slate-600 max-w-2xl mx-auto">
            最新のWeb技術を活用した、プロフェッショナルな債券市場分析プラットフォーム。
            高速な計算と洗練されたインターフェースを提供します。
          </p>
        </div>
      </div>

      {/* Grid Section */}
      <div className="container mx-auto px-6 py-12">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          {tools.map((tool) => (
            <Link 
              key={tool.title} 
              href={tool.status ? '#' : tool.href}
              className={`group relative bg-white p-8 rounded-2xl shadow-sm border border-slate-200 transition-all duration-300 ${
                tool.status 
                  ? 'opacity-75 grayscale-[0.5] cursor-not-allowed' 
                  : 'hover:shadow-xl hover:border-blue-200 cursor-pointer'
              }`}
            >
              <div className="flex items-start gap-6">
                <div className={`${tool.color} p-4 rounded-xl text-white shadow-lg group-hover:scale-110 transition-transform duration-300`}>
                  {tool.icon}
                </div>
                <div className="flex-1">
                  <div className="flex items-center justify-between mb-1">
                    <h3 className={`text-2xl font-bold transition-colors duration-300 ${
                      tool.status ? 'text-slate-600' : 'text-slate-800 group-hover:text-blue-600'
                    }`}>
                      {tool.title}
                    </h3>
                    {tool.status && (
                      <span className="text-[10px] uppercase tracking-wider font-bold bg-slate-100 text-slate-500 px-2 py-1 rounded">
                        {tool.status}
                      </span>
                    )}
                  </div>
                  <p className="text-slate-600 leading-relaxed">
                    {tool.description}
                  </p>
                </div>
              </div>
              
              {!tool.status && (
                <div className="mt-6 flex items-center text-blue-600 font-semibold text-sm">
                  ツールを開く
                  <svg className="w-4 h-4 ml-1 group-hover:translate-x-1 transition-transform" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                  </svg>
                </div>
              )}
            </Link>
          ))}
        </div>
      </div>

      {/* Footer */}
      <footer className="container mx-auto px-6 py-12 text-center text-slate-400 text-sm border-t border-slate-200/60 mt-12">
        <p>© 2026 Market Analytics System. All rights reserved.</p>
        <p className="mt-1 flex items-center justify-center gap-2">
          <span>Powered by Next.js</span>
          <span className="w-1 h-1 bg-slate-300 rounded-full"></span>
          <span>TypeScript</span>
          <span className="w-1 h-1 bg-slate-300 rounded-full"></span>
          <span>FastAPI</span>
        </p>
      </footer>
    </main>
  );
}