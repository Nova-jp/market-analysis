'use client';

import React, { useState, useEffect, useRef } from 'react';
import Link from 'next/link';
import { 
  fetcher, 
  BondSearchResponse, 
  BondSearchItem, 
  BondTimeseriesResponse 
} from '@/lib/api';
import { 
  LineChart, 
  Line, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  AreaChart,
  Area
} from 'recharts';
import { 
  Loader2, 
  Search, 
  TrendingUp, 
  ArrowLeft, 
  BarChart3, 
  Calendar,
  DollarSign,
  Info
} from 'lucide-react';

export default function MarketAmountPage() {
  // Search State
  const [searchTerm, setSearchTerm] = useState('');
  const [searchResults, setSearchResults] = useState<BondSearchItem[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);
  
  // Data State
  const [selectedBond, setSelectedBond] = useState<BondTimeseriesResponse | null>(null);
  const [loadingData, setLoadingData] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Debounce search
  useEffect(() => {
    const delayDebounceFn = setTimeout(async () => {
      // Always fetch initial list if empty or if searching
      fetchSearchResults(searchTerm);
    }, 300);

    return () => clearTimeout(delayDebounceFn);
  }, [searchTerm]);

  const fetchSearchResults = async (query: string) => {
    setIsSearching(true);
    try {
      // Note: The backend search currently returns a list. 
      // Ideally pass query to backend, but currently backend might just return recent ones or accepts bond_type?
      // Looking at the API code, it doesn't take a text query, only limit.
      // So we fetch all (limit 100) and filter client side if needed, or just show the list.
      // Wait, the API is `search_bonds(bond_type, limit)`. It doesn't seem to support text search yet.
      // So we will just fetch the list and filter client side if the list is small enough, 
      // or just show the available bonds sorted by update.
      
      const res: BondSearchResponse = await fetcher('/api/market-amount/bonds/search?limit=100');
      let filtered = res.bonds;
      if (query) {
        const lowerQ = query.toLowerCase();
        filtered = res.bonds.filter(b => 
          b.bond_code.includes(query) || 
          b.bond_name.toLowerCase().includes(lowerQ)
        );
      }
      setSearchResults(filtered);
    } catch (err) {
      console.error('Search failed', err);
    } finally {
      setIsSearching(false);
    }
  };

  const handleSelectBond = async (bond: BondSearchItem) => {
    setSearchTerm(`${bond.bond_code} - ${bond.bond_name}`);
    setShowDropdown(false);
    setLoadingData(true);
    setError(null);
    try {
      const data: BondTimeseriesResponse = await fetcher(`/api/market-amount/bond/${bond.bond_code}`);
      setSelectedBond(data);
    } catch (err) {
      console.error('Fetch bond data failed', err);
      setError('データの取得に失敗しました。');
      setSelectedBond(null);
    } finally {
      setLoadingData(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-100">
      <div className="container mx-auto p-4 md:p-8 space-y-8">
        
        {/* Header */}
        <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 bg-white/50 p-6 rounded-2xl border border-white/20 shadow-sm">
          <div>
            <Link 
              href="/" 
              className="inline-flex items-center gap-2 text-slate-500 hover:text-blue-600 font-bold mb-2 transition-colors"
            >
              <ArrowLeft className="w-4 h-4" />
              Back to Home
            </Link>
            <h1 className="text-4xl font-extrabold text-black flex items-center gap-3 tracking-tight">
              <BarChart3 className="w-10 h-10 text-blue-600" />
              Market Amount Analysis
            </h1>
            <p className="text-slate-800 font-medium text-lg mt-2">
              個別の銘柄ごとの市中残存額(Market Amount)の推移を追跡します。
            </p>
          </div>
        </div>

        {/* Search Section */}
        <div className="bg-white p-6 rounded-2xl shadow-md border border-slate-200">
          <label className="block text-slate-700 font-bold mb-2">Select Bond (Code or Name)</label>
          <div className="relative max-w-2xl">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-400" />
              <input
                type="text"
                placeholder="Search bonds (e.g., 00430045, 10年)"
                value={searchTerm}
                onChange={(e) => {
                  setSearchTerm(e.target.value);
                  setShowDropdown(true);
                }}
                onFocus={() => setShowDropdown(true)}
                className="w-full pl-10 pr-4 py-3 bg-slate-50 border-2 border-slate-200 rounded-xl text-lg font-medium focus:outline-none focus:border-blue-500 focus:bg-white transition-all"
              />
              {isSearching && (
                <div className="absolute right-3 top-1/2 -translate-y-1/2">
                  <Loader2 className="w-5 h-5 animate-spin text-blue-500" />
                </div>
              )}
            </div>

            {/* Dropdown Results */}
            {showDropdown && searchResults.length > 0 && (
              <div className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl shadow-xl border border-slate-200 max-h-[300px] overflow-y-auto z-50">
                {searchResults.map((bond) => (
                  <button
                    key={bond.bond_code}
                    onClick={() => handleSelectBond(bond)}
                    className="w-full text-left px-4 py-3 hover:bg-slate-50 flex items-center justify-between border-b border-slate-50 last:border-none group"
                  >
                    <div>
                      <div className="font-bold text-slate-800 group-hover:text-blue-600 transition-colors">
                        {bond.bond_name}
                      </div>
                      <div className="text-xs text-slate-400 font-mono">
                        {bond.bond_code} | Due: {bond.due_date}
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-sm font-bold text-slate-600">
                        {bond.latest_market_amount.toLocaleString()} 億円
                      </div>
                      <div className="text-xs text-slate-400">
                        {bond.latest_trade_date}
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            )}
            
            {showDropdown && searchResults.length === 0 && !isSearching && (
               <div className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl shadow-xl border border-slate-200 p-4 text-center text-slate-500 z-50">
                 No bonds found.
               </div>
            )}
          </div>
        </div>

        {/* Content Area */}
        {loadingData ? (
          <div className="h-[400px] flex flex-col items-center justify-center gap-4 bg-white rounded-3xl border border-slate-200">
            <Loader2 className="w-12 h-12 animate-spin text-blue-600" />
            <p className="text-xl font-bold text-slate-600">Loading Bond Data...</p>
          </div>
        ) : error ? (
           <div className="p-8 bg-red-50 text-red-600 font-bold rounded-2xl border border-red-100 text-center">
             {error}
           </div>
        ) : selectedBond ? (
          <div className="space-y-6">
            
            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
               <div className="bg-white p-5 rounded-2xl border border-slate-200 shadow-sm">
                 <div className="flex items-center gap-2 text-slate-500 text-sm font-bold mb-1">
                   <Calendar className="w-4 h-4" /> Latest Date
                 </div>
                 <div className="text-2xl font-black text-slate-800">
                   {selectedBond.statistics.latest_date}
                 </div>
               </div>
               <div className="bg-white p-5 rounded-2xl border border-slate-200 shadow-sm">
                 <div className="flex items-center gap-2 text-slate-500 text-sm font-bold mb-1">
                   <DollarSign className="w-4 h-4" /> Latest Amount
                 </div>
                 <div className="text-2xl font-black text-blue-600">
                   {selectedBond.statistics.latest_amount.toLocaleString()} <span className="text-sm text-slate-400 font-medium">億円</span>
                 </div>
               </div>
               <div className="bg-white p-5 rounded-2xl border border-slate-200 shadow-sm">
                 <div className="flex items-center gap-2 text-slate-500 text-sm font-bold mb-1">
                   <TrendingUp className="w-4 h-4" /> Average
                 </div>
                 <div className="text-2xl font-black text-slate-800">
                   {selectedBond.statistics.avg_amount.toLocaleString()} <span className="text-sm text-slate-400 font-medium">億円</span>
                 </div>
               </div>
               <div className="bg-white p-5 rounded-2xl border border-slate-200 shadow-sm">
                 <div className="flex items-center gap-2 text-slate-500 text-sm font-bold mb-1">
                   <Info className="w-4 h-4" /> Data Points
                 </div>
                 <div className="text-2xl font-black text-slate-800">
                   {selectedBond.statistics.data_points}
                 </div>
               </div>
            </div>

            {/* Chart */}
            <div className="bg-white p-6 rounded-3xl border border-slate-200 shadow-lg">
               <div className="mb-6">
                 <h2 className="text-2xl font-bold text-slate-800">
                   {selectedBond.bond_name}
                 </h2>
                 <p className="text-slate-500 font-mono text-sm">
                   Code: {selectedBond.bond_code} | Maturity: {selectedBond.due_date}
                 </p>
               </div>
               
               <div className="h-[500px] w-full">
                 <ResponsiveContainer width="100%" height="100%">
                   <AreaChart
                     data={selectedBond.timeseries}
                     margin={{ top: 10, right: 30, left: 20, bottom: 0 }}
                   >
                     <defs>
                       <linearGradient id="colorAmount" x1="0" y1="0" x2="0" y2="1">
                         <stop offset="5%" stopColor="#2563eb" stopOpacity={0.3}/>
                         <stop offset="95%" stopColor="#2563eb" stopOpacity={0}/>
                       </linearGradient>
                     </defs>
                     <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                     <XAxis 
                       dataKey="trade_date" 
                       tick={{ fontSize: 12 }}
                       stroke="#64748b"
                       minTickGap={30}
                     />
                     <YAxis 
                       domain={['auto', 'auto']}
                       tickFormatter={(value) => (value / 10000).toFixed(1) + '兆'}
                       tick={{ fontSize: 12 }}
                       stroke="#64748b"
                     />
                     <Tooltip 
                       contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                       formatter={(value: any) => [value?.toLocaleString() + ' 億円', 'Market Amount']}
                     />
                     <Area 
                       type="monotone" 
                       dataKey="market_amount" 
                       stroke="#2563eb" 
                       strokeWidth={3}
                       fillOpacity={1} 
                       fill="url(#colorAmount)" 
                       animationDuration={1000}
                     />
                   </AreaChart>
                 </ResponsiveContainer>
               </div>
            </div>

          </div>
        ) : (
          <div className="h-[500px] flex flex-col items-center justify-center text-slate-400 border-4 border-dashed border-slate-100 rounded-3xl bg-slate-50/50">
             <BarChart3 className="w-20 h-20 mb-6 text-slate-200" />
             <p className="text-2xl font-black text-slate-400">No Bond Selected</p>
             <p className="text-slate-400 mt-2 font-medium">Search and select a bond above to view its history.</p>
          </div>
        )}
      </div>
    </div>
  );
}
