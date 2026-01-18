'use client';

import React, { useState, useEffect, useMemo } from 'react';
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
  Legend
} from 'recharts';
import { 
  Loader2, 
  Search, 
  ArrowLeft, 
  BarChart3, 
  X,
  RefreshCw,
  Plus
} from 'lucide-react';

// Color palette for multiple lines
const COLORS = [
  '#2563eb', '#dc2626', '#059669', '#d97706', '#7c3aed', 
  '#db2777', '#0891b2', '#4f46e5', '#ea580c', '#1e293b'
];

interface SelectedBondData extends BondTimeseriesResponse {
  color: string;
}

export default function MarketAmountPage() {
  // Search State
  const [searchTerm, setSearchTerm] = useState('');
  const [searchResults, setSearchResults] = useState<BondSearchItem[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);
  
  // Data State
  const [selectedBonds, setSelectedBonds] = useState<SelectedBondData[]>([]);
  const [loadingData, setLoadingData] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Debounce search
  useEffect(() => {
    const delayDebounceFn = setTimeout(async () => {
      fetchSearchResults(searchTerm);
    }, 300);

    return () => clearTimeout(delayDebounceFn);
  }, [searchTerm]);

  const fetchSearchResults = async (query: string) => {
    setIsSearching(true);
    try {
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
    // Prevent duplicates
    if (selectedBonds.some(b => b.bond_code === bond.bond_code)) {
      setSearchTerm('');
      setShowDropdown(false);
      return;
    }
    
    if (selectedBonds.length >= 10) {
      setError('一度に表示できるのは最大10銘柄までです。');
      return;
    }

    setSearchTerm('');
    setShowDropdown(false);
    setLoadingData(true);
    setError(null);

    try {
      const data: BondTimeseriesResponse = await fetcher(`/api/market-amount/bond/${bond.bond_code}`);
      const newColor = COLORS[selectedBonds.length % COLORS.length];
      
      setSelectedBonds(prev => [...prev, { ...data, color: newColor }]);
    } catch (err) {
      console.error('Fetch bond data failed', err);
      setError('データの取得に失敗しました。');
    } finally {
      setLoadingData(false);
    }
  };

  const removeBond = (bondCode: string) => {
    setSelectedBonds(prev => {
      const filtered = prev.filter(b => b.bond_code !== bondCode);
      // Re-assign colors to maintain order if desired, or just keep them.
      // Keeping them simple for now.
      return filtered.map((b, idx) => ({
        ...b,
        color: COLORS[idx % COLORS.length]
      }));
    });
  };

  const clearAll = () => {
    setSelectedBonds([]);
    setError(null);
  };

  // Merge data for the chart
  const chartData = useMemo(() => {
    const dataMap = new Map<string, any>();
    
    selectedBonds.forEach(bond => {
      bond.timeseries.forEach(point => {
        if (!dataMap.has(point.trade_date)) {
          dataMap.set(point.trade_date, { date: point.trade_date });
        }
        const entry = dataMap.get(point.trade_date);
        entry[bond.bond_code] = point.market_amount;
        entry[`${bond.bond_code}_name`] = bond.bond_name;
      });
    });

    return Array.from(dataMap.values()).sort((a, b) => a.date.localeCompare(b.date));
  }, [selectedBonds]);

  // Custom Tooltip
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white/95 backdrop-blur-sm p-4 border border-slate-200 rounded-xl shadow-xl text-sm min-w-[200px]">
          <p className="font-bold text-slate-700 mb-2 border-b border-slate-100 pb-1">
            Date: {label}
          </p>
          <div className="space-y-2">
            {payload.map((entry: any, index: number) => {
              const bondCode = entry.dataKey;
              // Find the bond name from selectedBonds or payload
              const bondName = selectedBonds.find(b => b.bond_code === bondCode)?.bond_name || bondCode;
              
              return (
                <div key={index} className="flex flex-col gap-0.5">
                  <div className="flex items-center justify-between gap-4 font-bold" style={{ color: entry.color }}>
                    <span>{bondName}</span>
                    <span>{entry.value?.toLocaleString()} 億円</span>
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
              複数の銘柄の市中残存額(Market Amount)を比較・分析します。
            </p>
          </div>
          
          {selectedBonds.length > 0 && (
            <button 
              onClick={clearAll}
              className="flex items-center gap-1.5 px-4 py-2 bg-white text-slate-700 font-bold border border-slate-200 hover:text-red-600 hover:border-red-200 hover:bg-red-50 rounded-xl transition-all shadow-sm"
            >
              <RefreshCw className="w-4 h-4" />
              Reset All
            </button>
          )}
        </div>

        {/* Search Section */}
        <div className="bg-white p-6 rounded-2xl shadow-md border border-slate-200">
          <label className="block text-black font-black mb-2">Add Bond to Comparison</label>
          <div className="relative max-w-2xl">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-500" />
              <input
                type="text"
                placeholder="Search bonds (e.g., 00430045, 10年)"
                value={searchTerm}
                onChange={(e) => {
                  setSearchTerm(e.target.value);
                  setShowDropdown(true);
                }}
                onFocus={() => setShowDropdown(true)}
                className="w-full pl-10 pr-4 py-3 bg-slate-50 border-2 border-slate-300 rounded-xl text-lg font-black text-black focus:outline-none focus:border-blue-600 focus:bg-white transition-all placeholder:text-slate-400"
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
                      <div className="font-bold text-black group-hover:text-blue-600 transition-colors">
                        {bond.bond_name}
                      </div>
                      <div className="text-xs text-slate-600 font-mono">
                        {bond.bond_code} | Due: {bond.due_date}
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-sm font-bold text-slate-900">
                        {bond.latest_market_amount.toLocaleString()} 億円
                      </div>
                      <div className="text-xs text-slate-600">
                        {bond.latest_trade_date}
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            )}
            
            {showDropdown && searchResults.length === 0 && !isSearching && searchTerm && (
               <div className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl shadow-xl border border-slate-200 p-4 text-center text-slate-500 z-50">
                 No bonds found.
               </div>
            )}
          </div>
        </div>

        {/* Active Chips */}
        {selectedBonds.length > 0 && (
            <div className="flex flex-wrap gap-3 animate-in fade-in slide-in-from-top-4 duration-500">
                {selectedBonds.map((bond) => (
                    <div 
                        key={bond.bond_code} 
                        className="flex items-center gap-3 pl-4 pr-3 py-2 bg-white border-2 rounded-xl shadow-sm transition-all hover:shadow-md hover:-translate-y-0.5"
                        style={{ borderColor: bond.color }}
                    >
                        <div className="flex flex-col">
                            <span className="text-sm font-black text-black leading-tight">{bond.bond_name}</span>
                            <span className="text-[10px] text-slate-700 font-bold font-mono">{bond.bond_code}</span>
                        </div>
                        <button onClick={() => removeBond(bond.bond_code)} className="p-1 hover:bg-slate-100 rounded-full text-slate-400 hover:text-red-500 transition-colors">
                            <X className="w-4 h-4" />
                        </button>
                    </div>
                ))}
            </div>
        )}

        {/* Content Area */}
        {loadingData && selectedBonds.length === 0 ? (
          <div className="h-[400px] flex flex-col items-center justify-center gap-4 bg-white rounded-3xl border border-slate-200">
            <Loader2 className="w-12 h-12 animate-spin text-blue-600" />
            <p className="text-xl font-bold text-slate-600">Loading Bond Data...</p>
          </div>
        ) : error && selectedBonds.length === 0 ? (
           <div className="p-8 bg-red-50 text-red-600 font-bold rounded-2xl border border-red-100 text-center">
             {error}
           </div>
        ) : selectedBonds.length > 0 ? (
          <div className="bg-white p-6 rounded-3xl border border-slate-200 shadow-lg relative min-h-[500px]">
             {loadingData && (
                <div className="absolute inset-0 bg-white/60 backdrop-blur-[1px] z-10 flex items-center justify-center rounded-3xl">
                  <Loader2 className="w-10 h-10 animate-spin text-blue-600" />
                </div>
             )}
             
             <div className="h-[500px] w-full">
               <ResponsiveContainer width="100%" height="100%">
                 <LineChart
                   data={chartData}
                   margin={{ top: 10, right: 30, left: 20, bottom: 0 }}
                 >
                   <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                   <XAxis 
                     dataKey="date" 
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
                   <Tooltip content={<CustomTooltip />} />
                   <Legend verticalAlign="top" height={36}/>
                   
                   {selectedBonds.map((bond) => (
                     <Line
                       key={bond.bond_code}
                       type="monotone"
                       dataKey={bond.bond_code}
                       name={bond.bond_name}
                       stroke={bond.color}
                       strokeWidth={2}
                       dot={false}
                       activeDot={{ r: 6 }}
                       connectNulls
                       animationDuration={500}
                     />
                   ))}
                 </LineChart>
               </ResponsiveContainer>
             </div>
          </div>
        ) : (
          <div className="h-[500px] flex flex-col items-center justify-center text-slate-400 border-4 border-dashed border-slate-100 rounded-3xl bg-slate-50/50">
             <BarChart3 className="w-20 h-20 mb-6 text-slate-200" />
             <p className="text-2xl font-black text-slate-400">No Bonds Selected</p>
             <p className="text-slate-400 mt-2 font-medium">Search and select bonds above to compare their history.</p>
          </div>
        )}
      </div>
    </div>
  );
}