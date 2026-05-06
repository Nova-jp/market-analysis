import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  ZAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
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

const YieldCurveChart = ({ datasets, minMaturity = 0, maxMaturity = 40 }: YieldCurveChartProps) => {
  if (!datasets || datasets.length === 0) return null;

  // カスタムツールチップ: マウス位置に最も近い銘柄を全日程分抽出して表示
  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      // ホバーされている点の残存期間を基準にする
      const hoveredMaturity = payload[0].payload.maturity;

      // 1. 各日程（dataset）ごとに、hoveredMaturityに最も近い銘柄を抽出
      const closestPoints = datasets.map(ds => {
        if (!ds.data || ds.data.length === 0) return null;
        
        // 最小距離を特定
        const minDistance = Math.min(...ds.data.map(p => Math.abs(p.maturity - hoveredMaturity)));
        
        // その最小距離にある銘柄をすべて取得
        const closestBonds = ds.data.filter(p => Math.abs(Math.abs(p.maturity - hoveredMaturity) - minDistance) < 0.0001);
        
        return {
          date: ds.date,
          color: ds.color,
          bonds: closestBonds
        };
      }).filter(item => item !== null && item.bonds.length > 0);

      if (closestPoints.length === 0) return null;

      return (
        <div className="bg-white/95 backdrop-blur-sm p-4 border border-slate-200 rounded-xl shadow-xl text-sm min-w-[280px] z-50">
          <p className="font-bold text-slate-700 mb-3 border-b border-slate-100 pb-1 flex justify-between items-center">
            <span>Target Maturity:</span>
            <span className="text-blue-600 font-black">{hoveredMaturity.toFixed(3)}Y</span>
          </p>
          <div className="space-y-4">
            {closestPoints.map((group, gIdx) => (
              <div key={gIdx} className="space-y-2">
                {group.bonds.map((bond, bIdx) => (
                  <div key={`${gIdx}-${bIdx}`} className="flex flex-col border-l-2 pl-3" style={{ borderColor: group.color }}>
                    <div className="flex justify-between items-center mb-0.5">
                      <span className="text-[10px] font-bold text-slate-500 uppercase">{group.date}</span>
                      <span className="font-black text-base" style={{ color: group.color }}>{bond.yield.toFixed(3)}%</span>
                    </div>
                    <div className="font-bold text-slate-800 text-xs leading-tight">
                      {bond.bond_name}
                    </div>
                    <div className="text-[10px] text-slate-400 grid grid-cols-2 gap-x-2 gap-y-0.5 mt-1 border-t border-slate-50 pt-1">
                      <span>Maturity: {bond.maturity.toFixed(3)}Y</span>
                    </div>
                  </div>
                ))}
              </div>
            ))}
          </div>
        </div>
      );
    }
    return null;
  };

  // 1. 表示データの範囲に合わせてY軸のスケールを手動計算
  const allPoints = datasets.flatMap(ds => ds.data);
  const yValues = allPoints
    .filter(p => p.maturity >= minMaturity && p.maturity <= maxMaturity)
    .map(p => p.yield);
  
  let yDomain: [number | string, number | string] = ['auto', 'auto'];
  if (yValues.length > 0) {
    const min = Math.min(...yValues);
    const max = Math.max(...yValues);
    const padding = (max - min) * 0.1 || 0.01;
    yDomain = [Number((min - padding).toFixed(4)), Number((max + padding).toFixed(4))];
  }

  return (
    <div className="h-[550px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart
          margin={{ top: 10, right: 30, left: 20, bottom: 25 }}
        >
          <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
          <XAxis 
            dataKey="maturity" 
            name="Maturity"
            unit="Y"
            type="number"
            domain={[minMaturity, maxMaturity]} 
            allowDataOverflow={true}
            tick={{ fontSize: 12 }}
            stroke="#64748b"
            tickCount={10} 
          />
          <YAxis 
            dataKey="yield"
            name="Yield"
            unit="%"
            label={{ value: 'Yield (%)', angle: -90, position: 'insideLeft', offset: 0 }}
            tick={{ fontSize: 12 }}
            domain={yDomain}
            stroke="#64748b"
          />
          <ZAxis type="number" range={[50, 50]} /> 
          <Tooltip content={<CustomTooltip />} />
          <Legend 
            verticalAlign="top" 
            height={40}
            wrapperStyle={{ paddingBottom: '20px' }}
          />
          
          {datasets.map((ds) => (
            <Scatter
              key={ds.date}
              name={`JGB ${ds.date}`}
              data={ds.data.filter(p => p.maturity >= minMaturity && p.maturity <= maxMaturity)}
              fill={ds.color}
              fillOpacity={0.7}
            />
          ))}
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
};

export default YieldCurveChart;
