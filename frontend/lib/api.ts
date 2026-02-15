export const fetcher = async (url: string) => {
  const res = await fetch(url);
  if (!res.ok) {
    let detail = 'An error occurred while fetching the data.';
    try {
      const errorJson = await res.json();
      if (errorJson && errorJson.detail) {
        detail = errorJson.detail;
      }
    } catch (e) {
      // JSONのパースに失敗した場合はデフォルトメッセージのまま
    }
    const error = new Error(detail);
    (error as any).detail = detail; // プロパティを付随させる
    throw error;
  }
  return res.json();
};

export interface BondYieldData {
  maturity: number;
  yield: number;
  bond_name: string;
  bond_code?: string;
}

export interface YieldCurveResponse {
  date: string;
  data: BondYieldData[];
  error?: string;
}

export interface QuickDatesResponse {
  latest?: string;
  previous?: string;
  five_days_ago?: string;
  month_ago?: string;
}

export interface ASWData {
  maturity: number;
  bond_code?: string;
  bond_name: string;
  bond_yield: number;
  swap_rate: number;
  asw: number;
}

export interface ASWCurveResponse {
  date: string;
  data: ASWData[];
  error?: string;
}

export interface ForwardRateData {
  maturity: number;
  rate: number;
  start_tenor: string;
  swap_tenor: string;
}

export interface ForwardCurveResponse {
  date: string;
  type: 'fixed-start' | 'fixed-tenor';
  parameter: string;
  data: ForwardRateData[];
  error?: string;
}

// PCA Types
export interface PCAComponent {
  pc_number: number;
  eigenvalue: number;
  explained_variance_ratio: number;
  cumulative_variance_ratio: number;
  loadings: number[];
}

export interface PCAScore {
  date: string;
  pc1: number;
  pc2: number;
  pc3: number;
  [key: string]: string | number;
}

export interface ReconstructionDataPoint {
  maturity: number;
  bond_code: string;
  bond_name: string;
  original_yield: number;
  reconstructed_yield: number;
  error: number;
}

export interface ReconstructionStatistics {
  mae: number;
  mse: number;
  rmse: number;
  max_error: number;
  std: number;
  min: number;
  max: number;
}

export interface DailyReconstruction {
  data: ReconstructionDataPoint[];
  statistics: ReconstructionStatistics;
}

export interface PCAResponse {
  components: PCAComponent[];
  scores: PCAScore[];
  maturities: number[];
  mean_vector: number[];
  parameters: {
    days: number;
    components: number;
    actual_end_date?: string;
    date_range?: {
      start: string;
      end: string;
    };
  };
  reconstruction_dates?: string[];
  latest_reconstruction?: DailyReconstruction & { date: string };
  reconstruction?: { [date: string]: DailyReconstruction }; // 互換性のため残す
}

// Market Amount Types
export interface MarketAmountBucket {
  year: number;
  amount: number;
}

export interface MarketAmountResponse {
  date: string;
  buckets: MarketAmountBucket[];
  total_amount: number;
  bond_count: number;
  error?: string;
}

export interface BondTimeseriesPoint {
  trade_date: string;
  market_amount: number;
}

export interface BondTimeseriesStatistics {
  latest_date: string;
  latest_amount: number;
  min_amount: number;
  max_amount: number;
  avg_amount: number;
  data_points: number;
}

export interface BondTimeseriesResponse {
  bond_code: string;
  bond_name: string;
  due_date: string;
  timeseries: BondTimeseriesPoint[];
  statistics: BondTimeseriesStatistics;
}

export interface BondSearchItem {
  bond_code: string;
  bond_name: string;
  due_date: string;
  latest_market_amount: number;
  latest_trade_date: string;
}

export interface BondSearchResponse {
  bonds: BondSearchItem[];
  count: number;
}
