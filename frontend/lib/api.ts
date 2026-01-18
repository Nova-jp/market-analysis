export const fetcher = async (url: string) => {
  const res = await fetch(url);
  if (!res.ok) {
    const error = new Error('An error occurred while fetching the data.');
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

export interface PCAResponse {
  components: PCAComponent[];
  scores: PCAScore[];
  maturities: number[];
  mean_vector: number[];
  parameters: {
    days: number;
    components: number;
  };
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
