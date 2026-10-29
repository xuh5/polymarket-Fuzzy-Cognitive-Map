// 调用后端 API。本机开发时 VITE_API_BASE 不设，走 vite 的 /api 转发；
// 部署到 Cloudflare Pages 时设成后端的公网地址（Cloudflare Tunnel 的网址）
const BASE = import.meta.env.VITE_API_BASE ?? ''

// 下面的类型和后端返回的字段一一对应（pmdata/api/queries.py）
export interface Market {
  market_id: string
  question: string
  slug: string | null
  outcome: string
  end_date: string | null
  asset_id: string
  mid: number | null
  best_bid: number | null
  best_ask: number | null
  spread: number | null
  logodds_chg_60m: number | null
  updated_at: string
}

export interface Health {
  status: string
  latest_metric_at: string | null
  age_seconds: number | null
}

export interface HistoryPoint {
  second: string
  best_bid: number | null
  best_ask: number | null
  mid: number | null
  microprice: number | null
}

// 一格的成交，两个选项合并成 Yes 的视角（看涨 = 买 Yes / 卖 No），单位是份数
export interface TradeBucket {
  t: string
  trades: number
  bullish: number
  bearish: number
}

async function getJson<T>(path: string): Promise<T> {
  const r = await fetch(BASE + path)
  if (!r.ok) throw new Error(`${path} 返回 ${r.status}`)
  return r.json()
}

export const fetchMarkets = () => getJson<Market[]>('/api/markets')
export const fetchHealth = () => getJson<Health>('/api/health')
export const fetchHistory = (marketId: string, minutes: number, bucketSeconds: number) =>
  getJson<HistoryPoint[]>(`/api/markets/${marketId}/history?minutes=${minutes}&bucket_seconds=${bucketSeconds}`)
export const fetchTrades = (marketId: string, minutes: number, bucketSeconds: number) =>
  getJson<TradeBucket[]>(`/api/markets/${marketId}/trades?minutes=${minutes}&bucket_seconds=${bucketSeconds}`)

export interface OutcomeStats {
  outcome: string
  outcome_index: number
  asset_id: string
  mid: number | null
  best_bid: number | null
  best_ask: number | null
  spread: number | null
  microprice: number | null
  imbalance_5: number | null
  bid_depth_5c: number | null
  ask_depth_5c: number | null
  logodds_chg_60m: number | null
  updated_at: string | null
}

export interface MarketDetail {
  market_id: string
  question: string
  slug: string | null
  end_date: string | null
  outcomes: OutcomeStats[]
}

// 一档：[价格, 份数]。bids 价高在前，asks 价低在前
export type Level = [number, number]

export interface Book {
  asset_id: string
  outcome: string
  available: boolean
  exchange_ts: number | null
  written_at: string | null
  bids: Level[]
  asks: Level[]
}

export const fetchMarket = (marketId: string) => getJson<MarketDetail>(`/api/markets/${marketId}`)
export const fetchBook = (marketId: string, outcomeIndex: number) =>
  getJson<Book>(`/api/markets/${marketId}/book?outcome_index=${outcomeIndex}`)

// Polymarket 上这个市场的页面（/market/<slug> 会自动跳到所属事件下的正确网址）
export const polymarketUrl = (slug: string) => `https://polymarket.com/market/${slug}`

// ---------- Status 页面（pmdata/api/status.py） ----------
export type HealthLevel = 'ok' | 'degraded' | 'down' | 'none'
export const LEVEL_TEXT = { ok: 'Operational', degraded: 'Degraded', down: 'Outage', none: 'No data' } as const

// 各段延迟（毫秒）：每分钟的 p50 / p99 再取中位数
export interface Latency {
  exch_recv_p50: number | null
  exch_recv_p99: number | null
  recv_comp_p50: number | null
  recv_comp_p99: number | null
  comp_ins_p50: number | null
  comp_ins_p99: number | null
  end_to_end_p50: number | null
  end_to_end_p99: number | null
}

export interface UptimeHour {
  start: string
  level: HealthLevel
  down_seconds: number
}

export interface Incident {
  title: string
  start: string
  end: string
  down_seconds: number
  count: number
}

export type Stage = 'collector' | 'kafka' | 'metrics' | 'database' | 'archive'

export interface StatusSummary {
  generated_at: string
  level: Exclude<HealthLevel, 'none'>
  title: string
  reasons: string[]
  live_age_seconds: number | null
  latency_24h: Latency
  latency_1h: Latency
  throughput: { ingest_per_s: number | null; metrics_per_s: number | null }
  lag_seconds: Record<string, number | null>
  stages: Record<Stage, HealthLevel>
  quality_1h: { checked: number; mismatched: number; out_of_order: number; ignored: number; accuracy: number | null }
  uptime: Record<'collector' | 'live' | 'archive', { uptime: number | null; hours: UptimeHour[] }>
  incidents: Incident[]
}

export type StatusRange = '1h' | '24h' | '7d'
export type SeriesRow = { t: string } & Record<string, number | string | null>

export interface StatusCharts {
  range: StatusRange
  bucket_seconds: number
  latency: SeriesRow[]      // network / processing / storage / p50 / p99（毫秒）
  throughput: SeriesRow[]   // ingest / metrics（条/秒）
  lag: SeriesRow[]          // 每个 consumer group 落后几秒
}

export const fetchStatus = () => getJson<StatusSummary>('/api/status')
export const fetchStatusCharts = (range: StatusRange) => getJson<StatusCharts>(`/api/status/charts?range=${range}`)
