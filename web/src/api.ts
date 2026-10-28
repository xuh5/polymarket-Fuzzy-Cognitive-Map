// 调用后端 API。本机开发时 VITE_API_BASE 不设，走 vite 的 /api 转发；
// 部署到 Cloudflare Pages 时设成后端的公网地址（Cloudflare Tunnel 的网址）
const BASE = import.meta.env.VITE_API_BASE ?? ''

// 和后端 /api/markets 返回的字段一一对应（pmdata/api/queries.py 的 list_markets）
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

async function getJson<T>(path: string): Promise<T> {
  const r = await fetch(BASE + path)
  if (!r.ok) throw new Error(`${path} 返回 ${r.status}`)
  return r.json()
}

export const fetchMarkets = () => getJson<Market[]>('/api/markets')
