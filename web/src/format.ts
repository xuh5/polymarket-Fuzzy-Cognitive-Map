// 页面上数字和时间的显示格式

const clamp = (p: number) => Math.min(0.999, Math.max(0.001, p))
const logit = (p: number) => Math.log(p / (1 - p))
const sigmoid = (x: number) => 1 / (1 + Math.exp(-x))

// 概率，如 7.9%
export const pct = (p: number | null) => (p == null ? '—' : `${(p * 100).toFixed(1)}%`)

// 价格按美分显示（Polymarket 的习惯：0.073 美元 = 7.3¢）
export const cents = (p: number | null) => (p == null ? '—' : `${(p * 100).toFixed(1)}¢`)

// 后端给的是 60 分钟 log-odds 变化，换算成概率变了几个百分点，用户更好理解：
// 60 分钟前的 log-odds = 现在的 log-odds − 变化量（后端算的时候价格也夹在 [0.001, 0.999]）
export function pointsChange(mid: number | null, logoddsChg: number | null): number | null {
  if (mid == null || logoddsChg == null) return null
  const before = sigmoid(logit(clamp(mid)) - logoddsChg)
  return (clamp(mid) - before) * 100
}

export function secondsAgo(iso: string): number {
  return Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000)
}

export function ago(iso: string): string {
  const s = Math.round(secondsAgo(iso))
  if (s < 60) return `${s}s ago`
  if (s < 3600) return `${Math.round(s / 60)}m ago`
  return `${Math.round(s / 3600)}h ago`
}

export function endDate(iso: string | null): string | null {
  if (!iso) return null
  return new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

export function clock(iso: string): string {
  return new Date(iso).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })
}

// 美元金额，大数用 k / M
export function usd(x: number | null): string {
  if (x == null) return '—'
  if (Math.abs(x) >= 1e6) return `$${(x / 1e6).toFixed(1)}M`
  if (Math.abs(x) >= 1e4) return `$${(x / 1e3).toFixed(1)}k`
  return `$${x.toLocaleString('en-US', { maximumFractionDigits: 0 })}`
}

export const shares = (x: number) => x.toLocaleString('en-US', { maximumFractionDigits: 0 })

// 延迟：1 秒以下用毫秒，以上用秒
export function ms(x: number | null): string {
  if (x == null) return '—'
  return Math.abs(x) < 1000 ? `${Math.round(x)} ms` : `${(x / 1000).toFixed(1)} s`
}

// 时长：64 s、27 m、2 h 56 m
export function duration(seconds: number | null): string {
  if (seconds == null) return '—'
  const s = Math.round(seconds)
  if (s < 90) return `${s} s`
  if (s < 3600) return `${Math.round(s / 60)} m`
  const h = Math.floor(s / 3600)
  const m = Math.round((s % 3600) / 60)
  return m ? `${h} h ${m} m` : `${h} h`
}

// 每秒多少条
export const rate = (x: number | null) =>
  x == null ? '—' : `${x.toLocaleString('en-US', { maximumFractionDigits: x < 10 ? 1 : 0 })}/s`

// Oct 29
export const day = (iso: string) => new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })

// Oct 29, 4:00 PM
export const dayClock = (iso: string) => `${day(iso)}, ${clock(iso)}`
