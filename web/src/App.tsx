import { useEffect, useState } from 'react'
import { fetchMarkets, type Market } from './api'

const REFRESH_MS = 5000

const pct = (x: number | null) => (x == null ? '—' : `${(x * 100).toFixed(1)}%`)

// 60 分钟价格变化（log-odds 差）。只显示方向和大小，正数涨、负数跌
function Change({ x }: { x: number | null }) {
  if (x == null) return <span className="muted">—</span>
  const cls = x > 0.005 ? 'up' : x < -0.005 ? 'down' : 'muted'
  return <span className={cls}>{x > 0 ? '+' : ''}{x.toFixed(2)}</span>
}

function ago(iso: string) {
  const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000))
  return s < 60 ? `${s} 秒前` : s < 3600 ? `${Math.round(s / 60)} 分钟前` : `${Math.round(s / 3600)} 小时前`
}

export default function App() {
  const [markets, setMarkets] = useState<Market[]>([])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const load = () =>
      fetchMarkets()
        .then((m) => { setMarkets(m); setError(null) })
        .catch((e) => setError(String(e)))
    load()
    const timer = setInterval(load, REFRESH_MS)
    return () => clearInterval(timer)
  }, [])

  return (
    <main>
      <h1>Polymarket 订单簿分析</h1>
      <p className="muted">科技 / AI 类市场的实时盘口，每 {REFRESH_MS / 1000} 秒刷新。数据来自 Polymarket 公开接口，与 Polymarket 无关联，不构成投资建议。</p>
      {error && <p className="error">加载失败：{error}</p>}
      <table>
        <thead>
          <tr>
            <th>市场</th>
            <th>选项</th>
            <th className="num">概率</th>
            <th className="num">买一 / 卖一</th>
            <th className="num">60 分钟变化</th>
            <th className="num">更新</th>
          </tr>
        </thead>
        <tbody>
          {markets.map((m) => (
            <tr key={m.market_id}>
              <td>{m.question}</td>
              <td>{m.outcome}</td>
              <td className="num">{pct(m.mid)}</td>
              <td className="num">{pct(m.best_bid)} / {pct(m.best_ask)}</td>
              <td className="num"><Change x={m.logodds_chg_60m} /></td>
              <td className="num muted">{ago(m.updated_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </main>
  )
}
