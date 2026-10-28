import { useEffect, useState } from 'react'
import { fetchHistory, type HistoryPoint } from '../api'
import { clock, pct } from '../format'

const W = 240
const H = 56
const PAD = 4
const REFRESH_MS = 30000

// 过去 60 分钟的中间价走势（每分钟一个点）。鼠标移上去显示那一刻的时间和概率
export default function Sparkline({ marketId, direction }: { marketId: string; direction: 'up' | 'down' | 'flat' }) {
  const [points, setPoints] = useState<HistoryPoint[]>([])
  const [hover, setHover] = useState<number | null>(null)

  useEffect(() => {
    const load = () => fetchHistory(marketId, 60, 60).then(setPoints).catch(() => setPoints([]))
    load()
    const timer = setInterval(load, REFRESH_MS)
    return () => clearInterval(timer)
  }, [marketId])

  const data = points.filter((p) => p.mid != null) as (HistoryPoint & { mid: number })[]
  if (data.length < 2) return <div className="spark-empty">Not enough data yet</div>

  const t0 = new Date(data[0].second).getTime()
  const t1 = new Date(data[data.length - 1].second).getTime()
  const mids = data.map((p) => p.mid)
  let lo = Math.min(...mids)
  let hi = Math.max(...mids)
  // 几乎没动时给一个最小纵向范围，免得把 0.1 分的抖动放大成大起大落
  if (hi - lo < 0.01) {
    const c = (hi + lo) / 2
    lo = c - 0.005
    hi = c + 0.005
  }
  const x = (p: HistoryPoint) => PAD + ((new Date(p.second).getTime() - t0) / (t1 - t0 || 1)) * (W - 2 * PAD)
  const y = (m: number) => PAD + (1 - (m - lo) / (hi - lo)) * (H - 2 * PAD)
  const path = data.map((p, i) => `${i ? 'L' : 'M'}${x(p).toFixed(1)},${y(p.mid).toFixed(1)}`).join('')

  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const box = e.currentTarget.getBoundingClientRect()
    const mx = ((e.clientX - box.left) / box.width) * W
    let best = 0
    data.forEach((p, i) => {
      if (Math.abs(x(p) - mx) < Math.abs(x(data[best]) - mx)) best = i
    })
    setHover(best)
  }

  const h = hover == null ? null : data[hover]
  return (
    <div className="spark">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" onMouseMove={onMove} onMouseLeave={() => setHover(null)}
        role="img" aria-label="Probability over the last 60 minutes">
        <path d={path} className={`spark-line ${direction}`} vectorEffect="non-scaling-stroke" />
        {h && (
          <>
            <line x1={x(h)} x2={x(h)} y1={0} y2={H} className="spark-cross" vectorEffect="non-scaling-stroke" />
            <circle cx={x(h)} cy={y(h.mid)} r={3.5} className={`spark-dot ${direction}`} />
          </>
        )}
      </svg>
      <div className="spark-caption">{h ? `${clock(h.second)} · ${pct(h.mid)}` : 'Last 60 min'}</div>
    </div>
  )
}
