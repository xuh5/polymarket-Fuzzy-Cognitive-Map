import { useState } from 'react'
import type { Level } from '../api'
import { niceStep, ticks } from '../chart'
import { cents, usd } from '../format'
import { useWidth } from '../usePolling'

const H = 260
const M = { top: 28, right: 12, bottom: 26, left: 52 }   // 上边留出放 Bids / Asks 标签的地方

// 累计深度：从最优价往外，每一档把挂单金额（价格 × 份数）累加起来
function cumulative(levels: Level[], inRange: (p: number) => boolean) {
  let total = 0
  return levels.filter(([p]) => inRange(p)).map(([p, s]) => ({ p, cum: (total += p * s) }))
}

// 深度图：横轴是价格，买盘在左、卖盘在右，纵轴是「从最优价到这个价位一共挂了多少美元」。
// 一眼看出盘口两边厚不厚、哪边薄
export default function DepthChart({ bids, asks, range }: { bids: Level[]; asks: Level[]; range: number }) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const [hoverX, setHoverX] = useState<number | null>(null)

  const bestBid = bids[0]?.[0]
  const bestAsk = asks[0]?.[0]
  if (bestBid == null && bestAsk == null) {
    return <div ref={ref} className="chart-empty" style={{ height: H }}>The order book is empty</div>
  }
  const center = bestBid != null && bestAsk != null ? (bestBid + bestAsk) / 2 : (bestBid ?? bestAsk)!
  const lo = Math.max(0, center - range)
  const hi = Math.min(1, center + range)
  const b = cumulative(bids, (p) => p >= lo)
  const a = cumulative(asks, (p) => p <= hi)
  const rawMax = Math.max(b.at(-1)?.cum ?? 0, a.at(-1)?.cum ?? 0, 1)
  const maxCum = Math.ceil(rawMax / niceStep(rawMax, 4)) * niceStep(rawMax, 4)   // 纵轴顶取整，如 $3,000

  const W = Math.max(width, 300)
  const iw = W - M.left - M.right
  const ih = H - M.top - M.bottom
  const x = (p: number) => M.left + ((p - lo) / (hi - lo)) * iw
  const y = (v: number) => M.top + (1 - v / maxCum) * ih
  const base = y(0)

  // 阶梯形：每到一档价格，累计金额跳一级；画到图的边上为止
  const stepPath = (pts: { p: number; cum: number }[], edge: number) => {
    if (!pts.length) return ''
    let d = `M${x(pts[0].p)},${base}L${x(pts[0].p)},${y(pts[0].cum)}`
    for (let i = 1; i < pts.length; i++) d += `L${x(pts[i].p)},${y(pts[i - 1].cum)}L${x(pts[i].p)},${y(pts[i].cum)}`
    return d + `L${x(edge)},${y(pts.at(-1)!.cum)}L${x(edge)},${base}Z`
  }

  const yTicks = ticks(0, maxCum, 4)
  const xTicks = ticks(lo, hi, 6)

  // 鼠标所在价格的累计深度：在买一左边看买盘，在卖一右边看卖盘
  let tip: { side: 'Bids' | 'Asks'; price: number; cum: number } | null = null
  if (hoverX != null) {
    const price = lo + ((hoverX - M.left) / iw) * (hi - lo)
    if (bestBid != null && price <= bestBid) {
      const hit = b.filter((l) => l.p >= price).at(-1)
      if (hit) tip = { side: 'Bids', price, cum: hit.cum }
    } else if (bestAsk != null && price >= bestAsk) {
      const hit = a.filter((l) => l.p <= price).at(-1)
      if (hit) tip = { side: 'Asks', price, cum: hit.cum }
    }
  }

  return (
    <div ref={ref} className="chart">
      <svg width={W} height={H} role="img" aria-label="Cumulative order book depth"
        onMouseMove={(e) => setHoverX(e.clientX - e.currentTarget.getBoundingClientRect().left)}
        onMouseLeave={() => setHoverX(null)}>
        {yTicks.map((v) => (
          <g key={v}>
            <line x1={M.left} x2={W - M.right} y1={y(v)} y2={y(v)} className="grid" />
            <text x={M.left - 8} y={y(v)} className="axis" textAnchor="end" dominantBaseline="middle">{usd(v)}</text>
          </g>
        ))}
        {xTicks.map((p, i) => (
          <text key={i} x={x(p)} y={H - 6} className="axis" textAnchor="middle">{cents(p)}</text>
        ))}
        <path d={stepPath(b, lo)} className="depth bids" />
        <path d={stepPath(a, hi)} className="depth asks" />
        <line x1={x(center)} x2={x(center)} y1={M.top} y2={base} className="mid-line" />
        {/* 直接标出哪边是买盘、哪边是卖盘，不只靠颜色 */}
        <text x={M.left} y={14} className="series-label bids">Bids</text>
        <text x={W - M.right} y={14} className="series-label asks" textAnchor="end">Asks</text>
        {tip && hoverX != null && <line x1={hoverX} x2={hoverX} y1={M.top} y2={base} className="crosshair" />}
      </svg>
      {tip && hoverX != null && (
        <div className="tooltip" style={{ left: Math.min(hoverX + 12, W - 170), top: M.top }}>
          <div className="tt-time">{tip.side} {tip.side === 'Bids' ? '≥' : '≤'} {cents(tip.price)}</div>
          <div><b>{usd(tip.cum)}</b> resting</div>
        </div>
      )}
    </div>
  )
}
