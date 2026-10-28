import { useState } from 'react'
import type { HistoryPoint } from '../api'
import { ticks } from '../chart'
import { cents, clock } from '../format'
import { useWidth } from '../usePolling'

const H = 260
const M = { top: 12, right: 12, bottom: 26, left: 44 }

// 中间价走势 + 买一到卖一之间的价差带。鼠标移上去显示那一刻的中间价、买一、卖一
export default function PriceChart({ points }: { points: HistoryPoint[] }) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const [hover, setHover] = useState<number | null>(null)
  const data = points.filter((p) => p.mid != null)

  if (data.length < 2) {
    return <div ref={ref} className="chart-empty" style={{ height: H }}>Not enough data in this range yet</div>
  }

  const W = Math.max(width, 300)
  const iw = W - M.left - M.right
  const ih = H - M.top - M.bottom
  const ts = data.map((p) => new Date(p.second).getTime())
  const t0 = ts[0]
  const t1 = ts[ts.length - 1]
  const vals = data.flatMap((p) => [p.best_bid, p.best_ask, p.mid]).filter((v): v is number => v != null)
  let lo = Math.min(...vals)
  let hi = Math.max(...vals)
  if (hi - lo < 0.02) {           // 至少 2¢ 的纵向范围，免得小抖动被放大
    const c = (hi + lo) / 2
    lo = c - 0.01
    hi = c + 0.01
  }
  const pad = (hi - lo) * 0.08
  lo = Math.max(0, lo - pad)
  hi = Math.min(1, hi + pad)

  const x = (t: number) => M.left + ((t - t0) / (t1 - t0 || 1)) * iw
  const y = (v: number) => M.top + (1 - (v - lo) / (hi - lo)) * ih

  const mid = data.map((p, i) => `${i ? 'L' : 'M'}${x(ts[i]).toFixed(1)},${y(p.mid!).toFixed(1)}`).join('')
  const banded = data.map((p, i) => ({ p, t: ts[i] })).filter(({ p }) => p.best_bid != null && p.best_ask != null)
  const band = banded.length > 1
    ? banded.map(({ p, t }, i) => `${i ? 'L' : 'M'}${x(t).toFixed(1)},${y(p.best_ask!).toFixed(1)}`).join('') +
      banded.slice().reverse().map(({ p, t }) => `L${x(t).toFixed(1)},${y(p.best_bid!).toFixed(1)}`).join('') + 'Z'
    : ''

  const yTicks = ticks(lo, hi, 4)
  const xTicks = [0, 0.25, 0.5, 0.75, 1].map((f) => t0 + f * (t1 - t0))

  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const box = e.currentTarget.getBoundingClientRect()
    const t = t0 + ((e.clientX - box.left - M.left) / iw) * (t1 - t0)
    let best = 0
    ts.forEach((v, i) => { if (Math.abs(v - t) < Math.abs(ts[best] - t)) best = i })
    setHover(best)
  }

  const h = hover == null ? null : data[hover]
  const hx = hover == null ? 0 : x(ts[hover])
  return (
    <div ref={ref} className="chart">
      <svg width={W} height={H} onMouseMove={onMove} onMouseLeave={() => setHover(null)}
        role="img" aria-label="Mid price with bid–ask band over time">
        {yTicks.map((v) => (
          <g key={v}>
            <line x1={M.left} x2={W - M.right} y1={y(v)} y2={y(v)} className="grid" />
            <text x={M.left - 8} y={y(v)} className="axis" textAnchor="end" dominantBaseline="middle">
              {cents(v)}
            </text>
          </g>
        ))}
        {xTicks.map((t, i) => (
          <text key={t} x={x(t)} y={H - 6} className="axis"
            textAnchor={i === 0 ? 'start' : i === xTicks.length - 1 ? 'end' : 'middle'}>
            {clock(new Date(t).toISOString())}
          </text>
        ))}
        {band && <path d={band} className="band" />}
        <path d={mid} className="line-mid" />
        {h && (
          <>
            <line x1={hx} x2={hx} y1={M.top} y2={M.top + ih} className="crosshair" />
            <circle cx={hx} cy={y(h.mid!)} r={4} className="dot-mid" />
          </>
        )}
      </svg>
      {h && (
        <div className="tooltip" style={{ left: Math.min(hx + 12, W - 150), top: M.top }}>
          <div className="tt-time">{clock(h.second)}</div>
          <div><span className="key key-mid" />Mid <b>{cents(h.mid)}</b></div>
          <div><span className="key key-band" />Bid / Ask <b>{cents(h.best_bid)} / {cents(h.best_ask)}</b></div>
        </div>
      )}
    </div>
  )
}
