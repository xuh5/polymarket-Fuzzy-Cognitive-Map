import type { PricePoint } from '../replay'
import { ticks } from '../chart'
import { cents, clock } from '../format'
import { useWidth } from '../usePolling'

const H = 240
const M = { top: 12, right: 12, bottom: 26, left: 48 }

export interface TradeDot { t: number; price: number; side: 'BUY' | 'SELL' }

// 回放的价格图：整段的买一、卖一、中间价（阶梯线）+ 成交点；竖线是播放头，点或拖动跳到那一刻。
// 不用 Lightweight Charts：它的时间最细到秒，回放要毫秒
export default function ReplayChart({ start, end, points, trades, t, onSeek }: {
  start: number
  end: number
  points: PricePoint[]
  trades: TradeDot[]
  t: number
  onSeek: (t: number) => void
}) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const W = Math.max(width, 280)
  const iw = W - M.left - M.right
  const ih = H - M.top - M.bottom

  // 纵轴：整段买一卖一和成交价的范围，至少 2¢，上下留一点边
  const vals = [...points.flatMap((p) => [p.bid, p.ask]), ...trades.map((d) => d.price)]
    .filter((v): v is number => v != null)
  let lo = vals.length ? Math.min(...vals) : 0.4
  let hi = vals.length ? Math.max(...vals) : 0.6
  if (hi - lo < 0.02) { const c = (hi + lo) / 2; lo = c - 0.01; hi = c + 0.01 }
  const pad = (hi - lo) * 0.1
  lo = Math.max(0, lo - pad)
  hi = Math.min(1, hi + pad)

  const x = (v: number) => M.left + ((v - start) / (end - start)) * iw
  const y = (v: number) => M.top + (1 - (v - lo) / (hi - lo)) * ih

  // 阶梯线：值保持到下一个点；不可用（null）的地方断开
  const step = (pick: (p: PricePoint) => number | null) => {
    let d = ''
    let pen = false
    points.forEach((p, i) => {
      const v = pick(p)
      if (v == null) { pen = false; return }
      const next = i + 1 < points.length ? points[i + 1].t : end
      d += `${pen ? 'V' : `M${x(p.t).toFixed(1)},`}${y(v).toFixed(1)}H${x(Math.min(next, end)).toFixed(1)}`
      pen = true
    })
    return d
  }
  const mid = (p: PricePoint) => (p.bid != null && p.ask != null ? (p.bid + p.ask) / 2 : null)

  const seek = (e: React.PointerEvent<SVGSVGElement>) => {
    const box = e.currentTarget.getBoundingClientRect()
    const f = Math.min(1, Math.max(0, (e.clientX - box.left - M.left) / iw))
    onSeek(start + f * (end - start))
  }

  const xTicks = [0, 0.25, 0.5, 0.75, 1].map((f) => start + f * (end - start))
  const px = x(Math.min(Math.max(t, start), end))
  return (
    <div ref={ref} className="chart">
      <svg width={W} height={H} className="replay-chart" role="img" aria-label="Price during the replay window"
        onPointerDown={(e) => { e.currentTarget.setPointerCapture(e.pointerId); seek(e) }}
        onPointerMove={(e) => { if (e.buttons) seek(e) }}>
        {ticks(lo, hi, 4).map((v) => (
          <g key={v}>
            <line x1={M.left} x2={M.left + iw} y1={y(v)} y2={y(v)} className="grid" />
            <text x={M.left - 8} y={y(v)} className="axis" textAnchor="end" dominantBaseline="middle">{cents(v)}</text>
          </g>
        ))}
        {xTicks.map((v, i) => (
          <text key={i} x={x(v)} y={H - 6} className="axis"
            textAnchor={i === 0 ? 'start' : i === xTicks.length - 1 ? 'end' : 'middle'}>
            {clock(new Date(v).toISOString())}
          </text>
        ))}
        <path d={step((p) => p.bid)} className="replay-edge" />
        <path d={step((p) => p.ask)} className="replay-edge" />
        <path d={step(mid)} className="replay-mid" />
        {trades.map((d, i) => (
          <circle key={i} cx={x(d.t)} cy={y(d.price)} r={4} className={`trade-dot ${d.side === 'BUY' ? 'buy' : 'sell'}`} />
        ))}
        {/* 播放头之后的部分淡一点：已经放过的 / 还没放到的一眼分开 */}
        <rect x={px} y={M.top} width={Math.max(0, M.left + iw - px)} height={ih} className="replay-future" />
        <line x1={px} x2={px} y1={M.top - 4} y2={M.top + ih} className="playhead" />
      </svg>
    </div>
  )
}
