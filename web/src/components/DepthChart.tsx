import { useId, useState } from 'react'
import type { Level } from '../api'
import { niceStep, ticks } from '../chart'
import { cents, usd } from '../format'
import { useWidth } from '../usePolling'

const H = 260
const M = { top: 34, right: 12, bottom: 26, left: 48 }   // 上边留出放 Bids / Asks 合计的地方
const GUTTER = 64        // 中间写价差的宽度：两边都从自己的最优价开始画，价差再大也不会空出一大片
const NARROW = 420       // 比这窄就少画几个价格刻度

interface Step { d: number; p: number; prev: number; cum: number; flagged: number; flaggedCum: number }

// 累计深度：从最优价往外，每一档把挂单金额（价格 × 份数）累加起来。d 是离本侧最优价多远，超过 range 的不画
// flaggedAt：这一档可疑的金额，flaggedCum 是到这一档为止可疑的金额
function cumulative(levels: Level[], range: number, flaggedAt: (p: number) => number): Step[] {
  const best = levels[0]?.[0]
  let total = 0
  let flaggedTotal = 0
  return levels.filter(([p]) => Math.abs(p - best) <= range + 1e-9).map(([p, s]) => {
    const prev = total
    total += p * s
    const flagged = Math.min(flaggedAt(p), p * s)
    flaggedTotal += flagged
    return { d: Math.abs(p - best), p, prev, cum: total, flagged, flaggedCum: flaggedTotal }
  })
}

// 平方根刻度上的纵轴刻度：按高度大致等距取点，再取整成好看的数
function depthTicks(max: number) {
  const out = [0, 0.0625, 0.25, 0.5625].map((f) => {
    const v = f * max
    if (v === 0) return 0
    const step = niceStep(v, 2)
    return Math.round(v / step) * step
  })
  return [...new Set([...out.filter((v) => v < max * 0.9), max])]
}

// 深度图：买盘在左、卖盘在右，都从自己的最优价往外画，纵轴是「从最优价到这个价位一共挂了多少美元」。
// 纵轴用平方根刻度：两边差很多时小的一边也看得清，刻度上标的仍是真实美元。
// flagged：传了就把每档可疑的金额换颜色（累计图里，一档的金额就是从它的价格一直铺到图边上的一层）
export default function DepthChart({ bids, asks, range, flagged }: {
  bids: Level[]; asks: Level[]; range: number; flagged?: (side: 'bid' | 'ask', price: number) => number
}) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const [hoverX, setHoverX] = useState<number | null>(null)
  const gradient = useId()

  const bestBid = bids[0]?.[0]
  const bestAsk = asks[0]?.[0]
  if (bestBid == null && bestAsk == null) {
    return <div ref={ref} className="chart-empty" style={{ height: H }}>The order book is empty</div>
  }
  const b = cumulative(bids, range, (p) => flagged?.('bid', p) ?? 0)
  const a = cumulative(asks, range, (p) => flagged?.('ask', p) ?? 0)
  const totalBid = b.at(-1)?.cum ?? 0
  const totalAsk = a.at(-1)?.cum ?? 0
  const rawMax = Math.max(totalBid, totalAsk, 1)
  const maxCum = Math.ceil(rawMax / niceStep(rawMax, 4)) * niceStep(rawMax, 4)   // 纵轴顶取整，如 $3,000

  const W = Math.max(width, 300)
  const iw = W - M.left - M.right
  const ih = H - M.top - M.bottom
  const center = M.left + iw / 2
  const half = (iw - GUTTER) / 2
  const xBid = (d: number) => center - GUTTER / 2 - (d / range) * half
  const xAsk = (d: number) => center + GUTTER / 2 + (d / range) * half
  // 价格只能在 0～100¢ 之间，范围超出时画到 0 或 100¢ 为止
  const edgeBid = bestBid != null ? Math.min(range, bestBid) : range
  const edgeAsk = bestAsk != null ? Math.min(range, 1 - bestAsk) : range
  const y = (v: number) => M.top + (1 - Math.sqrt(Math.max(v, 0) / maxCum)) * ih
  const base = y(0)

  // 阶梯线：从最优价的第一档开始，每到一档累计金额跳一级，画到图的边上为止
  const linePath = (pts: Step[], x: (d: number) => number, edge: number) => {
    if (!pts.length) return ''
    let path = `M${x(pts[0].d)},${base}L${x(pts[0].d)},${y(pts[0].cum)}`
    for (let i = 1; i < pts.length; i++) path += `L${x(pts[i].d)},${y(pts[i - 1].cum)}L${x(pts[i].d)},${y(pts[i].cum)}`
    return path + `L${x(edge)},${y(pts.at(-1)!.cum)}`
  }
  const areaPath = (pts: Step[], x: (d: number) => number, edge: number) =>
    pts.length ? `${linePath(pts, x, edge)}L${x(edge)},${base}Z` : ''

  const bands = (pts: Step[], x: (d: number) => number, edge: number) => pts.filter((l) => l.flagged > 0).map((l) => (
    <rect key={l.p} x={Math.min(x(l.d), x(edge))} width={Math.abs(x(edge) - x(l.d))}
      y={y(l.cum)} height={Math.max(y(l.cum - l.flagged) - y(l.cum), 1)} className="depth-flagged" />
  ))

  const yTicks = depthTicks(maxCum)
  // 价格刻度取整数，最优价单独标；贴着图边的文字往里对齐，不被截断
  const priceTicks = (lo: number, hi: number, best: number) =>
    [best, ...ticks(lo, hi, W < NARROW ? 2 : 4).filter((p) => Math.abs(p - best) > range * 0.15)]
  const anchor = (x: number, fallback: 'start' | 'middle' | 'end') =>
    x < M.left + 18 ? 'start' : x > W - M.right - 18 ? 'end' : fallback

  // 鼠标所在价格的累计深度
  let tip: { side: 'Bids' | 'Asks'; price: number; cum: number; flagged: number } | null = null
  if (hoverX != null) {
    const dBid = ((xBid(0) - hoverX) / half) * range
    const dAsk = ((hoverX - xAsk(0)) / half) * range
    if (bestBid != null && dBid >= 0 && dBid <= edgeBid) {
      const hit = b.filter((l) => l.d <= dBid).at(-1)
      if (hit) tip = { side: 'Bids', price: bestBid - dBid, cum: hit.cum, flagged: hit.flaggedCum }
    } else if (bestAsk != null && dAsk >= 0 && dAsk <= edgeAsk) {
      const hit = a.filter((l) => l.d <= dAsk).at(-1)
      if (hit) tip = { side: 'Asks', price: bestAsk + dAsk, cum: hit.cum, flagged: hit.flaggedCum }
    }
  }

  return (
    <div ref={ref} className="chart">
      <svg width={W} height={H} role="img" aria-label="Cumulative order book depth, bids on the left and asks on the right"
        onMouseMove={(e) => setHoverX(e.clientX - e.currentTarget.getBoundingClientRect().left)}
        onMouseLeave={() => setHoverX(null)}>
        <defs>
          {(['bids', 'asks'] as const).map((s) => (
            <linearGradient key={s} id={`${gradient}-${s}`} x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" className={`depth-stop ${s}`} stopOpacity={0.26} />
              <stop offset="100%" className={`depth-stop ${s}`} stopOpacity={0.05} />
            </linearGradient>
          ))}
        </defs>

        {yTicks.map((v) => (
          <g key={v}>
            <line x1={M.left} x2={W - M.right} y1={y(v)} y2={y(v)} className="grid" />
            <text x={M.left - 8} y={y(v)} className="axis" textAnchor="end" dominantBaseline="middle">{usd(v)}</text>
          </g>
        ))}

        <path d={areaPath(b, xBid, edgeBid)} fill={`url(#${gradient}-bids)`} />
        <path d={areaPath(a, xAsk, edgeAsk)} fill={`url(#${gradient}-asks)`} />
        {bands(b, xBid, edgeBid)}
        {bands(a, xAsk, edgeAsk)}
        <path d={linePath(b, xBid, edgeBid)} className="depth-line bids" />
        <path d={linePath(a, xAsk, edgeAsk)} className="depth-line asks" />

        {bestBid != null && priceTicks(bestBid - edgeBid, bestBid, bestBid).map((p) => (
          <text key={`b${p}`} x={xBid(bestBid - p)} y={H - 6} className="axis"
            textAnchor={anchor(xBid(bestBid - p), p === bestBid ? 'end' : 'middle')}>{cents(p)}</text>
        ))}
        {bestAsk != null && priceTicks(bestAsk, bestAsk + edgeAsk, bestAsk).map((p) => (
          <text key={`a${p}`} x={xAsk(p - bestAsk)} y={H - 6} className="axis"
            textAnchor={anchor(xAsk(p - bestAsk), p === bestAsk ? 'start' : 'middle')}>{cents(p)}</text>
        ))}

        {/* 两边的合计直接写出来，不只靠颜色区分 */}
        <rect x={M.left} y={7} width={10} height={10} rx={2} className="depth-key bids" />
        <text x={M.left + 15} y={16} className="depth-title">Bids <tspan className="depth-total">{usd(totalBid)}</tspan></text>
        <rect x={W - M.right - 10} y={7} width={10} height={10} rx={2} className="depth-key asks" />
        <text x={W - M.right - 15} y={16} className="depth-title" textAnchor="end">Asks <tspan className="depth-total">{usd(totalAsk)}</tspan></text>
        {W >= NARROW && <text x={center} y={16} className="axis" textAnchor="middle">Cumulative, √ scale</text>}

        {bestBid != null && bestAsk != null && (
          <>
            <text x={center} y={M.top + ih / 2 - 7} className="axis" textAnchor="middle">Spread</text>
            <text x={center} y={M.top + ih / 2 + 9} className="depth-spread" textAnchor="middle">{cents(bestAsk - bestBid)}</text>
          </>
        )}

        {tip && hoverX != null && (
          <>
            <line x1={hoverX} x2={hoverX} y1={M.top} y2={base} className="crosshair" />
            <circle cx={hoverX} cy={y(tip.cum)} r={4} className={`depth-dot ${tip.side === 'Bids' ? 'bids' : 'asks'}`} />
          </>
        )}
      </svg>
      {tip && hoverX != null && (
        <div className="tooltip" style={{ left: Math.min(hoverX + 12, W - 170), top: M.top }}>
          <div className="tt-time">{tip.side} {tip.side === 'Bids' ? '≥' : '≤'} {cents(tip.price)}</div>
          <div><b>{usd(tip.cum)}</b> resting</div>
          {tip.flagged > 0 && <div><b>{usd(tip.flagged)}</b> flagged</div>}
        </div>
      )}
    </div>
  )
}
