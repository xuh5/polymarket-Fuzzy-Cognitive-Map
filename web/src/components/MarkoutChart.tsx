import { useState } from 'react'
import { ticks } from '../chart'
import { useWidth } from '../usePolling'

export interface MarkoutSeries {
  key: string
  label: string
  color: string                       // CSS 变量，如 var(--s1)
  values: (number | null)[]           // 和 steps 一一对应；null = 没有样本
  counts: number[]                    // 每个点用了几笔
}

const H = 240
const M = { top: 14, bottom: 28, left: 56 }
const LABEL_W = 110       // 右边留给线末端名字的宽度
const NARROW = 520        // 比这窄就不在末端标名字，只靠图例和提示框
const LABEL_GAP = 14

// markout 曲线：横轴是成交后的几个时刻（等距排，不按真实时间比例），纵轴是挂单方每股赚多少，0 线加粗。
// 每个时刻是一个独立的点，鼠标移到哪一列就显示这一列每条线的值和样本数
export default function MarkoutChart({ steps, series, format, label }: {
  steps: string[]
  series: MarkoutSeries[]
  format: (v: number) => string
  label: string
}) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const [hover, setHover] = useState<number | null>(null)

  const legend = series.length > 1 && (
    <div className="legend">
      {series.map((s) => (
        <span key={s.key}><span className="key key-line" style={{ background: s.color }} />{s.label}</span>
      ))}
    </div>
  )
  const vals = series.flatMap((s) => s.values).filter((v): v is number => v != null)
  if (!vals.length) {
    return (
      <div ref={ref}>
        {legend}
        <div className="chart-empty" style={{ height: H }}>No trades in this range yet</div>
      </div>
    )
  }

  const W = Math.max(width, 280)
  const right = W < NARROW ? 16 : LABEL_W
  const iw = W - M.left - right
  const ih = H - M.top - M.bottom
  // 纵轴一定包含 0，上下各留一点余量
  const lo0 = Math.min(0, ...vals)
  const hi0 = Math.max(0, ...vals)
  const pad = (hi0 - lo0 || 0.001) * 0.12
  const yTicks = ticks(lo0 - pad, hi0 + pad, 5)
  const lo = Math.min(lo0 - pad, yTicks[0])
  const hi = Math.max(hi0 + pad, yTicks[yTicks.length - 1])
  const step = iw / Math.max(steps.length - 1, 1)
  const x = (i: number) => M.left + i * step
  const y = (v: number) => M.top + (1 - (v - lo) / (hi - lo)) * ih

  const path = (s: MarkoutSeries) => {
    let d = ''
    let pen = false
    s.values.forEach((v, i) => {
      if (v == null) { pen = false; return }
      d += `${pen ? 'L' : 'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`
      pen = true
    })
    return d
  }

  // 末端名字：放在每条线最后一个有值的点旁边，挤在一起时往下推开
  const ends = series
    .map((s) => {
      let i = s.values.length - 1
      while (i >= 0 && s.values[i] == null) i--
      return i < 0 ? null : { s, y: y(s.values[i]!) }
    })
    .filter((e): e is { s: MarkoutSeries; y: number } => e != null)
    .sort((a, b) => a.y - b.y)
  for (let i = 1; i < ends.length; i++) ends[i].y = Math.max(ends[i].y, ends[i - 1].y + LABEL_GAP)

  const onMove = (e: React.PointerEvent<SVGSVGElement>) => {
    const box = e.currentTarget.getBoundingClientRect()
    const i = Math.round((e.clientX - box.left - M.left) / step)
    setHover(Math.min(steps.length - 1, Math.max(0, i)))
  }
  const hx = hover == null ? 0 : x(hover)

  return (
    <div>
      {legend}
      <div ref={ref} className="chart">
        <svg width={W} height={H} onPointerMove={onMove} onPointerLeave={() => setHover(null)} role="img" aria-label={label}>
          {yTicks.map((v) => (
            <g key={v}>
              <line x1={M.left} x2={M.left + iw} y1={y(v)} y2={y(v)} className={v === 0 ? 'grid zero' : 'grid'} />
              <text x={M.left - 8} y={y(v)} className="axis" textAnchor="end" dominantBaseline="middle">{format(v)}</text>
            </g>
          ))}
          {steps.map((s, i) => (
            <text key={s} x={x(i)} y={H - 8} className="axis"
              textAnchor={i === 0 ? 'start' : i === steps.length - 1 ? 'end' : 'middle'}>{s}</text>
          ))}
          {hover != null && <line x1={hx} x2={hx} y1={M.top} y2={M.top + ih} className="crosshair" />}
          {series.map((s) => <path key={s.key} d={path(s)} className="ts-line" style={{ stroke: s.color }} />)}
          {series.map((s) => s.values.map((v, i) => v == null ? null : (
            <circle key={`${s.key}/${i}`} cx={x(i)} cy={y(v)} r={hover === i ? 5 : 4} className="ts-dot" style={{ fill: s.color }} />
          )))}
          {W >= NARROW && ends.map(({ s, y: ly }) => (
            <text key={s.key} x={M.left + iw + 10} y={ly} className="ts-end" dominantBaseline="middle">{s.label}</text>
          ))}
        </svg>
        {hover != null && (
          <div className="tooltip" style={{ left: Math.min(hx + 12, W - 200), top: M.top }}>
            <div className="tt-time">{steps[hover]}</div>
            {series.map((s) => (
              <div key={s.key}>
                <span className="key key-line" style={{ background: s.color }} />
                {s.label} <b>{s.values[hover] == null ? '—' : format(s.values[hover]!)}</b>
                <span className="muted"> · {s.counts[hover]} trades</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
