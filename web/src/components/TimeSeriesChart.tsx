import { useId, useState } from 'react'
import type { SeriesRow } from '../api'
import { ticks } from '../chart'
import { useWidth } from '../usePolling'

export interface Series {
  key: string
  label: string
  color: string          // CSS 颜色（用 var(--s1) 这类变量）
  stack?: boolean        // true：和其他 stack 的系列叠成面积；false：画成线
  dashed?: boolean
}

const H = 220
const M = { top: 12, bottom: 26, left: 52 }
const LABEL_W = 92            // 右边留给线末端名字的宽度
const NARROW = 520            // 比这窄就不在末端标名字，只靠图例和提示框
const LABEL_GAP = 14          // 末端名字之间至少隔这么多像素

// 多条线 / 堆叠面积的时间序列图：一个纵轴（单位相同的才放一起），鼠标移上去显示这一刻每条的值。
// 2 条以上有图例；不太窄时线末端直接写名字，颜色不是唯一的区分方式
export default function TimeSeriesChart({
  rows, series, format, axisFormat = format, timeLabel, tipLabel = timeLabel, label, robust = false, bucketMs,
}: {
  rows: SeriesRow[]
  series: Series[]
  format: (v: number) => string          // 提示框里的值
  axisFormat?: (v: number) => string     // 纵轴刻度（默认同 format）
  timeLabel: (iso: string) => string     // 横轴刻度
  tipLabel?: (iso: string) => string     // 提示框里的时间（默认同横轴）
  label: string
  bucketMs?: number  // 每格多长；相邻两点隔得比 1.5 格还远，说明中间没数据，线和面积在这里断开
  robust?: boolean   // true：纵轴按线条值的第 95 百分位定，个别尖峰截掉（提示框里仍是真实值）
}) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const [hover, setHover] = useState<number | null>(null)
  const clipId = useId()

  const legend = series.length > 1 && (
    <div className="legend">
      {series.map((s) => (
        <span key={s.key}>
          <span className={`key ${s.stack ? '' : 'key-line'}${s.dashed ? ' dashed' : ''}`}
            style={{ background: s.dashed ? undefined : s.color, borderColor: s.color }} />
          {s.label}
        </span>
      ))}
    </div>
  )

  if (rows.length < 2) {
    return (
      <div ref={ref}>
        {legend}
        <div className="chart-empty" style={{ height: H }}>Not enough data in this range yet</div>
      </div>
    )
  }

  const W = Math.max(width, 280)
  const right = W < NARROW ? 12 : LABEL_W
  const iw = W - M.left - right
  const ih = H - M.top - M.bottom
  const ts = rows.map((r) => new Date(r.t).getTime())
  const t0 = ts[0]
  const t1 = ts[ts.length - 1]
  const num = (r: SeriesRow, k: string) => (typeof r[k] === 'number' ? Math.max(0, r[k] as number) : null)

  // 堆叠：每行从下往上累加，记下每个系列的下沿和上沿
  const stacked = series.filter((s) => s.stack)
  const bands = rows.map((r) => {
    let acc = 0
    const out: Record<string, [number, number]> = {}
    for (const s of stacked) {
      const v = num(r, s.key) ?? 0
      out[s.key] = [acc, acc + v]
      acc += v
    }
    return out
  })
  const top = (i: number, s: Series) => (s.stack ? bands[i][s.key][1] : num(rows[i], s.key))
  const all = rows.flatMap((_, i) => series.map((s) => top(i, s) ?? 0))
  let hi = Math.max(1e-9, ...all) * 1.08
  if (robust) {
    const stackTop = Math.max(0, ...rows.map((_, i) => (stacked.length ? bands[i][stacked[stacked.length - 1].key][1] : 0)))
    const lines = rows.flatMap((_, i) => series.filter((s) => !s.stack).map((s) => top(i, s)))
      .filter((v): v is number => v != null).sort((a, b) => a - b)
    const p95 = lines.length ? lines[Math.floor(lines.length * 0.95)] : 0
    hi = Math.min(hi, Math.max(1e-9, stackTop, p95) * 1.15)
  }
  const clipped = all.some((v) => v > hi)
  const yTicks = ticks(0, hi, 4)
  const yMax = Math.max(hi, yTicks[yTicks.length - 1])

  const x = (t: number) => M.left + ((t - t0) / (t1 - t0 || 1)) * iw
  const y = (v: number) => M.top + (1 - v / yMax) * ih

  // 没有数据的地方断开，不连成一条假线：值为 null，或者和上一个点隔得太远（中间整段没数据）
  const gapBefore = (i: number) => i > 0 && bucketMs != null && ts[i] - ts[i - 1] > bucketMs * 1.5
  const segments: number[][] = []
  rows.forEach((_, i) => {
    if (i === 0 || gapBefore(i)) segments.push([])
    segments[segments.length - 1].push(i)
  })
  const linePath = (s: Series) => {
    let d = ''
    let pen = false
    rows.forEach((r, i) => {
      const v = num(r, s.key)
      if (v == null || gapBefore(i)) pen = false
      if (v == null) return
      d += `${pen ? 'L' : 'M'}${x(ts[i]).toFixed(1)},${y(v).toFixed(1)}`
      pen = true
    })
    return d
  }
  const areaPath = (s: Series) => segments.map((seg) =>
    seg.map((i, j) => `${j ? 'L' : 'M'}${x(ts[i]).toFixed(1)},${y(bands[i][s.key][1]).toFixed(1)}`).join('') +
    seg.slice().reverse().map((i) => `L${x(ts[i]).toFixed(1)},${y(bands[i][s.key][0]).toFixed(1)}`).join('') + 'Z',
  ).join('')

  // 末端名字：放在每个系列最后一个有值的点旁边，挤在一起时往上下推开
  const ends = series
    .map((s) => {
      let i = rows.length - 1
      while (i >= 0 && top(i, s) == null) i--
      if (i < 0) return null
      const v = s.stack ? (bands[i][s.key][0] + bands[i][s.key][1]) / 2 : top(i, s)!
      return { s, y: y(Math.min(v, yMax)) }
    })
    .filter((e): e is { s: Series; y: number } => e != null)
    .sort((a, b) => a.y - b.y)
  for (let i = 1; i < ends.length; i++) ends[i].y = Math.max(ends[i].y, ends[i - 1].y + LABEL_GAP)
  const overflow = ends.length ? ends[ends.length - 1].y - (M.top + ih) : 0
  if (overflow > 0) ends.forEach((e) => { e.y -= overflow })

  // 窄的时候只标头、中、尾三个时间，免得挤在一起
  const xTicks = (W < NARROW ? [0, 0.5, 1] : [0, 0.25, 0.5, 0.75, 1]).map((f) => t0 + f * (t1 - t0))

  const onMove = (e: React.PointerEvent<SVGSVGElement>) => {
    const box = e.currentTarget.getBoundingClientRect()
    const t = t0 + ((e.clientX - box.left - M.left) / iw) * (t1 - t0)
    let best = 0
    ts.forEach((v, i) => { if (Math.abs(v - t) < Math.abs(ts[best] - t)) best = i })
    setHover(best)
  }

  const hx = hover == null ? 0 : x(ts[hover])
  return (
    <div>
      {legend}
      <div ref={ref} className="chart">
        <svg width={W} height={H} onPointerMove={onMove} onPointerLeave={() => setHover(null)}
          role="img" aria-label={label}>
          {yTicks.map((v) => (
            <g key={v}>
              <line x1={M.left} x2={M.left + iw} y1={y(v)} y2={y(v)} className="grid" />
              <text x={M.left - 8} y={y(v)} className="axis" textAnchor="end" dominantBaseline="middle">
                {axisFormat(v)}
              </text>
            </g>
          ))}
          {xTicks.map((t, i) => (
            <text key={t} x={x(t)} y={H - 6} className="axis"
              textAnchor={i === 0 ? 'start' : i === xTicks.length - 1 ? 'end' : 'middle'}>
              {timeLabel(new Date(t).toISOString())}
            </text>
          ))}
          <defs>
            <clipPath id={clipId}><rect x={M.left} y={M.top - 2} width={iw} height={ih + 2} /></clipPath>
          </defs>
          {clipped && (
            <text x={M.left + iw} y={M.top + 10} className="axis" textAnchor="end">Peaks clipped · hover for values</text>
          )}
          <g clipPath={`url(#${clipId})`}>
          {/* 面积之间描一圈和底色一样的边，叠在一起的几层分得开 */}
          {stacked.map((s) => <path key={s.key} d={areaPath(s)} className="ts-area" style={{ fill: s.color }} />)}
          {series.filter((s) => !s.stack).map((s) => (
            <path key={s.key} d={linePath(s)} className={`ts-line${s.dashed ? ' dashed' : ''}`}
              style={{ stroke: s.color }} />
          ))}
          </g>
          {W >= NARROW && ends.map(({ s, y: ly }) => (
            <text key={s.key} x={M.left + iw + 8} y={ly} className="ts-end" dominantBaseline="middle">
              {s.label}
            </text>
          ))}
          {hover != null && (
            <>
              <line x1={hx} x2={hx} y1={M.top} y2={M.top + ih} className="crosshair" />
              {series.map((s) => {
                const v = top(hover, s)
                return v == null || v > yMax ? null
                  : <circle key={s.key} cx={hx} cy={y(v)} r={4} className="ts-dot" style={{ fill: s.color }} />
              })}
            </>
          )}
        </svg>
        {hover != null && (
          <div className="tooltip" style={{ left: Math.min(hx + 12, W - 170), top: M.top }}>
            <div className="tt-time">{tipLabel(rows[hover].t)}</div>
            {series.map((s) => {
              const v = rows[hover][s.key]
              return (
                <div key={s.key}>
                  <span className={`key ${s.stack ? '' : 'key-line'}`} style={{ background: s.color }} />
                  {s.label} <b>{typeof v === 'number' ? format(v) : '—'}</b>
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}
