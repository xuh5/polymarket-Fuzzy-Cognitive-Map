import {
  AreaSeries, ColorType, createChart, CrosshairMode, HistogramSeries, LineSeries, LineType, TickMarkType,
  type IChartApi, type ISeriesApi, type Time, type UTCTimestamp,
} from 'lightweight-charts'
import { useEffect, useMemo, useRef, useState } from 'react'
import type { HistoryPoint, TradeBucket } from '../api'
import { cents, clock, shares } from '../format'

const H = 340
const VOL_H = 80     // 下面成交量那一栏的高度

// 颜色取自 index.css 的 :root 变量，和页面其他地方保持一致
const cssVar = (name: string) => getComputedStyle(document.documentElement).getPropertyValue(name).trim()
function alpha(hex: string, a: number) {
  const n = parseInt(hex.slice(1), 16)
  return `rgba(${n >> 16}, ${(n >> 8) & 255}, ${n & 255}, ${a})`
}

// 图表库按 UTC 显示时间；我们换成浏览器本地时间，和页面其他时间一致
const toLocal = (t: Time) => new Date((t as number) * 1000)
const toTime = (iso: string) => Math.floor(new Date(iso).getTime() / 1000) as UTCTimestamp

// 图表库的横轴按点的序号等距排，不按时间；后端只返回盘口有变化的时间格，所以把中间缺的格补上，
// 值沿用上一格（盘口没变，价格就停在那里），横轴才和真实时间成比例
function fillBuckets(points: HistoryPoint[], bucketSeconds: number) {
  const out: (HistoryPoint & { t: UTCTimestamp })[] = []
  for (const p of points) {
    const t = toTime(p.second)
    const prev = out[out.length - 1]
    for (let f = prev ? prev.t + bucketSeconds : t; f < t; f += bucketSeconds) {
      out.push({ ...prev, t: f as UTCTimestamp, second: new Date(f * 1000).toISOString() })
    }
    out.push({ ...p, t })
  }
  return out
}

// 上栏：中间价走势（面积）+ 买一、卖一两条细线；下栏：每格成交量，看涨往上、看跌往下。
// 滚轮缩放、拖动平移，双击时间轴复位；左上角显示鼠标所在时刻（不在图上时为最新 / 整段合计）的数值
export default function PriceChart({ points, bucketSeconds, trades, tradeBucketSeconds }: {
  points: HistoryPoint[]
  bucketSeconds: number
  trades: TradeBucket[]
  tradeBucketSeconds: number   // 成交的格子（至少 1 分钟），是 bucketSeconds 的整数倍，时间点对得上
}) {
  const box = useRef<HTMLDivElement>(null)
  const chart = useRef<IChartApi | null>(null)
  const series = useRef<{
    mid: ISeriesApi<'Area'>; bid: ISeriesApi<'Line'>; ask: ISeriesApi<'Line'>
    bull: ISeriesApi<'Histogram'>; bear: ISeriesApi<'Histogram'>
  } | null>(null)
  const fitted = useRef(false)
  const [hover, setHover] = useState<number | null>(null)   // 鼠标所在的时间（秒）

  // 图表只建一次，数据变了只换数据：重建会丢掉用户的缩放位置
  useEffect(() => {
    const accent = cssVar('--accent')
    const c = createChart(box.current!, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: cssVar('--muted'),
        fontFamily: "'Inter Variable', system-ui, sans-serif",
        fontSize: 11,
        panes: { separatorColor: cssVar('--border'), separatorHoverColor: 'transparent', enableResize: false },
      },
      grid: { vertLines: { visible: false }, horzLines: { color: cssVar('--border') } },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.18, bottom: 0.08 } },
      timeScale: {
        borderVisible: false, timeVisible: true, fixLeftEdge: true, fixRightEdge: true,
        tickMarkFormatter: (t: Time, type: TickMarkType) => (type <= TickMarkType.DayOfMonth
          ? toLocal(t).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
          : clock(toLocal(t).toISOString())),
      },
      localization: {
        timeFormatter: (t: Time) => toLocal(t).toLocaleString('en-US',
          { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit' }),
      },
      crosshair: {
        mode: CrosshairMode.Magnet,
        vertLine: { color: cssVar('--border-strong'), labelBackgroundColor: cssVar('--ink-2') },
        horzLine: { color: cssVar('--border-strong'), labelBackgroundColor: cssVar('--ink-2') },
      },
      handleScroll: { vertTouchDrag: false },
    })
    // 价格格式按系列设：两栏单位不同（美分 / 份数），不能用全局的 localization.priceFormatter
    const price = { type: 'custom', formatter: cents, minMove: 0.001 } as const
    const edge = { color: alpha(accent, 0.35), lineWidth: 1, lineType: LineType.WithSteps, priceFormat: price,
      priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false } as const
    const volume = { priceFormat: { type: 'custom', formatter: (v: number) => shares(Math.abs(v)), minMove: 1 },
      priceLineVisible: false, lastValueVisible: false } as const
    series.current = {
      bid: c.addSeries(LineSeries, edge),
      ask: c.addSeries(LineSeries, edge),
      mid: c.addSeries(AreaSeries, {
        lineColor: accent, lineWidth: 2, lineType: LineType.WithSteps, topColor: alpha(accent, 0.18), bottomColor: alpha(accent, 0),
        priceFormat: price, priceLineVisible: false, crosshairMarkerBorderColor: cssVar('--surface'),
        // 纵向至少 2¢，免得小抖动被放大成大起大落
        autoscaleInfoProvider: (original: () => { priceRange: { minValue: number; maxValue: number } } | null) => {
          const r = original()
          if (!r || r.priceRange.maxValue - r.priceRange.minValue >= 0.02) return r
          const mid = (r.priceRange.maxValue + r.priceRange.minValue) / 2
          return { ...r, priceRange: { minValue: mid - 0.01, maxValue: mid + 0.01 } }
        },
      }),
      bull: c.addSeries(HistogramSeries, { ...volume, color: cssVar('--up') }, 1),
      bear: c.addSeries(HistogramSeries, { ...volume, color: cssVar('--down') }, 1),
    }
    c.panes()[1].setHeight(VOL_H)
    c.subscribeCrosshairMove((p) => setHover(p.time == null ? null : (p.time as number)))
    chart.current = c
    return () => {
      c.remove()
      chart.current = null
      series.current = null
    }
  }, [])

  const filled = useMemo(() => fillBuckets(points, bucketSeconds), [points, bucketSeconds])

  // 值是 null（盘口不可用）的格传「空白点」，线在这里断开，不连成一条假线
  useEffect(() => {
    const s = series.current
    if (!s) return
    const line = (pick: (p: HistoryPoint) => number | null) =>
      filled.map((p) => (pick(p) == null ? { time: p.t } : { time: p.t, value: pick(p)! }))
    s.mid.setData(line((p) => p.mid))
    s.bid.setData(line((p) => p.best_bid))
    s.ask.setData(line((p) => p.best_ask))
    // 价格范围外的成交不画：横轴上没有对应的格，会把时间轴撑歪
    const first = filled[0]?.t ?? 0
    const last = filled[filled.length - 1]?.t ?? 0
    const inRange = trades.map((b) => ({ ...b, time: toTime(b.t) })).filter((b) => b.time >= first && b.time <= last)
    s.bull.setData(inRange.filter((b) => b.bullish > 0).map((b) => ({ time: b.time, value: b.bullish })))
    s.bear.setData(inRange.filter((b) => b.bearish > 0).map((b) => ({ time: b.time, value: -b.bearish })))
    // 只在第一次有数据时铺满；之后每 30 秒刷新不动用户的缩放
    if (!fitted.current && filled.some((p) => p.mid != null)) {
      chart.current!.timeScale().fitContent()
      fitted.current = true
    }
  }, [filled, trades])

  const withMid = points.filter((p) => p.mid != null)
  const shown = (hover != null && filled.find((p) => p.t === hover)) || withMid[withMid.length - 1]
  // 成交：鼠标所在的那一格；不在图上时是整段合计
  const vol = hover != null
    ? trades.find((b) => toTime(b.t) === Math.floor(hover / tradeBucketSeconds) * tradeBucketSeconds)
    : trades.reduce((a, b) => ({ ...a, trades: a.trades + b.trades, bullish: a.bullish + b.bullish,
      bearish: a.bearish + b.bearish }), { t: '', trades: 0, bullish: 0, bearish: 0 })

  return (
    <div className="chart" style={{ height: H }}>
      <div ref={box} style={{ height: H }} />
      {shown && (
        <div className="price-readout">
          <span><span className="key key-mid" />Mid <b>{cents(shown.mid)}</b></span>
          <span><span className="key key-edge" />Bid / Ask <b>{cents(shown.best_bid)} / {cents(shown.best_ask)}</b></span>
          <span>
            {hover != null ? 'Volume' : 'Volume (range)'}{' '}
            <b className="vol-up">▲ {shares(vol?.bullish ?? 0)}</b> <b className="vol-down">▼ {shares(vol?.bearish ?? 0)}</b>
            {' '}<span className="muted">shares</span>
          </span>
          <span className="tt-time">{hover != null ? clock(shown.second) : 'Latest'}</span>
        </div>
      )}
      {withMid.length >= 2 && trades.length === 0 && <div className="volume-empty">No trades in this range</div>}
      {withMid.length < 2 && <div className="chart-empty chart-overlay">Not enough data in this range yet</div>}
    </div>
  )
}
