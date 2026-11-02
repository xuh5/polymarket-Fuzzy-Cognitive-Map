import { useEffect, useMemo, useState } from 'react'
import { fetchReplay, type OutcomeStats, type Replay, type ReplayEvent, type ReplayPeriod } from '../api'
import { cents, clockMs, dateTime, shares } from '../format'
import { analyze, Cursor, LENGTHS, levels, periodOf, windowAtEnd } from '../replay'
import BookLadder from './BookLadder'
import DepthChart from './DepthChart'
import ReplayChart from './ReplayChart'

const SPEEDS = [1, 5, 20, 60] as const
const FEED_ROWS = 14
const DEPTH_RANGE = 0.1

// datetime-local 输入框用的本地时间「2026-10-29T14:03」
function localInput(t: number) {
  const d = new Date(t - new Date(t).getTimezoneOffset() * 60000)
  return d.toISOString().slice(0, 16)
}

// 流水里的一行：这一档怎么变了、成交、断线
function describe(e: ReplayEvent, prev: number) {
  if (e.type === 'change') {
    const side = e.side === 'bid' ? 'Bid' : 'Ask'
    const delta = e.size - prev
    const what = prev === 0 ? `new ${shares(e.size)}`
      : e.size === 0 ? `pulled ${shares(prev)}`
      : `${delta > 0 ? '+' : '−'}${shares(Math.abs(delta))} → ${shares(e.size)}`
    return { cls: e.side === 'bid' ? 'bids' : 'asks', label: `${side} ${cents(e.price)}`, text: what }
  }
  if (e.type === 'trade') {
    // 吃单方方向：Buy = 有人主动买入这个选项
    return { cls: e.side === 'BUY' ? 'buy' : 'sell', label: e.side === 'BUY' ? 'Trade · buy' : 'Trade · sell',
      text: `${shares(e.size)} @ ${cents(e.price)}` }
  }
  if (e.type === 'book') return { cls: 'info', label: 'Snapshot', text: 'full book from the exchange' }
  if (e.type === 'gap') return { cls: 'info', label: 'Reconnect', text: 'book reset until the next snapshot' }
  return { cls: 'info', label: 'Stopped', text: 'market unsubscribed or resolved' }
}

// Replay 标签：选一段时间，像看录像一样播放这段时间的价格、订单簿和每一次变化（毫秒级）
export default function ReplayPanel({ marketId, outcomes, periods, initialStart, initialLength, initialAt, onWindowChange }: {
  marketId: string
  outcomes: OutcomeStats[]
  periods: ReplayPeriod[]   // 录到数据的几段，开始时间只能在这里面选
  initialStart: number
  initialLength: number
  initialAt: number | null
  onWindowChange: (start: number, minutes: number) => void
}) {
  const [start, setStart] = useState(initialStart)
  const [minutes, setMinutes] = useState(initialLength)
  const [outcomeIndex, setOutcomeIndex] = useState(1)
  const [result, setResult] = useState<{ key: string; data: Replay | null; error: boolean } | null>(null)
  const [t, setT] = useState(() =>
    Math.min(Math.max(initialAt ?? initialStart, initialStart), initialStart + initialLength * 60000))
  const [playing, setPlaying] = useState(false)
  const [speed, setSpeed] = useState<(typeof SPEEDS)[number]>(5)
  const [cursor] = useState(() => new Cursor())

  const key = `${marketId}/${start}/${minutes}`
  const end = start + minutes * 60000
  const period = periodOf(periods, start, end)
  const recorded = period >= 0
  useEffect(() => {
    if (!recorded) return
    let alive = true
    fetchReplay(marketId, new Date(start).toISOString(), minutes)
      .then((data) => { if (alive) setResult({ key, data, error: false }) })
      .catch(() => { if (alive) setResult({ key, data: null, error: true }) })
    return () => { alive = false }
  }, [key, marketId, start, minutes, recorded])

  // 换了时间段：播放头回到开头（从 Overview 点进来的那一刻除外）
  const changeWindow = (s: number, m: number) => {
    setStart(s)
    setMinutes(m)
    setT(s)
    setPlaying(false)
    onWindowChange(s, m)
  }

  // 播放：每一帧按真实流逝的时间 × 倍速往前走，到头停下
  useEffect(() => {
    if (!playing) return
    let last = performance.now()
    let frame = requestAnimationFrame(function tick(now) {
      const dt = (now - last) * speed
      last = now
      setT((v) => {
        const next = Math.min(end, v + dt)
        if (next >= end) setPlaying(false)
        return next
      })
      frame = requestAnimationFrame(tick)
    })
    return () => cancelAnimationFrame(frame)
  }, [playing, speed, end])

  const data = result?.key === key ? result.data : null
  const failed = result?.key === key && result.error
  const info = useMemo(() => (data ? analyze(data) : null), [data])

  const controls = (
    <div className="controls">
      <label className="replay-start">
        <span className="muted">Start</span>
        <input type="datetime-local" value={localInput(start)}
          min={localInput(Date.parse(periods[0].start))} max={localInput(Date.parse(periods[periods.length - 1].end))}
          onChange={(e) => { const v = new Date(e.target.value).getTime(); if (!Number.isNaN(v)) changeWindow(v, minutes) }} />
      </label>
      <div className="segmented" role="tablist" aria-label="Window length">
        {LENGTHS.map((m) => (
          <button key={m} role="tab" aria-selected={m === minutes} className={m === minutes ? 'active' : ''}
            onClick={() => changeWindow(start, m)}>{m}m</button>
        ))}
      </div>
      {outcomes.length > 1 && (
        <div className="segmented" role="tablist" aria-label="Outcome">
          {outcomes.map((o) => (
            <button key={o.outcome_index} role="tab" aria-selected={o.outcome_index === outcomeIndex}
              className={o.outcome_index === outcomeIndex ? 'active' : ''}
              onClick={() => setOutcomeIndex(o.outcome_index)}>{o.outcome}</button>
          ))}
        </div>
      )}
    </div>
  )

  // 录到数据的几段：市场按成交量进出订阅名单，中间没订阅的日子没有数据。点一段跳到它的最后几分钟
  const chips = (
    <div className="periods">
      <span className="muted">Recorded</span>
      {periods.map((p, i) => (
        <button key={p.start} className={i === period ? 'active' : ''} aria-pressed={i === period}
          onClick={() => changeWindow(windowAtEnd(p, minutes), minutes)}>
          {dateTime(Date.parse(p.start))} – {dateTime(Date.parse(p.end))}
        </button>
      ))}
    </div>
  )

  const head = (
    <>
      <div className="section-head">
        <h2>Replay <span className="muted">· every order book change, to the millisecond</span></h2>
        {controls}
      </div>
      {chips}
    </>
  )

  if (!recorded) {
    return (
      <section>{head}
        <div className="card notice">This market wasn't being recorded at this time. Pick one of the recorded periods above.</div>
      </section>
    )
  }

  if (failed) {
    return <section>{head}<div className="card notice">Couldn't load this replay. Try another time window.</div></section>
  }
  if (!data || !info) {
    return <section>{head}<div className="card notice">Rebuilding the order book from the archive…</div></section>
  }

  const now = cursor.seek(data, t)
  const book = now.books.get(outcomeIndex)
  const shown = book ? levels(book) : null
  // 深度图默认看中间价 ±10¢；冷门市场价差常常比这还宽（买一 28¢、卖一 98¢），那就放宽到两边的最优价都看得到
  const spread = shown?.bids[0] && shown.asks[0] ? shown.asks[0][0] - shown.bids[0][0] : 0
  const depthRange = Math.min(0.5, Math.max(DEPTH_RANGE, spread / 2 + 0.05))
  const trades = data.events.flatMap((e) => (e.type === 'trade' && e.o === outcomeIndex ? [e] : []))
  const flash = (side: 'bid' | 'ask', price: number) => {
    const i = now.changedBy.get(`${outcomeIndex}:${side}:${price}`)
    if (i == null) return null
    const e = data.events[i] as Extract<ReplayEvent, { type: 'change' }>
    return { key: i, dir: e.size > info.prev[i] ? 'up' as const : 'down' as const }
  }

  // 流水：播放头之前最近的若干条（这个选项的变化和成交，加上断线这类整个市场的事）
  const feed: { e: ReplayEvent; i: number }[] = []
  for (let i = now.idx - 1; i >= 0 && feed.length < FEED_ROWS; i--) {
    const e = data.events[i]
    if (!('o' in e) || e.o === outcomeIndex) feed.push({ e, i })
  }

  return (
    <section>
      {head}
      {!data.complete && (
        <div className="card notice">
          Part of this window hasn't been archived yet (the archive runs about 5 minutes behind), so the end is missing.
        </div>
      )}
      <div className="card chart-card">
        <ReplayChart start={start} end={end} points={info.series.get(outcomeIndex) ?? []} trades={trades} t={t}
          onSeek={(v) => { setT(v); setPlaying(false) }} />
        <div className="player">
          <button className="play" onClick={() => { if (t >= end) setT(start); setPlaying(!playing) }}
            aria-label={playing ? 'Pause' : 'Play'}>{playing ? '❚❚' : '▶'}</button>
          <div className="segmented" role="tablist" aria-label="Playback speed">
            {SPEEDS.map((s) => (
              <button key={s} role="tab" aria-selected={s === speed} className={s === speed ? 'active' : ''}
                onClick={() => setSpeed(s)}>{s}×</button>
            ))}
          </div>
          <input type="range" className="scrub" min={start} max={end} step={100} value={t} aria-label="Playback position"
            onChange={(e) => { setT(Number(e.target.value)); setPlaying(false) }} />
          <span className="player-time">{clockMs(t)}</span>
        </div>
      </div>

      <div className="book-grid replay-grid">
        <div className="card chart-card">
          {shown ? (
            <>
              <DepthChart bids={shown.bids} asks={shown.asks} range={depthRange} />
              <BookLadder bids={shown.bids} asks={shown.asks} flash={flash} />
            </>
          ) : (
            <div className="chart-empty replay-unavailable">
              No order book at this moment — the market wasn't subscribed yet, or the feed was reconnecting.
            </div>
          )}
        </div>
        <div className="card feed">
          <h3>Activity</h3>
          {feed.length === 0 && <p className="muted">Nothing has happened yet in this window.</p>}
          <ol>
            {feed.map(({ e, i }) => {
              const d = describe(e, info.prev[i])
              return (
                <li key={i}>
                  <span className="feed-time">{clockMs(e.t)}</span>
                  <span className={`feed-label ${d.cls}`}>{d.label}</span>
                  <span className="feed-text">{d.text}</span>
                </li>
              )
            })}
          </ol>
        </div>
      </div>
    </section>
  )
}
