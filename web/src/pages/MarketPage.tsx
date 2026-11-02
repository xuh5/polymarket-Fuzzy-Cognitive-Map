import { useState } from 'react'
import { Link, useParams, useSearchParams } from 'react-router'
import { fetchBook, fetchHistory, fetchMarket, fetchReplayPeriods, fetchTrades, polymarketUrl } from '../api'
import BookLadder from '../components/BookLadder'
import Change from '../components/Change'
import DepthChart from '../components/DepthChart'
import Labels from '../components/Labels'
import PriceChart from '../components/PriceChart'
import ReplayPanel from '../components/ReplayPanel'
import SlippageCalc from '../components/SlippageCalc'
import { ago, cents, endDate, pct, pointsChange, usd } from '../format'
import { LENGTHS, minuteFloor, windowAtEnd } from '../replay'
import { usePolling } from '../usePolling'

// 走势图的时间范围：多长、每个点聚合多少秒（点数控制在几百个以内）
const RANGES = [
  { label: '1H', minutes: 60, bucket: 15 },
  { label: '6H', minutes: 360, bucket: 60 },
  { label: '24H', minutes: 1440, bucket: 300 },
]
const DEPTH_RANGES = [0.05, 0.1, 0.25]
const TABS = ['Overview', 'Replay', 'Depth quality'] as const

function Segmented<T extends string | number>({ options, value, onChange, label, render }: {
  options: readonly T[]; value: T; onChange: (v: T) => void; label: string; render?: (v: T) => string
}) {
  return (
    <div className="segmented" role="tablist" aria-label={label}>
      {options.map((o) => (
        <button key={String(o)} role="tab" aria-selected={o === value} className={o === value ? 'active' : ''}
          onClick={() => onChange(o)}>
          {render ? render(o) : String(o)}
        </button>
      ))}
    </div>
  )
}

const REPLAY_LEAD = 10 * 60000   // 从 Overview 价格图点进来：从点的那一刻往前 10 分钟开始

export default function MarketPage() {
  const { marketId = '' } = useParams()
  // 网址带上回放的时间（?replay=开始时间&len=分钟），回放某一刻可以直接发链接给别人
  const [params, setParams] = useSearchParams()
  const [tab, setTab] = useState<(typeof TABS)[number]>(() => (params.has('replay') ? 'Replay' : 'Overview'))
  const [replayAt, setReplayAt] = useState<number | null>(null)
  const [range, setRange] = useState(RANGES[0])
  const [outcomeIndex, setOutcomeIndex] = useState(1)
  const [depthRange, setDepthRange] = useState(0.1)

  const market = usePolling(() => fetchMarket(marketId), 5000, marketId)
  const history = usePolling(() => fetchHistory(marketId, range.minutes, range.bucket), 30000,
    `${marketId}/${range.label}`)
  // 成交最细按分钟存，所以格子至少 1 分钟
  const tradeBucket = Math.max(60, range.bucket)
  const trades = usePolling(() => fetchTrades(marketId, range.minutes, tradeBucket), 30000,
    `${marketId}/${range.label}`)
  const book = usePolling(() => fetchBook(marketId, outcomeIndex), 2000, `${marketId}/${outcomeIndex}`)
  // 正在订阅的市场最后一段一直在变长，隔一分钟刷新一次
  const periods = usePolling(() => fetchReplayPeriods(marketId), 60000, marketId)

  const m = market.data
  if (market.failed && !m) {
    return (
      <section className="hero">
        <Link to="/" className="back">← All markets</Link>
        <h1>Market not found</h1>
        <p>This market isn't tracked, or the data service is offline.</p>
      </section>
    )
  }
  if (!m) return <div className="loading">Loading…</div>

  const first = m.outcomes[0]
  const current = m.outcomes.find((o) => o.outcome_index === outcomeIndex) ?? first
  const pts = pointsChange(first.mid, first.logodds_chg_60m)
  const b = book.data

  const p = periods.data
  const lenParam = Number(params.get('len'))
  const replayLength = (LENGTHS as readonly number[]).includes(lenParam) ? lenParam : 20
  // 网址没带时间：默认看最近一段录制的最后 20 分钟
  const replayStart = Date.parse(params.get('replay') ?? '') || (p?.length ? windowAtEnd(p[p.length - 1], replayLength) : 0)
  const setReplayWindow = (start: number, minutes: number) =>
    setParams({ replay: new Date(start).toISOString(), len: String(minutes) }, { replace: true })
  const switchTab = (t: (typeof TABS)[number]) => {
    setTab(t)
    if (t !== 'Replay') setParams({}, { replace: true })
  }
  // Overview 价格图上点了某一刻（秒）：切到回放，播放头停在那一刻
  const replayFrom = (sec: number) => {
    setReplayAt(sec * 1000)
    setReplayWindow(minuteFloor(sec * 1000 - REPLAY_LEAD), 20)
    setTab('Replay')
  }

  return (
    <>
      <section className="market-head">
        <Link to="/" className="back">← All markets</Link>
        <h1>{m.question}</h1>
        <div className="sub">
          <Labels topic={m.topic} company={m.company} />
          {endDate(m.end_date) && <span>Ends {endDate(m.end_date)}</span>}
          {first.updated_at && <span>Updated {ago(first.updated_at)}</span>}
          {m.slug && (
            <a className="ext-link" href={polymarketUrl(m.slug)} target="_blank" rel="noreferrer">
              View on Polymarket ↗
            </a>
          )}
        </div>
      </section>

      <section className="tiles">
        <div className="card tile">
          <div className="tile-label">{first.outcome} probability</div>
          <div className="tile-value">{pct(first.mid)}</div>
          <Change points={pts} />
        </div>
        <div className="card tile">
          <div className="tile-label">Bid / Ask</div>
          <div className="tile-value sm">{cents(first.best_bid)} / {cents(first.best_ask)}</div>
          <div className="tile-note">Spread {cents(first.spread)}</div>
        </div>
        <div className="card tile">
          <div className="tile-label">Depth within 5¢</div>
          <div className="tile-value sm">{usd(first.bid_depth_5c)} / {usd(first.ask_depth_5c)}</div>
          <div className="tile-note">Bids / Asks</div>
        </div>
        <div className="card tile">
          <div className="tile-label">Book imbalance</div>
          <div className="tile-value sm">
            {first.imbalance_5 == null ? '—' : `${first.imbalance_5 > 0 ? '+' : ''}${(first.imbalance_5 * 100).toFixed(0)}%`}
          </div>
          <div className="tile-note">Top 5 levels, + = more bids</div>
        </div>
      </section>

      <div className="tabs" role="tablist" aria-label="Market views">
        {TABS.map((t) => (
          <button key={t} role="tab" aria-selected={tab === t} className={tab === t ? 'active' : ''}
            onClick={() => switchTab(t)}>{t}</button>
        ))}
      </div>

      {tab === 'Replay' && !p && (
        <div className="card notice">{periods.failed ? "Couldn't load the recorded periods." : 'Loading…'}</div>
      )}
      {tab === 'Replay' && p?.length === 0 && (
        <div className="card notice">Nothing recorded for this market yet, so there's nothing to replay.</div>
      )}
      {tab === 'Replay' && !!p?.length && (
        <ReplayPanel key={`${marketId}/${replayAt}`} marketId={marketId} outcomes={m.outcomes} periods={p}
          initialStart={replayStart} initialLength={replayLength} initialAt={replayAt ?? (Number(params.get('at')) || null)}
          onWindowChange={setReplayWindow} />
      )}
      {tab === 'Depth quality' && <div className="card notice placeholder">Coming soon.</div>}

      {tab === 'Overview' && (
        <>
          <section>
            <div className="section-head">
              <h2>{first.outcome} price <span className="muted">· mid with best bid / ask, volume · click to replay</span></h2>
              <Segmented options={RANGES.map((r) => r.label)} value={range.label} label="Time range"
                onChange={(l) => setRange(RANGES.find((r) => r.label === l)!)} />
            </div>
            <div className="card chart-card"><PriceChart key={`${marketId}/${range.label}`} points={history.data ?? []} bucketSeconds={range.bucket}
              trades={trades.data ?? []} tradeBucketSeconds={tradeBucket} onPick={replayFrom} /></div>
          </section>

          <section>
            <div className="section-head">
              <h2>Order book</h2>
              <div className="controls">
                {m.outcomes.length > 1 && (
                  <Segmented options={m.outcomes.map((o) => o.outcome_index)} value={outcomeIndex} label="Outcome"
                    onChange={setOutcomeIndex}
                    render={(i) => m.outcomes.find((o) => o.outcome_index === i)!.outcome} />
                )}
                <Segmented options={DEPTH_RANGES} value={depthRange} label="Depth range" onChange={setDepthRange}
                  render={(r) => `±${Math.round(r * 100)}¢`} />
              </div>
            </div>

            {b && !b.available && (
              <div className="card notice">
                The live order book for {current.outcome} is temporarily unavailable (reconnecting to the exchange).
              </div>
            )}
            {b?.available && (
              <div className="book-grid">
                <div className="card chart-card">
                  <DepthChart bids={b.bids} asks={b.asks} range={depthRange} />
                  <BookLadder bids={b.bids} asks={b.asks} />
                  {b.exchange_ts && (
                    <div className="book-note muted">Live book · last change {ago(new Date(b.exchange_ts).toISOString())}</div>
                  )}
                </div>
                <SlippageCalc bids={b.bids} asks={b.asks} outcome={current.outcome} />
              </div>
            )}
          </section>
        </>
      )}
    </>
  )
}
