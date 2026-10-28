import { useState } from 'react'
import { Link, useParams } from 'react-router'
import { fetchBook, fetchHistory, fetchMarket, polymarketUrl } from '../api'
import BookLadder from '../components/BookLadder'
import Change from '../components/Change'
import DepthChart from '../components/DepthChart'
import PriceChart from '../components/PriceChart'
import SlippageCalc from '../components/SlippageCalc'
import { ago, cents, endDate, pct, pointsChange, usd } from '../format'
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

export default function MarketPage() {
  const { marketId = '' } = useParams()
  const [tab, setTab] = useState<(typeof TABS)[number]>('Overview')
  const [range, setRange] = useState(RANGES[0])
  const [outcomeIndex, setOutcomeIndex] = useState(1)
  const [depthRange, setDepthRange] = useState(0.1)

  const market = usePolling(() => fetchMarket(marketId), 5000, marketId)
  const history = usePolling(() => fetchHistory(marketId, range.minutes, range.bucket), 30000,
    `${marketId}/${range.label}`)
  const book = usePolling(() => fetchBook(marketId, outcomeIndex), 2000, `${marketId}/${outcomeIndex}`)

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

  return (
    <>
      <section className="market-head">
        <Link to="/" className="back">← All markets</Link>
        <h1>{m.question}</h1>
        <div className="sub">
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
            onClick={() => setTab(t)}>{t}</button>
        ))}
      </div>

      {tab !== 'Overview' && <div className="card notice placeholder">Coming soon.</div>}

      {tab === 'Overview' && (
        <>
          <section>
            <div className="section-head">
              <h2>{first.outcome} price <span className="muted">· mid with bid–ask band</span></h2>
              <Segmented options={RANGES.map((r) => r.label)} value={range.label} label="Time range"
                onChange={(l) => setRange(RANGES.find((r) => r.label === l)!)} />
            </div>
            <div className="card chart-card"><PriceChart points={history.data ?? []} /></div>
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
