import { Link } from 'react-router'

// About 页：给外部访客讲清楚网站能看什么、数据从哪来、能说明什么不能说明什么。全是静态内容，不调接口。

// 只有自己有页面的才做成链接；深度图、回放、深度质量在市场详情页里
const FEATURES: { name: string; text: string; to?: string }[] = [
  {
    to: '/',
    name: 'Markets',
    text: 'The most active tech and AI markets, with live prices and how far each has moved in the last hour.',
  },
  {
    name: 'Depth chart and slippage',
    text: 'On each market page: the full order book rebuilt from the feed, and what a market order of any size would actually pay.',
  },
  {
    name: 'Replay',
    text: 'Pick any recorded window of up to 60 minutes and play back every order book change and trade, to the millisecond.',
  },
  {
    name: 'Depth quality',
    text: 'Price levels where large orders keep appearing near the best price and get pulled before they trade.',
  },
  {
    to: '/signals',
    name: 'Signals',
    text: 'Sudden price moves, with the trades behind them and where the price was 5 and 30 minutes later.',
  },
  {
    to: '/status',
    name: 'Status',
    text: 'Live health of the pipeline: latency, throughput, uptime and incidents.',
  },
]

const STEPS = [
  { name: 'Polymarket', sub: 'WebSocket + REST' },
  { name: 'Collector', sub: 'raw events, unchanged' },
  { name: 'Kafka', sub: 'event log' },
  { name: 'Stream services', sub: 'books, metrics, signals' },
  { name: 'Databases', sub: 'Postgres + Redis' },
  { name: 'This site', sub: 'API + React' },
]

const CHECKS = [
  { label: 'Order books vs. exchange', value: '99.66%', note: '582 of 584 rebuilt books identical at every level to official snapshots' },
  { label: 'Trades vs. official API', value: '253 / 253', note: 'recorded trades matched by transaction hash' },
  { label: 'Depth flags, live vs. replay', value: '42,620', note: 'market-minutes recomputed from the archive, all identical' },
  { label: 'End-to-end latency', value: '2.1 s', note: 'median from exchange to database · p99 5.6 s' },
]

export default function About() {
  return (
    <>
      <section className="hero">
        <h1>About</h1>
        <p>
          Depthscope watches the order books of Polymarket's tech and AI markets in real time. It shows how deep a
          market really is, flags depth that keeps vanishing, and lets you replay big price moves tick by tick.
        </p>
      </section>

      <section>
        <div className="section-head"><h2>What you can see</h2></div>
        <div className="about-grid">
          {FEATURES.map((f) => {
            const body = <><h3>{f.name}{f.to && <span aria-hidden="true"> →</span>}</h3><p>{f.text}</p></>
            return f.to
              ? <Link key={f.name} to={f.to} className="card about-feature link">{body}</Link>
              : <div key={f.name} className="card about-feature">{body}</div>
          })}
        </div>
      </section>

      <section>
        <div className="section-head"><h2>Which markets</h2></div>
        <div className="card about-text">
          <p>
            Every hour, Depthscope reads all open markets tagged Tech or AI on Polymarket and follows the 30 with the
            highest 24-hour volume that are still taking orders and have at least two days left. Markets join and
            leave the list as volume shifts, so a market's history only covers the periods it was on the list. The
            Replay tab shows exactly which periods were recorded.
          </p>
        </div>
      </section>

      <section>
        <div className="section-head"><h2>How it works</h2></div>
        <div className="card pipeline about-pipe">
          <div className="pipe-row">
            {STEPS.map((s, i) => (
              <div key={s.name} className="pipe-step">
                {i > 0 && <div className="pipe-hop"><span /></div>}
                <div className="pipe-node">
                  <div className="pipe-name">{s.name}</div>
                  <div className="pipe-sub">{s.sub}</div>
                </div>
              </div>
            ))}
          </div>
          <div className="pipe-archive">
            Kafka also feeds a <b>Parquet archive</b> of every raw event. Replays are rebuilt from it, and a daily
            batch job turns it into tables for analysis.
          </div>
        </div>
        <div className="card about-text">
          <p>
            A collector keeps a WebSocket open to Polymarket and stores every order book snapshot, price-level change
            and trade exactly as received. If the connection drops, it reconnects within seconds and marks the gap,
            so nothing downstream mistakes missing data for a quiet market.
          </p>
          <p>
            Separate services read that stream and each keep their own copy of every order book in memory: one
            computes prices, spreads and depth once a second, one detects price jumps, and one tracks how long large
            orders stay on the book. All of them run on exchange timestamps, so replaying the archive gives exactly
            the same results as the live run.
          </p>
          <p>
            Numbers on this site lag the exchange by about two seconds. That is on purpose: Polymarket sometimes
            splits one update across several messages, so each second is published only after it has fully closed.
          </p>
        </div>
      </section>

      <section>
        <div className="section-head"><h2>How it's checked</h2></div>
        <div className="tiles">
          {CHECKS.map((c) => (
            <div key={c.label} className="card tile">
              <div className="tile-label">{c.label}</div>
              <div className="tile-value sm">{c.value}</div>
              <div className="tile-note">{c.note}</div>
            </div>
          ))}
        </div>
      </section>

      <section>
        <div className="section-head"><h2>What the data can't tell you</h2></div>
        <div className="card about-text">
          <ul>
            <li>
              <b>Who placed an order.</b> The public feed only gives the total size at each price, with no order IDs
              or accounts. A flagged level describes how the money behaved, not who placed it or why. It may be
              spoofing, or a market maker that pulls quotes quickly.
            </li>
            <li>
              <b>How many orders sit at a price.</b> One $5,000 level could be one order or fifty. An order cancelled
              and replaced in the same instant doesn't show up at all.
            </li>
            <li>
              <b>Anything before we started recording.</b> Order book history begins in October 2026, and only for
              markets while they were followed. Polymarket offers no historical order book data.
            </li>
            <li>
              <b>Gaps.</b> When the connection drops, changes during the gap are lost. Each reconnect is listed on the{' '}
              <Link to="/status">Status</Link> page.
            </li>
          </ul>
        </div>
      </section>

      <section>
        <div className="section-head"><h2>Fine print</h2></div>
        <div className="card about-text">
          <p>
            Depthscope is an independent project and is not affiliated with Polymarket. It doesn't trade, take
            payment, or have user accounts, and it doesn't redistribute raw exchange data. Nothing here is investment
            advice. The source code is on{' '}
            <a href="https://github.com/xuh5/polymarket-Fuzzy-Cognitive-Map" target="_blank" rel="noreferrer">GitHub</a>.
          </p>
        </div>
      </section>
    </>
  )
}
