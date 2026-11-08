import { useEffect, useRef } from 'react'
import { fetchBook, fetchDepthQuality, type Book, type FlaggedLevel } from '../api'
import { ago, cents, clock, dayClock, usd } from '../format'
import { usePolling } from '../usePolling'
import DepthChart from './DepthChart'
import TimeSeriesChart, { type Series } from './TimeSeriesChart'

const SERIES: Series[] = [{ key: 'flagged_pct', label: 'Flagged share', color: 'var(--s1)' }]
const BUCKET_MS = 5 * 60000
const NEAR = 0.05      // 和后端的 NEAR_PRICE 一致：离本侧最优价 5¢ 内
const percent = (v: number) => `${v.toFixed(1)}%`

interface Row { side: 'BUY' | 'SELL'; price: number; usd: number; flag?: FlaggedLevel }

// 离最优价 5¢ 内的全部价位。盘口是实时的，可疑标记每分钟更新：
// 被标记的价位此刻可能正好撤掉了（闪现的价位经常这样），也列出来，金额为 0
function ladderSide(side: Row['side'], levels: Book['bids'], flagged: FlaggedLevel[]): Row[] {
  const flags = flagged.filter((l) => l.side === side)
  const best = levels[0]?.[0]
  const rows = new Map<number, Row>()
  if (best != null) {
    for (const [price, size] of levels) {
      if (Math.abs(price - best) <= NEAR + 1e-9) {
        rows.set(price, { side, price, usd: price * size, flag: flags.find((l) => l.price === price) })
      }
    }
  }
  for (const flag of flags) {
    if (!rows.has(flag.price)) rows.set(flag.price, { side, price: flag.price, usd: 0, flag })
  }
  return [...rows.values()].sort((a, b) => b.price - a.price)
}

function QualityLadder({ book, flagged }: { book: Book; flagged: FlaggedLevel[] }) {
  const asks = ladderSide('SELL', book.asks, flagged)
  const bids = ladderSide('BUY', book.bids, flagged)
  const max = Math.max(...[...asks, ...bids].map((r) => r.usd), 1)
  const bestBid = book.bids[0]?.[0]
  const bestAsk = book.asks[0]?.[0]

  // 价位多的时候表格要滚动：一打开先停在买卖价中间，最值得看的价位都在这附近
  const wrap = useRef<HTMLDivElement>(null)
  const spread = useRef<HTMLTableRowElement>(null)
  useEffect(() => {
    if (!wrap.current || !spread.current) return
    const offset = spread.current.getBoundingClientRect().top - wrap.current.getBoundingClientRect().top
    wrap.current.scrollTop += offset - wrap.current.clientHeight / 2
  }, [])

  const row = (r: Row) => (
    <tr key={`${r.side}/${r.price}`} className={`${r.side === 'BUY' ? 'bids' : 'asks'}${r.flag ? ' flagged' : ''}`}>
      <td className="num">{cents(r.price)}</td>
      <td className="val">
        <span className="bar" style={{ width: `${(r.usd / max) * 100}%` }} aria-hidden="true">
          {r.flag && r.usd > 0 && <span className="bar-flagged" style={{ width: `${Math.min(r.flag.flagged_usd / r.usd, 1) * 100}%` }} />}
        </span>
        <span className="amount">{r.usd > 0 ? usd(r.usd) : '—'}</span>
      </td>
      <td className="num">{r.flag ? usd(Math.min(r.flag.flagged_usd, r.usd)) : ''}</td>
      <td className="note">
        {r.flag && `Large orders appeared and vanished ${r.flag.flashes} times, with ${usd(r.flag.filled_usd)} filled`}
        {r.flag && r.flag.reactions > 0 && `. Over half was pulled within 1 second of a trade ${r.flag.reactions} ${r.flag.reactions === 1 ? 'time' : 'times'}`}
        {r.flag && r.usd === 0 && ' · none resting now'}
      </td>
    </tr>
  )

  return (
    <div className="card table-wrap quality-ladder" ref={wrap}>
      <table>
        <thead>
          <tr><th className="num">Price</th><th className="num">Resting now</th><th className="num">Flagged</th><th>In the past hour</th></tr>
        </thead>
        <tbody>
          {asks.map(row)}
          <tr className="spread-row" ref={spread}>
            <td colSpan={4}>{bestBid != null && bestAsk != null ? `Spread ${cents(bestAsk - bestBid)}` : 'One side of the book is empty'}</td>
          </tr>
          {bids.map(row)}
        </tbody>
      </table>
    </div>
  )
}

// 市场详情页的 Depth quality 标签：规则在后端（pmdata/depth/tracker.py），这里只显示结果，措辞只讲发生了什么。
// 行情的可疑标记都是第一个选项的盘口，所以这里固定显示第一个选项
export default function DepthQualityPanel({ marketId, outcome }: { marketId: string; outcome: string }) {
  const q = usePolling(() => fetchDepthQuality(marketId, 24), 30000, marketId)   // 后端每分钟更新一次
  const book = usePolling(() => fetchBook(marketId, 1), 2000, marketId)

  if (!q.data) {
    return <div className="card notice">{q.failed ? "Couldn't load depth quality." : 'Loading…'}</div>
  }
  const { latest, history } = q.data
  if (!latest) {
    return (
      <div className="card notice">
        No recent depth quality data. This market may not be tracked right now, or was added within the last hour.
      </div>
    )
  }

  const share = latest.depth_usd > 0 ? latest.suspicious_usd / latest.depth_usd : null
  const rows = history.map((h) => ({ t: h.t, flagged_pct: h.flagged_pct }))
  const b = book.data
  const flagged = latest.levels
  const flaggedAt = (side: 'bid' | 'ask', price: number) =>
    flagged.find((l) => l.price === price && l.side === (side === 'bid' ? 'BUY' : 'SELL'))?.flagged_usd ?? 0
  // 深度图以中间价为中心，要让离最优价 5¢ 的价位都落在图里
  const range = b?.bids[0] && b.asks[0] ? NEAR + (b.asks[0][0] - b.bids[0][0]) / 2 : NEAR

  return (
    <>
      <section className="tiles">
        <div className="card tile">
          <div className="tile-label">Depth confidence</div>
          <div className="tile-value">{share == null ? '—' : `${Math.round(100 * (1 - share))} / 100`}</div>
          <div className="tile-note">100 minus the flagged share</div>
        </div>
        <div className="card tile">
          <div className="tile-label">Depth near the best price</div>
          <div className="tile-value sm">{usd(latest.depth_usd)}</div>
          <div className="tile-note">Both sides, within 5¢ of the best bid or ask</div>
        </div>
        <div className="card tile">
          <div className="tile-label">Flagged amount</div>
          <div className="tile-value sm">{usd(latest.suspicious_usd)}</div>
          <div className="tile-note">{share == null ? '—' : percent(share * 100)} of that depth</div>
        </div>
        <div className="card tile">
          <div className="tile-label">Flagged price levels</div>
          <div className="tile-value sm">{flagged.length}</div>
          <div className="tile-note">Updated {ago(latest.minute)}</div>
        </div>
      </section>

      <section>
        <div className="section-head">
          <h2>Order book <span className="muted">· {outcome}, within 5¢ of the best price</span></h2>
          <div className="legend">
            <span><span className="key" style={{ background: 'var(--warn)' }} />Flagged</span>
          </div>
        </div>
        {b && !b.available && <div className="card notice">The live order book is temporarily unavailable.</div>}
        {b?.available && (
          <>
            <QualityLadder book={b} flagged={flagged} />
            <p className="muted depth-caption">
              {flagged.length === 0
                ? 'No price levels are flagged in this market right now.'
                : 'Live book. Flags update every minute.'}
            </p>
            <div className="card chart-card">
              <DepthChart bids={b.bids} asks={b.asks} range={range} flagged={flaggedAt} />
            </div>
          </>
        )}
      </section>

      <section>
        <div className="section-head">
          <h2>Flagged share of depth <span className="muted">· past 24 hours</span></h2>
        </div>
        <div className="card chart-card">
          <TimeSeriesChart rows={rows} series={SERIES} format={percent} timeLabel={clock} tipLabel={dayClock}
            bucketMs={BUCKET_MS} label="Flagged share of depth over the past 24 hours" />
        </div>
      </section>

      <section>
        <div className="card notice depth-method">
          <h3>How this is calculated</h3>
          <p>
            A price level is flagged when, at least 5 times in the past hour, an order of $1,000 or more within 5¢ of
            the best price was removed within 10 minutes with under 5% of it filled. Only money placed at that price in
            the last 10 minutes counts as flagged.
          </p>
          <p>
            The market feed only shows the total size at each price, so individual orders are inferred from how that
            total changes. This describes order behavior only: it can't show who placed the orders or why, and routine
            market making can look the same.
          </p>
        </div>
      </section>
    </>
  )
}
