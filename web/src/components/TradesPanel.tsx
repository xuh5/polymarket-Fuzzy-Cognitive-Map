import { useState } from 'react'
import { fetchExecutions, type Execution } from '../api'
import { cents, shares, usd } from '../format'
import { usePolling } from '../usePolling'
import Flash from './Flash'
import MarkoutChart, { type MarkoutSeries } from './MarkoutChart'
import PaidBreakdown from './PaidBreakdown'
import Pager from './Pager'

// 7 天的数据多（最多几千笔），刷新慢一点；新成交本来就要 5 分钟后才出现
const RANGES = [{ label: '1H', hours: 1, every: 30000 }, { label: '24H', hours: 24, every: 30000 },
  { label: '7D', hours: 168, every: 300000 }]
const PAGE_SIZES = [20, 50, 100]
// 曲线的几个点：成交那一刻（= 吃单方多付的，挂单方按成交前中间价算赚到的），之后 1 秒 / 10 秒 / 1 分钟 / 5 分钟
const STEPS = ['At the trade', '1s', '10s', '1m', '5m']
const PNL = ['maker_pnl_1s', 'maker_pnl_10s', 'maker_pnl_60s', 'maker_pnl_300s'] as const

// 每股多少美分，带正负号：+0.24¢
const signedCents = (v: number | null) => (v == null ? '—' : `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v * 100).toFixed(2)}¢`)
const axisCents = (v: number) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v * 100).toFixed(1)}¢`
const time = (iso: string) => new Date(iso).toLocaleString('en-US',
  { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit' })

// 按股数加权的平均（大单的权重大），只算有值的
function weighted(rows: Execution[], get: (r: Execution) => number | null) {
  let sum = 0
  let n = 0
  let count = 0
  for (const r of rows) {
    const v = get(r)
    if (v == null) continue
    sum += v * r.size
    n += r.size
    count++
  }
  return { value: n > 0 ? sum / n : null, count }
}

const rowKey = (r: Execution) => `${r.tx_hash}/${r.outcome_index}`

function curve(key: string, label: string, color: string, rows: Execution[]): MarkoutSeries {
  const pts = [weighted(rows, (r) => r.cost), ...PNL.map((k) => weighted(rows, (r) => r[k]))]
  return { key, label, color, values: pts.map((p) => p.value), counts: pts.map((p) => p.count) }
}

// 市场详情页的 Trades 标签：每笔吃单成交前的盘口、吃单方多付多少、挂单方事后赚亏（markout）。
// 规则在后端（pmdata/execution/tracker.py），这里只做汇总和显示
export default function TradesPanel({ marketId, outcome, other, onReplay }: {
  marketId: string
  outcome: string              // 第一个选项的名字（二元市场是 Yes）
  other: string                // 第二个选项
  onReplay: (ms: number) => void
}) {
  const [range, setRange] = useState(RANGES[1])
  const [pageSize, setPageSize] = useState(20)
  const [page, setPage] = useState(1)
  // 新出现的成交：和上一次拉到的同一时间范围的数据比，多出来的行闪一下（第一次加载、切换范围时不闪）
  const [prev, setPrev] = useState<{ data: Execution[] | null; range: string }>({ data: null, range: '' })
  const [fresh, setFresh] = useState<Set<string>>(new Set())
  const q = usePolling(() => fetchExecutions(marketId, range.hours), range.every, `${marketId}/${range.label}`)

  const head = (
    <div className="section-head">
      <h2>Trades <span className="muted">· past {range.label === '7D' ? '7 days' : range.label === '1H' ? 'hour' : '24 hours'}</span></h2>
      <div className="segmented" role="tablist" aria-label="Time range">
        {RANGES.map((r) => (
          <button key={r.label} role="tab" aria-selected={r === range} className={r === range ? 'active' : ''}
            onClick={() => { setRange(r); setPage(1) }}>{r.label}</button>
        ))}
      </div>
    </div>
  )
  if (q.data && q.data !== prev.data) {
    setPrev({ data: q.data, range: range.label })
    if (prev.data && prev.range === range.label) {
      const old = new Set(prev.data.map(rowKey))
      setFresh(new Set(q.data.map(rowKey).filter((k) => !old.has(k))))
    } else {
      setFresh(new Set())
    }
  }
  if (!q.data) {
    return <>{head}<div className="card notice">{q.failed ? "Couldn't load trades." : 'Loading…'}</div></>
  }
  const rows = q.data
  if (!rows.length) {
    return (
      <>
        {head}
        <div className="card notice">
          No trades in this range. Trades show up here 5 minutes after they happen, once the 5-minute
          follow-up price is known.
        </div>
      </>
    )
  }

  const volume = rows.reduce((a, r) => a + r.notional, 0)
  // 往第一个选项方向的成交额：买第一个选项，或卖第二个选项（和 Overview 成交量图的看涨/看跌一致）
  const toFirst = rows.filter((r) => (r.outcome_index === 1) === (r.side === 'BUY')).reduce((a, r) => a + r.notional, 0)
  const spread = weighted(rows, (r) => r.pre_spread).value
  // 多付的、价格变动、挂单方剩下的：只用两个数都有的成交，三个数才正好 多付 = 变动 + 剩下。
  // 价格变动（价格冲击）= 5 分钟后中间价往吃单方向走了多少 = 多付的 − 挂单方 5 分钟后还剩的
  const complete = rows.filter((r) => r.cost != null && r.maker_pnl_300s != null)
  const both = weighted(complete, (r) => r.cost)
  const paidSame = both.value
  const movedSame = weighted(complete, (r) => r.cost! - r.maker_pnl_300s!).value
  // 主动买的均价比成交前中间价高多少、主动卖的低多少（每股，按股数加权）
  const buyAbove = weighted(rows.filter((r) => r.side === 'BUY'), (r) => r.cost)
  const sellBelow = weighted(rows.filter((r) => r.side === 'SELL'), (r) => r.cost)
  const known = rows.filter((r) => r.levels != null)
  const multi = known.filter((r) => r.levels! > 1)
  const multiVolume = multi.reduce((a, r) => a + r.notional, 0)
  const knownVolume = known.reduce((a, r) => a + r.notional, 0)
  // 每 30 秒刷新一次，条数会变：页码不超出范围
  const current = Math.min(page, Math.max(1, Math.ceil(rows.length / pageSize)))
  const series = [
    curve('single', 'Orders using 1 price', 'var(--s1)', known.filter((r) => r.levels === 1)),
    curve('multi', 'Big orders', 'var(--s2)', multi),
  ]

  return (
    <>
      {head}
      <section className="card stat-strip">
        <div className="stat">
          <div className="tile-label">Trades</div>
          <div className="stat-value"><Flash value={rows.length}>{rows.length.toLocaleString('en-US')}</Flash></div>
          <div className="tile-note">{usd(volume)} in total</div>
        </div>
        <div className="stat">
          <div className="tile-label">Money betting on</div>
          {volume > 0 ? (
            <>
              <div className="split-labels">
                <span className="vol-up">{outcome} {Math.round((toFirst / volume) * 100)}%</span>
                <span className="vol-down">{other} {100 - Math.round((toFirst / volume) * 100)}%</span>
              </div>
              <div className="split-bar" aria-hidden="true">
                <span className="up" style={{ width: `${(toFirst / volume) * 100}%` }} />
                <span className="down" />
              </div>
            </>
          ) : <div className="stat-value">—</div>}
          <div className="tile-note">Buying {other} counts as betting against {outcome}</div>
        </div>
        <div className="stat">
          <div className="tile-label">Bid–ask gap</div>
          <div className="stat-value">{spread == null ? '—' : `${(spread * 100).toFixed(2)}¢`}</div>
          <div className="tile-note">Average, just before each trade</div>
        </div>
        <div className="stat">
          <div className="tile-label">Price vs middle</div>
          <div className="vs-mid">
            <div><span className="vol-up">Buys</span> <b>{buyAbove.value == null ? '—' : `${(buyAbove.value * 100).toFixed(2)}¢`}</b> above</div>
            <div><span className="vol-down">Sells</span> <b>{sellBelow.value == null ? '—' : `${(sellBelow.value * 100).toFixed(2)}¢`}</b> below</div>
          </div>
          <div className="tile-note">Average trade price vs the middle price just before</div>
        </div>
        <div className="stat">
          <div className="tile-label">Big orders</div>
          {known.length > 0 && knownVolume > 0 ? (
            <div className="share-bars">
              <div><span className="share-track"><span style={{ width: `${(multi.length / known.length) * 100}%` }} /></span>
                <b>{Math.round((multi.length / known.length) * 100)}%</b> of trades</div>
              <div><span className="share-track"><span style={{ width: `${(multiVolume / knownVolume) * 100}%` }} /></span>
                <b>{Math.round((multiVolume / knownVolume) * 100)}%</b> of the money</div>
            </div>
          ) : <div className="stat-value">—</div>}
          <div className="tile-note">Orders that used up more than one price</div>
        </div>
      </section>


      <section>
        <div className="section-head">
          <h2>Over the 5 minutes after <span className="muted">· what the waiting orders had, cents per share</span></h2>
        </div>
        <div className="card chart-card">
          <MarkoutChart steps={STEPS} series={series} format={axisCents}
            label="What the waiting order's side was worth per share, at the trade and 1 second, 10 seconds, 1 minute and 5 minutes later" />
          <p className="fine-print">
            Every trade has two sides: someone who traded right away, and an order that was already waiting on the book.
            This chart follows the waiting side. It hasn't sold yet, so each point values its shares at the middle price
            at that moment. It starts at the extra the other side paid. Below zero means the price kept moving against
            it after the trade.
          </p>
        </div>
      </section>

      <section>
        <div className="card table-wrap">
          <table className="trades-table">
            <thead>
              <tr>
                <th>Time</th><th>Trade</th><th className="num">Avg price</th><th className="num">Shares</th>
                <th className="num">Value</th><th className="num">Bid / ask<br />before</th><th className="num">Prices<br />hit</th>
                <th className="num">Extra<br />paid</th><th className="num">Waiting order<br />after 5 min</th><th />
              </tr>
            </thead>
            <tbody>
              {rows.slice((current - 1) * pageSize, current * pageSize).map((r) => (
                <tr key={rowKey(r)} className={fresh.has(rowKey(r)) ? 'fresh' : undefined}>
                  <td>{time(r.ts)}</td>
                  <td>
                    <span className={r.side === 'BUY' ? 'side buy' : 'side sell'}>{r.side === 'BUY' ? 'Buy' : 'Sell'}</span>{' '}
                    {r.outcome}
                  </td>
                  <td className="num">{cents(r.price)}</td>
                  <td className="num">{r.size < 10 ? r.size.toLocaleString('en-US', { maximumFractionDigits: 2 }) : shares(r.size)}</td>
                  <td className="num">{usd(r.notional)}</td>
                  <td className="num">{cents(r.pre_bid)} / {cents(r.pre_ask)}</td>
                  <td className="num">{r.levels ?? '?'}</td>
                  <td className="num">{signedCents(r.cost)}</td>
                  <td className="num">{signedCents(r.maker_pnl_300s)}</td>
                  <td><button className="link-button" onClick={() => onReplay(new Date(r.ts).getTime())}>Replay</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <Pager page={current} pageSize={pageSize} total={rows.length} sizes={PAGE_SIZES}
          onPage={setPage} onPageSize={(n) => { setPageSize(n); setPage(1) }} />
      </section>

      <section>
        <div className="section-head">
          <h2>Who came out ahead?</h2>
        </div>
        {both.value != null && paidSame != null && movedSame != null
          ? <PaidBreakdown paid={paidSame} moved={movedSame} count={both.count} />
          : <div className="card notice">Not enough trades with a 5-minute price yet.</div>}
      </section>

      <section>
        <div className="card notice depth-method">
          <h3>How this is calculated</h3>
          <p>
            Each row is one order that traded right away against orders already waiting on the book. Buy or Sell is
            what that trader did. The exchange reports one trade with the total size and average price, even when the
            order used up several prices. Prices hit counts them; ? means we couldn't match the trade to the book.
          </p>
          <p>
            The middle price is halfway between the best bid and ask. Extra paid is how far the average price was from
            the middle price just before the trade, against the trader: the cost of not waiting, before fees. Waiting
            order, 5 min values the other side of the trade at the middle price 5 minutes later. Trades appear here 5
            minutes after they happen. Times use the exchange clock.
          </p>
        </div>
      </section>
    </>
  )
}
