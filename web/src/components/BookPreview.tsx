import { fetchBook, type Level } from '../api'
import { cents, shares } from '../format'
import { usePolling } from '../usePolling'

const ROWS = 6
export const PREVIEW_WIDTH = 260

// 鼠标在首页表格某一行停一会儿弹出的迷你订单簿：卖盘在上（最优价靠中间）、买盘在下，中间一行是价差和中间价
export default function BookPreview({ marketId, outcome, x, y }: {
  marketId: string; outcome: string; x: number; y: number
}) {
  const book = usePolling(() => fetchBook(marketId, 1), 2000, marketId)
  const b = book.data

  let body
  if (!b) {
    body = <div className="pv-empty">Loading order book…</div>
  } else if (!b.available) {
    body = <div className="pv-empty">Order book temporarily unavailable</div>
  } else {
    const asks = b.asks.slice(0, ROWS).reverse()
    const bids = b.bids.slice(0, ROWS)
    const max = Math.max(...[...asks, ...bids].map(([p, s]) => p * s), 1)
    const bestBid = b.bids[0]?.[0]
    const bestAsk = b.asks[0]?.[0]
    const row = ([p, s]: Level, side: 'bid' | 'ask') => (
      <div key={`${side}${p}`} className={`pv-row ${side}`}>
        <span className="pv-bar" style={{ width: `${((p * s) / max) * 100}%` }} />
        <span className="pv-price">{cents(p)}</span>
        <span className="pv-size">{shares(s)}</span>
      </div>
    )
    body = (
      <>
        {asks.map((l) => row(l, 'ask'))}
        <div className="pv-mid">
          {bestBid != null && bestAsk != null
            ? <>Spread {cents(bestAsk - bestBid)} · Mid {cents((bestAsk + bestBid) / 2)}</>
            : 'One side of the book is empty'}
        </div>
        {bids.map((l) => row(l, 'bid'))}
      </>
    )
  }

  return (
    <div className="card book-preview" style={{ left: x, top: y, width: PREVIEW_WIDTH }} role="tooltip">
      <div className="pv-head">
        <span>Order book · {outcome}</span>
        <span className="muted">Price / Shares</span>
      </div>
      {body}
    </div>
  )
}
