import type { Level } from '../api'
import { cents, shares, usd } from '../format'

const ROWS = 10

// 买卖各前 10 档：价格、份数、金额；背景条长度 = 这一档金额占两边最大一档的比例
export default function BookLadder({ bids, asks }: { bids: Level[]; asks: Level[] }) {
  const b = bids.slice(0, ROWS)
  const a = asks.slice(0, ROWS)
  const max = Math.max(...[...b, ...a].map(([p, s]) => p * s), 1)

  const side = (rows: Level[], cls: 'bids' | 'asks', title: string) => (
    <table className={`ladder ${cls}`}>
      <thead>
        <tr><th>{title}</th><th className="num">Shares</th><th className="num">Value</th></tr>
      </thead>
      <tbody>
        {rows.map(([p, s]) => (
          <tr key={p}>
            <td className="price">
              <span className="bar" style={{ width: `${((p * s) / max) * 100}%` }} aria-hidden="true" />
              {cents(p)}
            </td>
            <td className="num">{shares(s)}</td>
            <td className="num">{usd(p * s)}</td>
          </tr>
        ))}
        {rows.length === 0 && <tr><td colSpan={3} className="muted">Empty</td></tr>}
      </tbody>
    </table>
  )

  return (
    <div className="ladders">
      {side(b, 'bids', 'Bid')}
      {side(a, 'asks', 'Ask')}
    </div>
  )
}
