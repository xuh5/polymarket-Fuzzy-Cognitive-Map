// Trades 标签的重点：吃单方多付的钱，有多少被之后的价格变动吃掉，挂单方还剩多少。
// 画成阶梯条：第一行多付的、第二行价格往吃单方走了多少、第三行是两者之差（挂单方剩下的），
// 第三行从第二行的末端画到第一行的末端，一眼看出差距。三个数用同一批成交算，正好 多付 = 变动 + 剩下
const fmt = (v: number) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v * 100).toFixed(2)}¢`

function verdict(paid: number, kept: number) {
  if (kept <= 0) return 'Waiting orders lost money: the price moved further than the extra takers paid.'
  if (paid > 0 && kept < 0.3 * paid) return 'Takers earned back most of the extra they paid. Waiting orders kept little.'
  return 'Waiting orders kept most of the extra that takers paid.'
}

export default function PaidBreakdown({ paid, moved, count }: { paid: number; moved: number; count: number }) {
  const kept = paid - moved
  const lo = Math.min(0, paid, moved)
  const hi = Math.max(0, paid, moved) || 1e-9
  const x = (v: number) => ((v - lo) / (hi - lo)) * 100
  const bar = (from: number, to: number, cls: string) => (
    <span className={`pb-bar ${cls}`} style={{ left: `${x(Math.min(from, to))}%`, width: `${Math.max(x(Math.max(from, to)) - x(Math.min(from, to)), 0.6)}%` }} />
  )
  const rows = [
    { label: 'Takers paid extra', note: 'over the middle price, to trade right away', value: paid, el: bar(0, paid, 'paid') },
    { label: 'Then the price moved their way', note: 'over the next 5 minutes', value: moved, el: bar(0, moved, 'moved') },
    { label: 'Left for the waiting orders', note: 'what the orders they hit kept', value: kept, el: bar(moved, paid, kept >= 0 ? 'kept' : 'lost') },
  ]
  return (
    <div className="card paid-breakdown">
      <p className="pb-verdict">{verdict(paid, kept)}</p>
      <div className="pb-rows">
        {rows.map((r) => (
          <div key={r.label} className="pb-row">
            <div className="pb-label">{r.label}<span className="muted">{r.note}</span></div>
            <div className="pb-track">
              {lo < 0 && <span className="pb-zero" style={{ left: `${x(0)}%` }} />}
              {r.el}
            </div>
            <div className="pb-value">{fmt(r.value)}</div>
          </div>
        ))}
      </div>
      <p className="fine-print">Cents per share, bigger trades count more · {count} trades · fees not included</p>
    </div>
  )
}
