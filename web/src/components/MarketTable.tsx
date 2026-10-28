import { useMemo, useState } from 'react'
import type { Market } from '../api'
import { ago, cents, endDate, pct, pointsChange, secondsAgo } from '../format'
import Change from './Change'

type SortKey = 'moves' | 'prob' | 'name'
const SORTS: { key: SortKey; label: string }[] = [
  { key: 'moves', label: 'Biggest moves' },
  { key: 'prob', label: 'Probability' },
  { key: 'name', label: 'A–Z' },
]

// 超过这么久没更新的市场（多半已移出订阅名单）淡化显示
const STALE_SECONDS = 15 * 60

type Row = { m: Market; stale: boolean; pts: number | null }

export default function MarketTable({ markets }: { markets: Market[] }) {
  const [query, setQuery] = useState('')
  const [sort, setSort] = useState<SortKey>('moves')

  const rows = useMemo(() => {
    const q = query.trim().toLowerCase()
    const list = markets
      .filter((m) => !q || m.question.toLowerCase().includes(q))
      .map((m) => {
        const stale = secondsAgo(m.updated_at) > STALE_SECONDS
        // 停止更新的市场，60 分钟变化是停之前的旧值，不显示
        return { m, stale, pts: stale ? null : pointsChange(m.mid, m.logodds_chg_60m) }
      })
    const by = {
      moves: (a: Row, b: Row) => Math.abs(b.pts ?? 0) - Math.abs(a.pts ?? 0),
      prob: (a: Row, b: Row) => (b.m.mid ?? -1) - (a.m.mid ?? -1),
      name: (a: Row, b: Row) => a.m.question.localeCompare(b.m.question),
    }[sort]
    // 不管按什么排，停止更新的都排在后面
    return list.sort((a, b) => Number(a.stale) - Number(b.stale) || by(a, b))
  }, [markets, query, sort])

  return (
    <section>
      <div className="section-head">
        <h2>All markets</h2>
        <div className="controls">
          <input className="search" type="search" placeholder="Search markets" value={query}
            onChange={(e) => setQuery(e.target.value)} aria-label="Search markets" />
          <div className="segmented" role="tablist" aria-label="Sort by">
            {SORTS.map((s) => (
              <button key={s.key} role="tab" aria-selected={sort === s.key}
                className={sort === s.key ? 'active' : ''} onClick={() => setSort(s.key)}>
                {s.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="card table-wrap">
        <table>
          <thead>
            <tr>
              <th>Market</th>
              <th className="num">Probability</th>
              <th className="num hide-sm">Bid / Ask</th>
              <th className="num hide-sm">Spread</th>
              <th className="num">1h change</th>
              <th className="num hide-sm">Updated</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(({ m, stale, pts }) => (
              <tr key={m.market_id} className={stale ? 'stale' : ''}>
                <td>
                  <div className="q">{m.question}</div>
                  <div className="sub">
                    {m.outcome !== 'Yes' && <span className="chip">{m.outcome}</span>}
                    {endDate(m.end_date) && <span>Ends {endDate(m.end_date)}</span>}
                  </div>
                </td>
                <td className="num">
                  <div className="prob">{pct(m.mid)}</div>
                  <div className="meter" aria-hidden="true">
                    <span style={{ width: `${(m.mid ?? 0) * 100}%` }} />
                  </div>
                </td>
                <td className="num hide-sm">{cents(m.best_bid)} <span className="muted">/</span> {cents(m.best_ask)}</td>
                <td className="num hide-sm">{cents(m.spread)}</td>
                <td className="num"><Change points={pts} /></td>
                <td className="num hide-sm muted">{ago(m.updated_at)}</td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr><td colSpan={6} className="empty">No markets match “{query}”.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  )
}
