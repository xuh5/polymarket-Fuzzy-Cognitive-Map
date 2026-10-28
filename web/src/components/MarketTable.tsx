import { useMemo, useState } from 'react'
import { Link, useNavigate } from 'react-router'
import { polymarketUrl, type Market, type Sparklines } from '../api'
import { ago, cents, endDate, pct, pointsChange, secondsAgo } from '../format'
import Change from './Change'
import Flash from './Flash'
import MiniSpark from './MiniSpark'

type SortKey = 'moves' | 'prob' | 'name'
const SORTS: { key: SortKey; label: string }[] = [
  { key: 'moves', label: 'Biggest moves' },
  { key: 'prob', label: 'Probability' },
  { key: 'name', label: 'A–Z' },
]
const PAGE_SIZES = [10, 25, 50]

// 超过这么久没更新的市场（多半已移出订阅名单）淡化显示
const STALE_SECONDS = 15 * 60

type Row = { m: Market; stale: boolean; pts: number | null }

// 页码按钮：页数少就全列出来，多了只留首尾和当前页附近，中间用 … 省略
function pageList(page: number, pages: number): (number | '…')[] {
  if (pages <= 7) return Array.from({ length: pages }, (_, i) => i + 1)
  const near = [page - 1, page, page + 1].filter((p) => p > 1 && p < pages)
  return [1, ...(near[0] > 2 ? ['…' as const] : []), ...near, ...(near.at(-1)! < pages - 1 ? ['…' as const] : []), pages]
}

export default function MarketTable({ markets, sparks }: { markets: Market[]; sparks: Sparklines }) {
  const [query, setQuery] = useState('')
  const [sort, setSort] = useState<SortKey>('moves')
  const [pageSize, setPageSize] = useState(25)
  const [page, setPage] = useState(1)
  const navigate = useNavigate()

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

  const pages = Math.max(1, Math.ceil(rows.length / pageSize))
  const current = Math.min(page, pages)          // 搜索后结果变少时，页码不会超出范围
  const shown = rows.slice((current - 1) * pageSize, current * pageSize)
  // 搜索、换排序、换每页条数都回到第 1 页
  const reset = <T,>(set: (v: T) => void) => (v: T) => { set(v); setPage(1) }

  return (
    <section>
      <div className="section-head">
        <h2>All markets</h2>
        <div className="controls">
          <input className="search" type="search" placeholder="Search markets" value={query}
            onChange={(e) => reset(setQuery)(e.target.value)} aria-label="Search markets" />
          <div className="segmented" role="tablist" aria-label="Sort by">
            {SORTS.map((s) => (
              <button key={s.key} role="tab" aria-selected={sort === s.key}
                className={sort === s.key ? 'active' : ''} onClick={() => reset(setSort)(s.key)}>
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
              <th className="num hide-sm">24h</th>
              <th className="num hide-sm">Bid / Ask</th>
              <th className="num hide-md">Spread</th>
              <th className="num">1h change</th>
              <th className="link-col"><span className="sr-only">Polymarket</span></th>
            </tr>
          </thead>
          <tbody>
            {shown.map(({ m, stale, pts }) => (
              // 整行可点，进市场详情页；问题文字是真正的链接（键盘、右键新标签页都能用）
              <tr key={m.market_id} className={`clickable${stale ? ' stale' : ''}`}
                onClick={() => navigate(`/markets/${m.market_id}`)}>
                <td>
                  <Link className="q" to={`/markets/${m.market_id}`} onClick={(e) => e.stopPropagation()}>
                    {m.question}
                  </Link>
                  <div className="sub">
                    {m.outcome !== 'Yes' && <span className="chip">{m.outcome}</span>}
                    {endDate(m.end_date) && <span>Ends {endDate(m.end_date)}</span>}
                    <span>· {ago(m.updated_at)}</span>
                  </div>
                </td>
                <td className="num">
                  <div className="prob"><Flash value={m.mid}>{pct(m.mid)}</Flash></div>
                  <div className="meter" aria-hidden="true">
                    <span style={{ width: `${(m.mid ?? 0) * 100}%` }} />
                  </div>
                </td>
                <td className="num hide-sm"><MiniSpark points={sparks[m.market_id]} /></td>
                <td className="num hide-sm">
                  <Flash value={m.best_bid}>{cents(m.best_bid)}</Flash>
                  <span className="muted"> / </span>
                  <Flash value={m.best_ask}>{cents(m.best_ask)}</Flash>
                </td>
                <td className="num hide-md">{cents(m.spread)}</td>
                <td className="num"><Change points={pts} /></td>
                <td className="link-col">
                  {m.slug && (
                    <a className="ext" href={polymarketUrl(m.slug)} target="_blank" rel="noreferrer"
                      title="Open on Polymarket" aria-label="Open on Polymarket" onClick={(e) => e.stopPropagation()}>
                      ↗
                    </a>
                  )}
                </td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr><td colSpan={7} className="empty">No markets match “{query}”.</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {rows.length > 0 && (
        <div className="pager">
          <span className="muted">
            {(current - 1) * pageSize + 1}–{Math.min(current * pageSize, rows.length)} of {rows.length}
          </span>
          <label className="page-size">
            Rows per page
            <select value={pageSize} onChange={(e) => reset(setPageSize)(Number(e.target.value))}>
              {PAGE_SIZES.map((n) => <option key={n} value={n}>{n}</option>)}
            </select>
          </label>
          <nav className="pages" aria-label="Pages">
            <button disabled={current === 1} onClick={() => setPage(current - 1)} aria-label="Previous page">‹</button>
            {pageList(current, pages).map((p, i) =>
              p === '…'
                ? <span key={`gap${i}`} className="muted">…</span>
                : <button key={p} className={p === current ? 'active' : ''} aria-current={p === current ? 'page' : undefined}
                    onClick={() => setPage(p)}>{p}</button>,
            )}
            <button disabled={current === pages} onClick={() => setPage(current + 1)} aria-label="Next page">›</button>
          </nav>
        </div>
      )}
    </section>
  )
}
