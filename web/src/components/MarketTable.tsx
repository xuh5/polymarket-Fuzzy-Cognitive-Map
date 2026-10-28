import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useNavigate } from 'react-router'
import { polymarketUrl, type Market } from '../api'
import { ago, cents, endDate, pct, pointsChange, secondsAgo } from '../format'
import BookPreview, { PREVIEW_WIDTH } from './BookPreview'
import Change from './Change'
import Flash from './Flash'

type SortKey = 'moves' | 'prob' | 'name'
const SORTS: { key: SortKey; label: string }[] = [
  { key: 'moves', label: 'Biggest moves' },
  { key: 'prob', label: 'Probability' },
  { key: 'name', label: 'A–Z' },
]
const PAGE_SIZES = [10, 25, 50]

// 超过这么久没更新的市场（多半已移出订阅名单）淡化显示
const STALE_SECONDS = 15 * 60

// 鼠标在市场名字上停多久才弹出迷你订单簿（太短的话扫过表格时会一直弹）
const HOVER_DELAY_MS = 2000
// 离开市场名字后小窗口多留一会儿，够把鼠标移进小窗口里
const CLOSE_GRACE_MS = 150
const PREVIEW_HALF_HEIGHT = 170   // 卡片大约一半高，用来让它不超出窗口上下边

type Row = { m: Market; stale: boolean; pts: number | null }

// 页码按钮：页数少就全列出来，多了只留首尾和当前页附近，中间用 … 省略
function pageList(page: number, pages: number): (number | '…')[] {
  if (pages <= 7) return Array.from({ length: pages }, (_, i) => i + 1)
  const near = [page - 1, page, page + 1].filter((p) => p > 1 && p < pages)
  return [1, ...(near[0] > 2 ? ['…' as const] : []), ...near, ...(near.at(-1)! < pages - 1 ? ['…' as const] : []), pages]
}

export default function MarketTable({ markets }: { markets: Market[] }) {
  const [query, setQuery] = useState('')
  const [sort, setSort] = useState<SortKey>('moves')
  const [pageSize, setPageSize] = useState(25)
  const [page, setPage] = useState(1)
  const navigate = useNavigate()

  // 悬停弹出的迷你订单簿。只响应鼠标（pointerType === 'mouse'）：手机上手指点屏幕不会弹
  const [preview, setPreview] = useState<{ id: string; outcome: string; x: number; y: number } | null>(null)
  const timer = useRef<number | undefined>(undefined)        // 等着弹出
  const closeTimer = useRef<number | undefined>(undefined)   // 等着关掉
  const mouseX = useRef(0)

  const keepPreview = () => window.clearTimeout(closeTimer.current)
  const closePreviewSoon = () => {
    window.clearTimeout(timer.current)
    window.clearTimeout(closeTimer.current)
    closeTimer.current = window.setTimeout(() => setPreview(null), CLOSE_GRACE_MS)
  }

  const openPreviewSoon = (e: React.PointerEvent<HTMLElement>, m: Market) => {
    if (e.pointerType !== 'mouse') return
    keepPreview()
    if (preview?.id === m.market_id) return     // 从小窗口回到同一个名字：保持打开
    window.clearTimeout(timer.current)
    mouseX.current = e.clientX
    const el = e.currentTarget
    timer.current = window.setTimeout(() => {
      const r = el.getBoundingClientRect()
      // 优先放在鼠标左边，左边放不下就放右边
      const leftSide = mouseX.current - 16 - PREVIEW_WIDTH
      const x = leftSide >= 8 ? leftSide : mouseX.current + 16
      const y = Math.min(Math.max(r.top + r.height / 2, PREVIEW_HALF_HEIGHT), window.innerHeight - PREVIEW_HALF_HEIGHT)
      setPreview({ id: m.market_id, outcome: m.outcome, x, y })
    }, HOVER_DELAY_MS)
  }

  // 卡片是固定在窗口上的，页面一滚动位置就对不上了，直接关掉；离开页面时清掉计时器
  useEffect(() => {
    if (!preview) return
    const onScroll = () => setPreview(null)
    window.addEventListener('scroll', onScroll, { passive: true })
    return () => window.removeEventListener('scroll', onScroll)
  }, [preview])
  useEffect(() => () => { window.clearTimeout(timer.current); window.clearTimeout(closeTimer.current) }, [])

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
                  {/* 只有鼠标停在市场名字上才弹迷你订单簿，不是整行 */}
                  <Link className="q" to={`/markets/${m.market_id}`} onClick={(e) => e.stopPropagation()}
                    onPointerEnter={(e) => openPreviewSoon(e, m)}
                    onPointerMove={(e) => { mouseX.current = e.clientX }}
                    onPointerLeave={closePreviewSoon}>
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
              <tr><td colSpan={6} className="empty">No markets match “{query}”.</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {preview && (
        <BookPreview marketId={preview.id} outcome={preview.outcome} x={preview.x} y={preview.y}
          onEnter={keepPreview} onLeave={closePreviewSoon} />
      )}

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
