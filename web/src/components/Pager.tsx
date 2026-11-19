// 表格翻页：第几条到第几条、每页几条、页码按钮。市场表和成交表共用

// 页码按钮：页数少就全列出来，多了只留首尾和当前页附近，中间用 … 省略
function pageList(page: number, pages: number): (number | '…')[] {
  if (pages <= 7) return Array.from({ length: pages }, (_, i) => i + 1)
  const near = [page - 1, page, page + 1].filter((p) => p > 1 && p < pages)
  return [1, ...(near[0] > 2 ? ['…' as const] : []), ...near, ...(near.at(-1)! < pages - 1 ? ['…' as const] : []), pages]
}

export default function Pager({ page, pageSize, total, sizes, onPage, onPageSize }: {
  page: number                  // 当前第几页（从 1 开始，调用方保证不超过总页数）
  pageSize: number
  total: number
  sizes: number[]               // 每页条数的可选值
  onPage: (p: number) => void
  onPageSize: (n: number) => void
}) {
  const pages = Math.max(1, Math.ceil(total / pageSize))
  return (
    <div className="pager">
      <span className="muted">
        {(page - 1) * pageSize + 1}–{Math.min(page * pageSize, total)} of {total}
      </span>
      <label className="page-size">
        Rows per page
        <select value={pageSize} onChange={(e) => onPageSize(Number(e.target.value))}>
          {sizes.map((n) => <option key={n} value={n}>{n}</option>)}
        </select>
      </label>
      <nav className="pages" aria-label="Pages">
        <button disabled={page === 1} onClick={() => onPage(page - 1)} aria-label="Previous page">‹</button>
        {pageList(page, pages).map((p, i) =>
          p === '…'
            ? <span key={`gap${i}`} className="muted">…</span>
            : <button key={p} className={p === page ? 'active' : ''} aria-current={p === page ? 'page' : undefined}
                onClick={() => onPage(p)}>{p}</button>,
        )}
        <button disabled={page === pages} onClick={() => onPage(page + 1)} aria-label="Next page">›</button>
      </nav>
    </div>
  )
}
