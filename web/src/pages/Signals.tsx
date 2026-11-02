import { useMemo, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router'
import { fetchSignals, type MoveType, type Signal, type SignalKind } from '../api'
import Change from '../components/Change'
import { SkeletonTable } from '../components/Skeleton'
import { cents, dateTime, usd } from '../format'
import {
  DEFAULT_FILTER, filterFromParams, filterToParams, kept, matches, replayLink, type JumpFilter, type SignalFilter,
} from '../signals'
import { usePolling } from '../usePolling'

const RANGES = [
  { key: '24h', hours: 24 },
  { key: '7d', hours: 168 },
  { key: '30d', hours: 720 },
] as const
const DEFAULT_RANGE = '7d'
const PAGE = 50

// signal 的类型：网页上的名字。以后每加一种，在这里和 SignalKind 里各加一项，再写它自己的筛选和 Details
const KINDS: { key: SignalKind | null; label: string }[] = [
  { key: null, label: 'All types' },
  { key: 'jump', label: 'Price jump' },
]
const KIND_LABEL: Record<SignalKind, string> = { jump: 'Price jump' }

const MOVE_POINTS = [3, 5, 10, 20]
const RATIOS = [1, 6, 30]
const NOTIONALS = [10, 100, 1000]
const SPREADS = [0.02, 0.05]

// 盘口的三种动法，网页上的名字和解释
const MOVES: { key: MoveType; label: string; hint: string }[] = [
  { key: 'both', label: 'Repriced', hint: 'Bid and ask moved together: the market changed its price' },
  { key: 'wider', label: 'Side pulled', hint: 'The spread widened: orders on one side were pulled or eaten' },
  { key: 'narrower', label: 'Side filled', hint: 'The spread narrowed: a new order appeared on one side' },
]
const MOVE_LABEL: Record<MoveType, string> = { both: 'Repriced', wider: 'Side pulled', narrower: 'Side filled', other: 'Other' }
const MOVES_HINT = MOVES.map((m) => `${m.label}: ${m.hint}`).join('\n')

function Segmented<T extends string | number | null>({ label, options, value, onChange, format }: {
  label: string; options: readonly T[]; value: T; onChange: (v: T) => void; format: (v: T) => string
}) {
  return (
    <div className="segmented" role="tablist" aria-label={label}>
      {options.map((o) => (
        <button key={String(o)} role="tab" aria-selected={o === value} className={o === value ? 'active' : ''}
          onClick={() => onChange(o)}>{format(o)}</button>
      ))}
    </div>
  )
}

// 下拉框：Any = null
function Pick({ label, options, value, onChange, format }: {
  label: string; options: number[]; value: number | null; onChange: (v: number | null) => void; format: (v: number) => string
}) {
  return (
    <label className="filter">
      <span>{label}</span>
      <select value={value ?? ''} onChange={(e) => onChange(e.target.value === '' ? null : Number(e.target.value))}>
        <option value="">Any</option>
        {options.map((o) => <option key={o} value={o}>{format(o)}</option>)}
      </select>
    </label>
  )
}

// 价格跳动自己的筛选
function JumpFilters({ f, set }: { f: JumpFilter; set: (patch: Partial<JumpFilter>) => void }) {
  const toggleMove = (t: MoveType) => {
    const cur = f.types ?? MOVES.map((x) => x.key)
    const next = cur.includes(t) ? cur.filter((x) => x !== t) : [...cur, t]
    set({ types: next.length === MOVES.length ? null : next })
  }
  return (
    <>
      <div className="filter-row">
        <div className="filter">
          <span>Move at least</span>
          <Segmented label="Move at least" options={MOVE_POINTS} value={f.minPoints}
            onChange={(v) => set({ minPoints: v })} format={(v) => `${v} pts`} />
        </div>
        <Pick label="Volume vs usual" options={RATIOS} value={f.minRatio}
          onChange={(v) => set({ minRatio: v })} format={(v) => `≥ ${v}×`} />
        <Pick label="Traded" options={NOTIONALS} value={f.minNotional}
          onChange={(v) => set({ minNotional: v })} format={(v) => `≥ ${usd(v)}`} />
        <Pick label="Spread at peak" options={SPREADS} value={f.maxSpread}
          onChange={(v) => set({ maxSpread: v })} format={(v) => `≤ ${Math.round(v * 100)}¢`} />
      </div>
      <div className="filter-row">
        <div className="filter">
          <span>How the book moved <span className="info" title={MOVES_HINT} aria-label={MOVES_HINT}>ⓘ</span></span>
          <div className="toggles">
            {MOVES.map((t) => (
              <label key={t.key} className="toggle" title={t.hint}>
                <input type="checkbox" checked={!f.types || f.types.includes(t.key)} onChange={() => toggleMove(t.key)} />
                {t.label}
              </label>
            ))}
          </div>
        </div>
        <div className="toggles">
          <label className="toggle" title="Buyers pushed it up, or sellers pushed it down">
            <input type="checkbox" checked={f.withFlow} onChange={(e) => set({ withFlow: e.target.checked })} />
            Trades with the move
          </label>
          <label className="toggle" title="The price didn't flip back and forth during the move">
            <input type="checkbox" checked={f.noReversals} onChange={(e) => set({ noReversals: e.target.checked })} />
            No back-and-forth
          </label>
        </div>
      </div>
    </>
  )
}

// 表格里各类型自己的「怎么回事」和「详情」
function JumpWhat({ s }: { s: Signal }) {
  return (
    <div className="sub-line">
      <span>{MOVE_LABEL[s.move_type]}</span>
      {s.reversals > 0 && <span title="Times the price changed direction during the move">· flipped {s.reversals}×</span>}
    </div>
  )
}

function JumpDetails({ s }: { s: Signal }) {
  const agrees = s.flow != null && s.flow * s.change_points > 0
  return (
    <>
      <div className="details-head">
        <Change points={s.change_points} />
        <span className="muted">{cents(s.mid_before)} → {cents(s.mid_peak)}</span>
      </div>
      <div className="sub-line">
        {s.trades === 0 ? <span>No trades</span> : (
          <>
            <span>{s.trades} {s.trades === 1 ? 'trade' : 'trades'} · {usd(s.notional)}</span>
            {s.volume_ratio != null && <span>{s.volume_ratio.toFixed(s.volume_ratio < 10 ? 1 : 0)}× usual</span>}
            {s.flow != null && <span className={agrees ? '' : 'against'}>{agrees ? 'with move' : 'against move'}</span>}
          </>
        )}
        <span>spread {cents(s.spread_before)} → {cents(s.spread_peak)}</span>
      </div>
    </>
  )
}

function After({ s }: { s: Signal }) {
  if (s.status !== 'final') return <span className="muted">Pending</span>
  const k = kept(s)
  if (k == null) return <span className="muted">—</span>
  const [label, cls] = k >= 0.7 ? ['Held', 'held'] : k <= 0.3 ? ['Reverted', 'reverted'] : ['Partly held', '']
  return (
    <>
      <div className={`outcome ${cls}`}>{label}</div>
      <div className="sub-line"><span>{cents(s.mid_after_30m)}</span></div>
    </>
  )
}

function KindBadge({ s }: { s: Signal }) {
  return <span className={`chip kind-${s.kind}`}>{KIND_LABEL[s.kind] ?? s.kind}</span>
}

export default function Signals() {
  const [params, setParams] = useSearchParams()
  const [shown, setShown] = useState(PAGE)
  const navigate = useNavigate()

  const range = RANGES.find((r) => r.key === params.get('range')) ?? RANGES.find((r) => r.key === DEFAULT_RANGE)!
  const filter = useMemo(() => filterFromParams(params), [params])
  const signals = usePolling(() => fetchSignals(range.hours), 30000, range.key)

  // 筛选条件都放在网址里：刷新不丢，复制链接给别人看到的是同一份结果
  const update = (f: SignalFilter, r: string = range.key) => {
    setParams({ ...(r !== DEFAULT_RANGE ? { range: r } : {}), ...filterToParams(f) }, { replace: true })
    setShown(PAGE)
  }

  const all = signals.data ?? []
  const rows = all.filter((s) => matches(s, filter))
  const isDefault = Object.keys(filterToParams(filter)).length === 0

  return (
    <>
      <section className="hero">
        <h1>Signals</h1>
        <p>
          Unusual activity detected in the live order book. Pick a type and filter it down;
          click any signal to replay that moment tick by tick.
        </p>
      </section>

      <section className="card filters">
        <div className="filter-row spread-row">
          <div className="filter">
            <span>Type</span>
            <Segmented label="Signal type" options={KINDS.map((k) => k.key)} value={filter.kind}
              onChange={(k) => update({ ...DEFAULT_FILTER, kind: k })}
              format={(k) => KINDS.find((x) => x.key === k)!.label} />
          </div>
          <Segmented label="Time range" options={RANGES.map((r) => r.key)} value={range.key}
            onChange={(r) => update(filter, r)} format={(r) => `Last ${r}`} />
        </div>
        {filter.kind === null && (
          <p className="filter-note">
            Price jumps: the Yes price moved at least 5 points within 5 minutes. Pick a type for more filters.
          </p>
        )}
        {filter.kind === 'jump' && (
          <div className="kind-filters">
            <p className="filter-note">The Yes price moved sharply within 5 minutes.</p>
            <JumpFilters f={filter.jump} set={(patch) => update({ ...filter, jump: { ...filter.jump, ...patch } })} />
          </div>
        )}
      </section>

      <section>
        <div className="section-head">
          <h2>
            {signals.data ? <>{rows.length} <span className="muted">signals</span></> : 'Signals'}
          </h2>
          {!isDefault && <button className="link-button" onClick={() => update(DEFAULT_FILTER)}>Reset filters</button>}
        </div>

        {!signals.data && signals.failed && (
          <div className="card notice">The data service is offline right now. Please check back in a few minutes.</div>
        )}
        {!signals.data && !signals.failed && <SkeletonTable />}

        {signals.data && (
          <div className="card table-wrap">
            <table className="signals">
              <thead>
                <tr>
                  <th className="when">Time</th>
                  <th className="hide-sm">Signal</th>
                  <th>Market</th>
                  <th className="hide-sm">Details</th>
                  <th className="num">After 30 min</th>
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, shown).map((s) => (
                  // 整行可点，跳到那一刻的回放；问题文字是真正的链接（右键新标签页也能用）
                  <tr key={s.event_id} className="clickable" onClick={() => navigate(replayLink(s))}>
                    <td className="when">
                      {dateTime(Date.parse(s.peak_at))}
                      {s.status === 'open' && <div><span className="chip live-chip">Happening now</span></div>}
                    </td>
                    <td className="hide-sm">
                      <KindBadge s={s} />
                      {s.kind === 'jump' && <JumpWhat s={s} />}
                    </td>
                    <td>
                      <Link className="q" to={replayLink(s)} onClick={(e) => e.stopPropagation()}>
                        {s.question ?? s.market_id}
                      </Link>
                      {s.outcome && s.outcome !== 'Yes' && <div className="sub"><span className="chip">{s.outcome}</span></div>}
                      {/* 手机上 Time、Signal 两列不显示，放到市场名下面 */}
                      <div className="sub only-sm">
                        <span>{dateTime(Date.parse(s.peak_at))}</span>
                        <KindBadge s={s} />
                        {s.status === 'open' && <span className="chip live-chip">Happening now</span>}
                      </div>
                      {s.kind === 'jump' && (
                        <div className="only-sm details-sm"><Change points={s.change_points} /></div>
                      )}
                    </td>
                    <td className="hide-sm">{s.kind === 'jump' && <JumpDetails s={s} />}</td>
                    <td className="num">{s.kind === 'jump' && <After s={s} />}</td>
                  </tr>
                ))}
                {rows.length === 0 && (
                  <tr><td colSpan={5} className="empty">
                    {all.length === 0 ? 'No signals in this period yet.' : 'No signals match these filters.'}
                  </td></tr>
                )}
              </tbody>
            </table>
          </div>
        )}
        {rows.length > shown && (
          <div className="more">
            <button className="link-button" onClick={() => setShown(shown + PAGE)}>
              Show {Math.min(PAGE, rows.length - shown)} more
            </button>
          </div>
        )}
      </section>
    </>
  )
}
