import { useState } from 'react'
import { Link } from 'react-router'
import { polymarketUrl, type Side, type Twin, type TwinQuote, type TwinRelation } from '../api'
import { ago, cents, shortId } from '../format'

// Related 标签：别的事件里、按结算规则和这个市场绑在一起的市场（只列两边都在订阅、都还开着的）。
// 一个孪生一行（同一个市场常有十几个，用卡片太长、一样的框看着累）；后端已按缺口从大到小排好。
// 点 Details 展开下面一整行：理由、成立条件、规则差异、为什么这组买法一定拿回 $1、谁判断的

const RELATION_TEXT: Record<TwinRelation, string> = {
  exclusive: "Can't both be Yes",
  complement: 'Exactly one is Yes',
  identical: 'Same question',
  implies: 'If this is Yes, that is Yes',
  implied_by: 'If that is Yes, this is Yes',
}

// 为什么这组买法至少拿回 $1（按关系和买的方向）
function payoff(relation: TwinRelation, mine: Side): string {
  switch (relation) {
    case 'exclusive': return "They can't both be Yes, so at least one of the two No pays $1."
    case 'complement': return `Exactly one is Yes, so one of the two ${mine} pays $1.`
    case 'implies': return 'If this is Yes, that is Yes too, so either this No or that Yes pays $1.'
    case 'implied_by': return 'If that is Yes, this is Yes too, so either this Yes or that No pays $1.'
    case 'identical': return 'It is the same question, so exactly one of the two sides pays $1.'
  }
}

// 吃单买 Yes 付卖一，买 No 付 1 − Yes 买一（No 的盘口是 Yes 的镜像）
function legPrice(side: Side, q: TwinQuote | null) {
  if (!q) return null
  if (side === 'Yes') return q.ask
  return q.bid == null ? null : 1 - q.bid
}

const quote = (q: TwinQuote | null) => (q ? `${cents(q.bid)} / ${cents(q.ask)}` : '—')
const modelName = (m: string | null) => (m ? m.replace(/^claude:/, 'Claude ').replace(/\b\w/g, (c) => c.toUpperCase()) : 'AI')

function Row({ t }: { t: Twin }) {
  const [open, setOpen] = useState(false)
  const g = t.gap
  const edge = g ? Math.round(g.edge * 1000) / 10 : null   // 分，保留一位
  const midEdge = t.mid_edge == null ? null : Math.round(t.mid_edge * 1000) / 10
  const why = g ? undefined : t.this_quote && t.other_quote
    ? 'One side has no orders at the price needed (the result may already be settled)'
    : 'No live order book for one of the two right now'
  return (
    <>
      <tr className={open ? 'open' : undefined}>
        <td>
          <Link to={`/markets/${t.market_id}`} className="q">{t.question}</Link>
          <span className="mono twin-id">#{shortId(t.market_id)}</span>
          {t.reason && <div className="twin-why">{t.reason}</div>}
          <button className="twin-toggle" aria-expanded={open} onClick={() => setOpen(!open)}>
            {open ? 'Hide details ▴' : `Details${t.differences.length ? ` · ${t.differences.length} rule differences` : ''} ▾`}
          </button>
        </td>
        <td>
          <span className="chip">{RELATION_TEXT[t.relation]}</span>
          {t.strength === 'conditional' && <div className="twin-cond">Only if: {t.condition || 'see details'}</div>}
        </td>
        <td className="num">{quote(t.other_quote)}</td>
        <td title={why}>
          {g ? <>{g.this} here {cents(legPrice(g.this, t.this_quote))}<br />{g.other} there {cents(legPrice(g.other, t.other_quote))}</> : '—'}
        </td>
        <td className="num" title={why}>{g ? cents(g.cost) : '—'}</td>
        <td className={`num twin-gap ${edge != null && edge > 0 ? 'up' : 'flat'}`} title={why}>
          {edge == null ? '—' : `${edge > 0 ? '+' : ''}${edge.toFixed(1)}¢`}
          {midEdge != null && (
            <span className={`twin-mid${midEdge > 0 ? ' pos' : ''}`} title="Same trade at both markets' mid prices">
              mid {midEdge > 0 ? '+' : ''}{midEdge.toFixed(1)}¢
            </span>
          )}
        </td>
        <td className="link-col">
          {t.slug && (
            <a className="ext" href={polymarketUrl(t.slug)} target="_blank" rel="noreferrer"
              title="Open on Polymarket" aria-label="Open on Polymarket">↗</a>
          )}
        </td>
      </tr>
      {open && (
        <tr className="twin-detail">
          <td colSpan={7}>
            <dl>
              <dt>Why they're tied</dt>
              <dd>{t.reason || '—'}</dd>
              {t.strength === 'conditional' && (<><dt>Holds only if</dt><dd>{t.condition || '—'}</dd></>)}
              <dt>Rule differences</dt>
              <dd>
                {t.differences.length
                  ? <ul>{t.differences.map((d, i) => <li key={i}>{d}</li>)}</ul>
                  : 'None found: same source, same check time, same boundaries.'}
              </dd>
              <dt>The trade</dt>
              <dd>
                {g
                  ? <>Buy {g.this} here at {cents(legPrice(g.this, t.this_quote))} and {g.other} there at{' '}
                    {cents(legPrice(g.other, t.other_quote))}: {cents(g.cost)} in total. {payoff(t.relation, g.this)}
                    {t.strength === 'conditional' && ' (Only if the condition above holds.)'} Before fees.</>
                  : why}
              </dd>
              <dt>Checked</dt>
              <dd>
                {t.source === 'manual' ? 'Set by hand' : `${modelName(t.model)} read both rule texts`}
                {t.judged_at && `, ${ago(t.judged_at)}`} · {t.confidence} confidence
              </dd>
              <dt>Links</dt>
              <dd className="twin-links">
                <Link to={`/markets/${t.market_id}`}>Its page here</Link>
                {t.slug && <a className="ext-link" href={polymarketUrl(t.slug)} target="_blank" rel="noreferrer">On Polymarket ↗</a>}
              </dd>
            </dl>
          </td>
        </tr>
      )}
    </>
  )
}

export default function TwinsPanel({ twins }: { twins: Twin[] }) {
  if (!twins.length) {
    return <div className="card notice">No tracked market in another event is tied to this one right now.</div>
  }
  const open = twins.filter((t) => t.gap && t.gap.edge > 0).length
  const me = twins[0].this_quote
  return (
    <section>
      <p className="twins-intro">
        {twins.length} tracked {twins.length === 1 ? 'market' : 'markets'} in other events {twins.length === 1 ? 'is' : 'are'} tied
        to this one by the rules, so some results can't happen together. This market's Yes: {quote(me)}.{' '}
        <strong>{open > 0 ? `${open} with a gap right now.` : 'No gap right now.'}</strong>
      </p>
      <div className="card table-wrap">
        <table className="twins-table">
          <thead>
            <tr>
              <th>Market in another event</th><th>Tied by</th><th className="num">That Yes<br />bid / ask</th>
              <th>Buy</th><th className="num">Cost</th><th className="num">Gap</th>
              <th className="link-col"><span className="sr-only">Polymarket</span></th>
            </tr>
          </thead>
          <tbody>{twins.map((t) => <Row key={t.market_id} t={t} />)}</tbody>
        </table>
      </div>
      <p className="fine-print">
        Found by an AI reading both markets' full rules, checked every hour. Buy is the cheapest combination that
        always pays at least $1, at the current best prices; the gap is what's left of the $1. "mid" is the same
        trade at both markets' mid prices, which you only get by placing orders and waiting. Before fees, and the
        size at the best price may be small. "—" means one side has no orders at the price needed.
      </p>
    </section>
  )
}
