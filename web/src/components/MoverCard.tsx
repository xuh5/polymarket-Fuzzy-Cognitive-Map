import type { Market } from '../api'
import { pct, pointsChange } from '../format'
import Change from './Change'
import Sparkline from './Sparkline'

export default function MoverCard({ m }: { m: Market }) {
  const pts = pointsChange(m.mid, m.logodds_chg_60m)
  const dir = pts == null || Math.abs(pts) < 0.05 ? 'flat' : pts > 0 ? 'up' : 'down'
  return (
    <article className="card mover">
      <h3 className="mover-q" title={m.question}>{m.question}</h3>
      <div className="mover-row">
        <div>
          <div className="mover-prob">{pct(m.mid)}</div>
          <div className="mover-outcome">{m.outcome}</div>
        </div>
        <Change points={pts} size="lg" />
      </div>
      <Sparkline marketId={m.market_id} direction={dir} />
    </article>
  )
}
