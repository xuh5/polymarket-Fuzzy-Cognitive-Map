import { useState } from 'react'
import type { Level } from '../api'
import { cents, shares, usd } from '../format'
import { simulateFill } from '../slippage'

const PRESETS = [100, 1000, 10000]

// 滑点计算器：输入金额，按当前盘口模拟主动吃单，看成交均价比中间价差多少
export default function SlippageCalc({ bids, asks, outcome }: { bids: Level[]; asks: Level[]; outcome: string }) {
  const [side, setSide] = useState<'buy' | 'sell'>('buy')
  const [amount, setAmount] = useState('1000')

  const levels = side === 'buy' ? asks : bids
  const fill = simulateFill(levels, Number(amount))
  const mid = bids[0] && asks[0] ? (bids[0][0] + asks[0][0]) / 2 : null
  // 买：均价比中间价高多少；卖：均价比中间价低多少。都用正数表示「吃亏了多少」
  const impact = fill && mid != null ? (side === 'buy' ? fill.avgPrice - mid : mid - fill.avgPrice) : null

  return (
    <div className="card calc">
      <h3>Slippage calculator</h3>
      <div className="segmented full" role="tablist" aria-label="Side">
        <button role="tab" aria-selected={side === 'buy'} className={side === 'buy' ? 'active' : ''}
          onClick={() => setSide('buy')}>Buy {outcome}</button>
        <button role="tab" aria-selected={side === 'sell'} className={side === 'sell' ? 'active' : ''}
          onClick={() => setSide('sell')}>Sell {outcome}</button>
      </div>
      <label className="calc-input">
        <span>$</span>
        <input type="number" min="0" step="100" value={amount} onChange={(e) => setAmount(e.target.value)}
          aria-label="Order size in dollars" />
      </label>
      <div className="presets">
        {PRESETS.map((p) => (
          <button key={p} onClick={() => setAmount(String(p))}>{usd(p)}</button>
        ))}
      </div>

      {fill ? (
        <dl className="calc-out">
          <div><dt>Avg fill price</dt><dd>{cents(fill.avgPrice)}</dd></div>
          <div><dt>Slippage vs mid</dt><dd className={impact != null && impact > 0.005 ? 'warn' : ''}>
            {impact == null ? '—' : `${(impact * 100).toFixed(2)}¢`}
            {impact != null && mid ? <span className="muted"> ({((impact / mid) * 100).toFixed(1)}%)</span> : null}
          </dd></div>
          <div><dt>Worst price hit</dt><dd>{cents(fill.worstPrice)}</dd></div>
          <div><dt>{side === 'buy' ? 'Shares bought' : 'Shares sold'}</dt><dd>{shares(fill.shares)}</dd></div>
          <div><dt>Levels used</dt><dd>{fill.levels}</dd></div>
        </dl>
      ) : (
        <p className="muted calc-note">Enter an amount to simulate a market order.</p>
      )}
      {fill && !fill.complete && (
        <p className="calc-warn">Only {usd(fill.spent)} can fill — the book runs out.</p>
      )}
      <p className="muted calc-note">Simulated against the live book. Excludes fees.</p>
    </div>
  )
}
