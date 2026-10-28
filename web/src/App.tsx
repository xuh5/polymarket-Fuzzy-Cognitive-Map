import { useEffect, useMemo, useState } from 'react'
import { fetchHealth, fetchMarkets, type Health, type Market } from './api'
import Header from './components/Header'
import MarketTable from './components/MarketTable'
import MoverCard from './components/MoverCard'
import { pointsChange, secondsAgo } from './format'

const REFRESH_MS = 5000
const MOVERS = 4
// 异动榜只看最近还在更新的市场（已移出订阅名单的市场，60 分钟变化是旧的）
const MOVER_MAX_AGE_SECONDS = 15 * 60

export default function App() {
  const [markets, setMarkets] = useState<Market[]>([])
  const [health, setHealth] = useState<Health | null>(null)
  const [failed, setFailed] = useState(false)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    const load = () =>
      Promise.all([fetchMarkets(), fetchHealth()])
        .then(([m, h]) => { setMarkets(m); setHealth(h); setFailed(false) })
        .catch(() => setFailed(true))
        .finally(() => setLoaded(true))
    load()
    const timer = setInterval(load, REFRESH_MS)
    return () => clearInterval(timer)
  }, [])

  const movers = useMemo(
    () =>
      markets
        .filter((m) => secondsAgo(m.updated_at) <= MOVER_MAX_AGE_SECONDS)
        .map((m) => ({ m, pts: pointsChange(m.mid, m.logodds_chg_60m) }))
        .filter((x) => x.pts != null)
        .sort((a, b) => Math.abs(b.pts!) - Math.abs(a.pts!))
        .slice(0, MOVERS)
        .map((x) => x.m),
    [markets],
  )
  const tracked = markets.filter((m) => secondsAgo(m.updated_at) <= MOVER_MAX_AGE_SECONDS).length

  return (
    <>
      <Header health={health} failed={failed} />
      <main className="container">
        <section className="hero">
          <h1>Order book analytics for prediction markets</h1>
          <p>
            Live depth, spreads and price moves for tech &amp; AI markets on Polymarket, rebuilt tick by tick
            from the public order book feed.
          </p>
          {loaded && !failed && (
            <div className="hero-stats">
              <span><strong>{tracked}</strong> markets live</span>
              <span><strong>{markets.length}</strong> seen in the last 24h</span>
            </div>
          )}
        </section>

        {failed && markets.length === 0 && (
          <div className="card notice">The data service is offline right now. Please check back in a few minutes.</div>
        )}

        {movers.length > 0 && (
          <section>
            <div className="section-head">
              <h2>Top movers <span className="muted">· last 60 min</span></h2>
            </div>
            <div className="movers">
              {movers.map((m) => <MoverCard key={m.market_id} m={m} />)}
            </div>
          </section>
        )}

        {markets.length > 0 && <MarketTable markets={markets} />}
      </main>

      <footer className="site-footer">
        <div className="container">
          Data from Polymarket's public APIs. Depthscope is not affiliated with Polymarket. Not investment advice.
        </div>
      </footer>
    </>
  )
}
