import { useEffect, useMemo, useState } from 'react'
import { fetchMarkets, type Market } from '../api'
import MarketTable from '../components/MarketTable'
import MoverCard from '../components/MoverCard'
import { SkeletonBlock, SkeletonMovers, SkeletonTable } from '../components/Skeleton'
import { pointsChange, secondsAgo } from '../format'

const REFRESH_MS = 5000
const MOVERS = 4
// 异动榜只看最近还在更新的市场（已移出订阅名单的市场，60 分钟变化是旧的）
const MOVER_MAX_AGE_SECONDS = 15 * 60

export default function Markets() {
  const [markets, setMarkets] = useState<Market[]>([])
  const [failed, setFailed] = useState(false)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    const load = () =>
      fetchMarkets()
        .then((m) => { setMarkets(m); setFailed(false) })
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
      <section className="hero">
        <h1>Order book analytics for prediction markets</h1>
        <p>
          Live depth, spreads and price moves for tech &amp; AI markets on Polymarket, rebuilt tick by tick
          from the public order book feed.
        </p>
        {!loaded && (
          <div className="hero-stats"><SkeletonBlock w={120} /><SkeletonBlock w={160} /></div>
        )}
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

      {!loaded && (
        <>
          <section>
            <div className="section-head"><h2>Top movers <span className="muted">· last 60 min</span></h2></div>
            <SkeletonMovers />
          </section>
          <section>
            <div className="section-head"><h2>All markets</h2></div>
            <SkeletonTable />
          </section>
        </>
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
    </>
  )
}
