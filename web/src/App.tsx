import { useEffect, useState } from 'react'
import { Route, Routes } from 'react-router'
import { fetchHealth, type Health } from './api'
import Footer from './components/Footer'
import Header from './components/Header'
import MarketPage from './pages/MarketPage'
import Markets from './pages/Markets'
import Placeholder from './pages/Placeholder'

const HEALTH_REFRESH_MS = 5000

// 所有页面共用顶栏和页脚；顶栏右上角的实时状态在这里统一拉
export default function App() {
  const [health, setHealth] = useState<Health | null>(null)
  const [failed, setFailed] = useState(false)

  useEffect(() => {
    const load = () =>
      fetchHealth()
        .then((h) => { setHealth(h); setFailed(false) })
        .catch(() => setFailed(true))
    load()
    const timer = setInterval(load, HEALTH_REFRESH_MS)
    return () => clearInterval(timer)
  }, [])

  return (
    <>
      <Header health={health} failed={failed} />
      <main className="container">
        <Routes>
          <Route path="/" element={<Markets />} />
          <Route path="/markets/:marketId" element={<MarketPage />} />
          <Route path="/signals" element={
            <Placeholder title="Signals"
              description="Markets with suspicious depth, and recent alerts for large moves and liquidity drops." />
          } />
          <Route path="/fuzzy-map" element={
            <Placeholder title="Fuzzy Map"
              description="A fuzzy cognitive map of how markets and events push each other's odds up or down." />
          } />
          <Route path="/status" element={
            <Placeholder title="Status"
              description="Live health of the data pipeline: latency, throughput, consumer lag and resyncs." />
          } />
          <Route path="/about" element={
            <Placeholder title="About"
              description="How Depthscope works: from the exchange feed to your browser." />
          } />
          <Route path="*" element={
            <Placeholder title="Page not found" description="This page doesn't exist." />
          } />
        </Routes>
      </main>
      <Footer />
    </>
  )
}
