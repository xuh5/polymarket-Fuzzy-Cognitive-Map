import { Link, NavLink } from 'react-router'
import type { Health } from '../api'

// 右上角的实时状态：最新指标 30 秒内算 Live，否则 Delayed，拿不到就是 Offline
function LiveBadge({ health, failed }: { health: Health | null; failed: boolean }) {
  const age = health?.age_seconds
  const state = failed ? 'offline' : age == null ? 'offline' : age <= 30 ? 'live' : 'delayed'
  const label = state === 'live' ? 'Live' : state === 'delayed' ? `Delayed ${Math.round(age!)}s` : 'Offline'
  return (
    <span className={`live ${state}`} title={label}>
      <span className="dot" aria-hidden="true" />
      <span className="label">{label}</span>
    </span>
  )
}

export function Logo() {
  // 标志：一左一右两组长短不一的横条，像订单簿两边的深度阶梯
  return (
    <svg width="22" height="22" viewBox="0 0 22 22" aria-hidden="true">
      <rect x="1" y="3" width="9" height="3" rx="1.5" fill="var(--accent)" />
      <rect x="4" y="9.5" width="6" height="3" rx="1.5" fill="var(--accent)" />
      <rect x="6" y="16" width="4" height="3" rx="1.5" fill="var(--accent)" />
      <rect x="12" y="3" width="4" height="3" rx="1.5" fill="var(--ink-2)" />
      <rect x="12" y="9.5" width="7" height="3" rx="1.5" fill="var(--ink-2)" />
      <rect x="12" y="16" width="9" height="3" rx="1.5" fill="var(--ink-2)" />
    </svg>
  )
}

export default function Header({ health, failed }: { health: Health | null; failed: boolean }) {
  return (
    <header className="site-header">
      <div className="container header-inner">
        <Link className="brand" to="/">
          <Logo />
          <span className="word">Depthscope</span>
        </Link>
        <nav className="nav">
          {/* NavLink 会给当前页面的链接加上 active 样式 */}
          <NavLink to="/" end>Markets</NavLink>
          <NavLink to="/signals">Signals</NavLink>
          <NavLink to="/status">Status</NavLink>
          <NavLink to="/about">About</NavLink>
        </nav>
        <LiveBadge health={health} failed={failed} />
      </div>
    </header>
  )
}
