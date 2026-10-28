import { useLayoutEffect, useRef, useState } from 'react'
import { Link, NavLink, useLocation } from 'react-router'
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

const NAV = [
  { to: '/', label: 'Markets', end: true },
  { to: '/signals', label: 'Signals' },
  { to: '/fuzzy-map', label: 'Fuzzy Map' },
  { to: '/status', label: 'Status' },
  { to: '/about', label: 'About' },
]

type Box = { x: number; w: number }
const boxOf = (el: HTMLElement): Box => ({ x: el.offsetLeft, w: el.offsetWidth })

// 导航栏 + 两个动画：当前页的下划线滑到新 tab；鼠标下的浅色底块跟着鼠标在 tab 之间滑
function Nav() {
  const navRef = useRef<HTMLElement>(null)
  const { pathname } = useLocation()
  const [active, setActive] = useState<Box | null>(null)
  const [hover, setHover] = useState<Box & { visible: boolean; slide: boolean }>(
    { x: 0, w: 0, visible: false, slide: false },
  )

  // 换页面、窗口大小变了、字体加载完（文字宽度会变），都重新量一下当前 tab 的位置
  useLayoutEffect(() => {
    const measure = () => {
      const el = navRef.current?.querySelector<HTMLElement>('a.active')
      setActive(el ? boxOf(el) : null)
    }
    measure()
    document.fonts?.ready.then(measure)
    window.addEventListener('resize', measure)
    return () => window.removeEventListener('resize', measure)
  }, [pathname])

  return (
    <nav className="nav" ref={navRef} onMouseLeave={() => setHover((h) => ({ ...h, visible: false }))}>
      <span className={`nav-hover${hover.visible ? ' visible' : ''}${hover.slide ? ' slide' : ''}`}
        style={{ transform: `translateX(${hover.x}px)`, width: hover.w }} aria-hidden="true" />
      {NAV.map((n) => (
        // NavLink 会给当前页面的链接加上 active 样式
        <NavLink key={n.to} to={n.to} end={n.end}
          onMouseEnter={(e) => {
            const box = boxOf(e.currentTarget)
            setHover((h) => ({ ...box, visible: true, slide: h.visible }))
          }}>
          {n.label}
        </NavLink>
      ))}
      {active && (
        <span className="nav-indicator" style={{ transform: `translateX(${active.x}px)`, width: active.w }}
          aria-hidden="true" />
      )}
    </nav>
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
        <Nav />
        <LiveBadge health={health} failed={failed} />
      </div>
    </header>
  )
}
