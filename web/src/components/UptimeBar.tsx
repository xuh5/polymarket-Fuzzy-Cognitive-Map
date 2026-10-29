import { useState } from 'react'
import { LEVEL_TEXT, type UptimeHour } from '../api'
import { dayClock, duration } from '../format'

// 过去 7 天每小时一格；鼠标停在格子上显示那一小时停了多久
export default function UptimeBar({ hours, label }: { hours: UptimeHour[]; label: string }) {
  const [hover, setHover] = useState<number | null>(null)
  const h = hover == null ? null : hours[hover]
  return (
    <div className="uptime-bar" onPointerLeave={() => setHover(null)} role="img"
      aria-label={`${label}: hourly availability over the last 7 days`}>
      {hours.map((c, i) => (
        <span key={c.start} className={`cell ${c.level}${hover === i ? ' hover' : ''}`}
          onPointerEnter={() => setHover(i)} />
      ))}
      {h && (
        <div className="tooltip uptime-tip" style={{ left: `${((hover! + 0.5) / hours.length) * 100}%` }}>
          <div className="tt-time">{dayClock(h.start)}</div>
          <div><span className={`status-dot ${h.level}`} />{LEVEL_TEXT[h.level]}</div>
          {h.down_seconds > 0 && <div>Down <b>{duration(h.down_seconds)}</b></div>}
        </div>
      )}
    </div>
  )
}
