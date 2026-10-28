// 概率变化（百分点）：涨绿跌红，同时带 ▲▼ 和正负号，色盲也分得清
export default function Change({ points, size = 'sm' }: { points: number | null; size?: 'sm' | 'lg' }) {
  if (points == null) return <span className="change flat">—</span>
  const dir = points >= 0.05 ? 'up' : points <= -0.05 ? 'down' : 'flat'
  const arrow = dir === 'up' ? '▲' : dir === 'down' ? '▼' : ''
  const sign = dir === 'up' ? '+' : ''
  return (
    <span className={`change ${dir} ${size}`}>
      {arrow && <span aria-hidden="true">{arrow}</span>}
      {sign}{(dir === 'flat' ? Math.abs(points) : points).toFixed(1)} pts
    </span>
  )
}
