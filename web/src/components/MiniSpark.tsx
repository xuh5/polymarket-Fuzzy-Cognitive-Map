// 表格里的小走势图（24 小时，每 30 分钟一个点）。只看形状和涨跌方向，不带坐标轴和悬停
const W = 96
const H = 28

export default function MiniSpark({ points }: { points: [number, number][] | undefined }) {
  if (!points || points.length < 2) return <span className="muted">—</span>
  const mids = points.map((p) => p[1])
  let lo = Math.min(...mids)
  let hi = Math.max(...mids)
  if (hi - lo < 0.01) {          // 至少 1¢ 的纵向范围，几乎不动的市场画出来就是一条平线
    const c = (hi + lo) / 2
    lo = c - 0.005
    hi = c + 0.005
  }
  const t0 = points[0][0]
  const t1 = points[points.length - 1][0]
  const d = points
    .map(([t, m], i) => `${i ? 'L' : 'M'}${(((t - t0) / (t1 - t0)) * (W - 4) + 2).toFixed(1)},` +
      `${((1 - (m - lo) / (hi - lo)) * (H - 4) + 2).toFixed(1)}`)
    .join('')
  const change = mids[mids.length - 1] - mids[0]
  const dir = change > 0.0005 ? 'up' : change < -0.0005 ? 'down' : 'flat'
  return (
    <svg width={W} height={H} className="mini-spark" role="img"
      aria-label={`24h trend: ${dir === 'flat' ? 'flat' : dir === 'up' ? 'up' : 'down'}`}>
      <path d={d} className={`spark-line ${dir}`} />
    </svg>
  )
}
