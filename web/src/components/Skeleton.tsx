// 数据回来之前的灰色占位块，大小和真内容差不多，加载完不会跳来跳去
export function SkeletonBlock({ w = '100%', h = 14 }: { w?: number | string; h?: number }) {
  return <span className="skeleton" style={{ width: w, height: h }} aria-hidden="true" />
}

export function SkeletonMovers() {
  return (
    <div className="movers" aria-busy="true" aria-label="Loading top movers">
      {[0, 1, 2, 3].map((i) => (
        <div key={i} className="card mover">
          <SkeletonBlock w="90%" />
          <SkeletonBlock w="60%" />
          <SkeletonBlock w={110} h={30} />
          <SkeletonBlock h={56} />
        </div>
      ))}
    </div>
  )
}

export function SkeletonTable({ rows = 8 }: { rows?: number }) {
  return (
    <div className="card table-wrap" aria-busy="true" aria-label="Loading markets">
      <table>
        <tbody>
          {Array.from({ length: rows }, (_, i) => (
            <tr key={i}>
              <td><SkeletonBlock w="70%" /><div style={{ height: 6 }} /><SkeletonBlock w="30%" h={10} /></td>
              <td className="num"><SkeletonBlock w={50} /></td>
              <td className="num hide-sm"><SkeletonBlock w={96} h={24} /></td>
              <td className="num hide-sm"><SkeletonBlock w={90} /></td>
              <td className="num"><SkeletonBlock w={60} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
