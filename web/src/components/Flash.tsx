import { useState, type ReactNode } from 'react'

// 数值变了就闪一下：变大绿色、变小红色，1 秒后褪掉。用来让人一眼看出数据是实时的
export default function Flash({ value, children }: { value: number | null; children: ReactNode }) {
  // React 推荐的写法：渲染时和上一次的值比较（不用 effect），变了就记下方向，并换 key 让动画重新播放
  const [prev, setPrev] = useState(value)
  const [flash, setFlash] = useState<{ dir: 'up' | 'down' | null; n: number }>({ dir: null, n: 0 })
  if (value !== prev) {
    setPrev(value)
    if (value != null && prev != null) setFlash((f) => ({ dir: value > prev ? 'up' : 'down', n: f.n + 1 }))
  }
  return <span key={flash.n} className={flash.dir ? `flash flash-${flash.dir}` : 'flash'}>{children}</span>
}
