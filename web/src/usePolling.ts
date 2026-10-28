import { useEffect, useState } from 'react'

// 每隔 ms 毫秒调一次 load，返回最近一次成功的结果；key 变了（比如换了市场）就清空重来
export function usePolling<T>(load: () => Promise<T>, ms: number, key: string) {
  const [state, setState] = useState<{ key: string; data: T | null; failed: boolean }>(
    { key, data: null, failed: false },
  )

  useEffect(() => {
    let alive = true
    const run = () =>
      load()
        .then((data) => { if (alive) setState({ key, data, failed: false }) })
        .catch(() => { if (alive) setState((s) => ({ ...s, key, failed: true })) })
    run()
    const timer = setInterval(run, ms)
    return () => { alive = false; clearInterval(timer) }
    // load 每次渲染都是新函数，只按 key 和 ms 重新订阅
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, ms])

  // key 刚变、新数据还没回来时，不把上一个市场的数据显示出来
  return state.key === key ? state : { key, data: null, failed: false }
}

// 量容器宽度，SVG 图表按实际宽度画（文字不会被拉伸变形）
export function useWidth<E extends HTMLElement>() {
  const [el, setEl] = useState<E | null>(null)
  const [width, setWidth] = useState(0)
  useEffect(() => {
    if (!el) return
    const ro = new ResizeObserver(([entry]) => setWidth(entry.contentRect.width))
    ro.observe(el)
    return () => ro.disconnect()
  }, [el])
  return [setEl, width] as const
}
