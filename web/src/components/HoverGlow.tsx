import { forwardRef, useImperativeHandle, useRef } from 'react'

export interface HoverGlowHandle {
  start: (x: number, y: number) => void   // 在鼠标位置出现，开始慢慢变亮
  move: (x: number, y: number) => void
  stop: (done?: boolean) => void          // done = 计时到了（向外散开淡掉）；否则直接消失
}

const SIZE = 64

// 跟着鼠标的一团柔光：悬停计时的这 2 秒里慢慢变亮变大，告诉用户「再等一下就弹出来」。
// 系统鼠标本身改不了，这是一个跟着鼠标走的元素。位置直接改 DOM 的 style，
// 不走 React 状态：鼠标每动一下都触发整张表重新渲染太浪费
const HoverGlow = forwardRef<HoverGlowHandle, { durationMs: number }>(function HoverGlow({ durationMs }, ref) {
  const el = useRef<HTMLDivElement>(null)

  useImperativeHandle(ref, () => {
    const place = (x: number, y: number) => {
      if (el.current) el.current.style.transform = `translate(${x - SIZE / 2}px, ${y - SIZE / 2}px)`
    }
    return {
      start(x, y) {
        const node = el.current
        if (!node) return
        place(x, y)
        node.classList.remove('active', 'done')
        void node.offsetWidth          // 强制浏览器重排一次，动画才会从头开始播
        node.classList.add('active')
      },
      move: place,
      stop(done = false) {
        const node = el.current
        if (!node) return
        node.classList.remove('active')
        if (done) node.classList.add('done')
      },
    }
  }, [])

  return (
    <div ref={el} className="hover-glow" aria-hidden="true" style={{ '--glow-ms': `${durationMs}ms` } as React.CSSProperties}>
      <span className="glow" />
    </div>
  )
})

export default HoverGlow
