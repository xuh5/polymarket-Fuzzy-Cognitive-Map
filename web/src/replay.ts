import type { Level, Replay, ReplayEvent, ReplayPeriod } from './api'

// 回放的纯逻辑：从起点订单簿出发，把后端给的变化依次套上去（规则和后端 BookStore 一样）

export const LENGTHS = [10, 20, 30, 60] as const   // 可选的回放长度（分钟），后端最长 60

export const minuteFloor = (t: number) => Math.floor(t / 60000) * 60000

// 一段录制的最后 minutes 分钟从哪一刻开始（这段比它还短就从这段开头）
export function windowAtEnd(p: ReplayPeriod, minutes: number) {
  return Math.max(minuteFloor(Date.parse(p.start)), minuteFloor(Date.parse(p.end) - minutes * 60000))
}

// 这个窗口和哪一段录制有重叠；都没有就是 -1（那时这个市场不在订阅名单里，没有数据）
export const periodOf = (periods: ReplayPeriod[], start: number, end: number) =>
  periods.findIndex((p) => Date.parse(p.start) < end && Date.parse(p.end) > start)

type Side = Map<number, number>   // 价格 → 份数
export type LiveBook = { bid: Side; ask: Side } | null   // null = 这一刻盘口不可用

const toBook = (bids: Level[], asks: Level[]) => ({ bid: new Map(bids), ask: new Map(asks) })

function initialBooks(r: Replay) {
  return new Map<number, LiveBook>(r.outcomes.map((o) => [o.outcome_index, o.initial && toBook(o.initial.bids, o.initial.asks)]))
}

function apply(books: Map<number, LiveBook>, e: ReplayEvent) {
  if (e.type === 'book') books.set(e.o, toBook(e.bids, e.asks))
  else if (e.type === 'change') {
    const side = books.get(e.o)?.[e.side]
    if (!side) return
    if (e.size === 0) side.delete(e.price)
    else side.set(e.price, e.size)
  } else if (e.type === 'gap' || e.type === 'closed') {
    for (const o of books.keys()) books.set(o, null)
  }
}

export function levels(b: NonNullable<LiveBook>) {
  return {
    bids: [...b.bid].sort((x, y) => y[0] - x[0]) as Level[],
    asks: [...b.ask].sort((x, y) => x[0] - y[0]) as Level[],
  }
}

const bestBid = (b: NonNullable<LiveBook>) => (b.bid.size ? Math.max(...b.bid.keys()) : null)
const bestAsk = (b: NonNullable<LiveBook>) => (b.ask.size ? Math.min(...b.ask.keys()) : null)

export interface PricePoint {
  t: number
  bid: number | null
  ask: number | null
  live: boolean   // false：盘口不可用，线在这里断开
}

// 一次过一遍所有事件，算出：
// - 每个选项的买一卖一走势（只在变了时记一个点，画阶梯线）
// - 每条变化之前这一档原来是多少份（流水里显示「+300」、「撤了 300」）
export function analyze(r: Replay) {
  const books = initialBooks(r)
  const start = new Date(r.start).getTime()
  const series = new Map<number, PricePoint[]>()
  const prev: number[] = new Array(r.events.length).fill(0)

  const record = (o: number, t: number) => {
    const b = books.get(o)
    const p: PricePoint = b ? { t, bid: bestBid(b), ask: bestAsk(b), live: true } : { t, bid: null, ask: null, live: false }
    const pts = series.get(o)!
    const last = pts[pts.length - 1]
    if (!last || last.bid !== p.bid || last.ask !== p.ask || last.live !== p.live) pts.push(p)
  }
  for (const o of books.keys()) {
    series.set(o, [])
    record(o, start)
  }
  r.events.forEach((e, i) => {
    if (e.type === 'change') prev[i] = books.get(e.o)?.[e.side].get(e.price) ?? 0
    apply(books, e)
    if (e.type === 'book' || e.type === 'change') record(e.o, e.t)
    else if (e.type === 'gap' || e.type === 'closed') for (const o of books.keys()) record(o, e.t)
  })
  return { series, prev }
}

// 播放头：往后走只套新的事件，往回拖才从起点重算（几千条，一瞬间）
export class Cursor {
  private r: Replay | null = null
  private t = -Infinity
  idx = 0                                      // 已经套上的事件条数
  books = new Map<number, LiveBook>()
  changedBy = new Map<string, number>()        // 「选项:买卖:价格」→ 最后一次改它的事件下标（订单簿闪一下用）

  seek(r: Replay, t: number) {
    if (r !== this.r || t < this.t) {
      this.r = r
      this.idx = 0
      this.books = initialBooks(r)
      this.changedBy = new Map()
    }
    while (this.idx < r.events.length && r.events[this.idx].t <= t) {
      const e = r.events[this.idx]
      apply(this.books, e)
      if (e.type === 'change') this.changedBy.set(`${e.o}:${e.side}:${e.price}`, this.idx)
      this.idx++
    }
    this.t = t
    return this
  }
}
