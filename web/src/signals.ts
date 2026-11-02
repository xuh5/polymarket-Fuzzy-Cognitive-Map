// Signals 的筛选规则。规则是一个固定格式的对象（SignalFilter），网址参数、网页控件都只是它的两种写法；
// 以后做自定义推送，后端按同样的字段和含义实现一遍 matches 就行。
// 结构：先选类型（kind），再是这个类型自己的条件；每加一种 signal，就加一组条件（像 jump 这样）。
import type { MoveType, Signal, SignalKind } from './api'
import { LENGTHS, minuteFloor } from './replay'

// 价格跳动（kind = jump）的条件
export interface JumpFilter {
  minPoints: number            // |变化| ≥ 几个点
  minRatio: number | null      // 成交 ≥ 平时的几倍（没有基准的跳动不算通过）
  minNotional: number | null   // 成交金额 ≥ 多少美元
  types: MoveType[] | null     // 只看这几种盘口动法；null = 全部
  withFlow: boolean            // 成交方向和跳动方向一致（买的推上去 / 卖的砸下来）
  noReversals: boolean         // 期间没有来回跳
  maxSpread: number | null     // 峰值时价差 ≤ 多少（0～1，0.05 = 5¢）
}

export interface SignalFilter {
  kind: SignalKind | null      // null = 所有类型，每种类型按自己的默认条件
  jump: JumpFilter
}

export const DEFAULT_JUMP: JumpFilter = {
  minPoints: 5, minRatio: null, minNotional: null, types: null, withFlow: false, noReversals: false, maxSpread: null,
}
export const DEFAULT_FILTER: SignalFilter = { kind: null, jump: DEFAULT_JUMP }

function matchesJump(s: Signal, f: JumpFilter): boolean {
  if (Math.abs(s.change_points) < f.minPoints) return false
  if (f.minRatio != null && !(s.volume_ratio != null && s.volume_ratio >= f.minRatio)) return false
  if (f.minNotional != null && s.notional < f.minNotional) return false
  if (f.types && !f.types.includes(s.move_type)) return false
  if (f.withFlow && !(s.flow != null && s.flow * s.change_points > 0)) return false
  if (f.noReversals && s.reversals > 0) return false
  if (f.maxSpread != null && !(s.spread_peak != null && s.spread_peak <= f.maxSpread + 1e-9)) return false
  return true
}

export function matches(s: Signal, f: SignalFilter): boolean {
  if (f.kind && s.kind !== f.kind) return false
  const own = f.kind ? f : DEFAULT_FILTER   // 看所有类型时，各类型用默认条件
  if (s.kind === 'jump') return matchesJump(s, own.jump)
  return true
}

// 网址参数：kind=jump 时才带上跳动的条件；只写和默认值不同的，链接短一些
const num = (v: string | null) => (v == null || v === '' || Number.isNaN(Number(v)) ? null : Number(v))
const KINDS: SignalKind[] = ['jump']

export function filterFromParams(p: URLSearchParams): SignalFilter {
  const kind = KINDS.find((k) => k === p.get('kind')) ?? null
  if (kind !== 'jump') return { kind, jump: DEFAULT_JUMP }
  const types = p.get('type')?.split(',').filter((t): t is MoveType =>
    ['both', 'wider', 'narrower', 'other'].includes(t)) ?? null
  return {
    kind,
    jump: {
      minPoints: num(p.get('min')) ?? DEFAULT_JUMP.minPoints,
      minRatio: num(p.get('ratio')),
      minNotional: num(p.get('usd')),
      types: types?.length ? types : null,
      withFlow: p.get('flow') === '1',
      noReversals: p.get('steady') === '1',
      maxSpread: num(p.get('spread')),
    },
  }
}

export function filterToParams(f: SignalFilter): Record<string, string> {
  if (!f.kind) return {}
  const p: Record<string, string> = { kind: f.kind }
  const j = f.jump
  if (j.minPoints !== DEFAULT_JUMP.minPoints) p.min = String(j.minPoints)
  if (j.minRatio != null) p.ratio = String(j.minRatio)
  if (j.minNotional != null) p.usd = String(j.minNotional)
  if (j.types) p.type = j.types.join(',')
  if (j.withFlow) p.flow = '1'
  if (j.noReversals) p.steady = '1'
  if (j.maxSpread != null) p.spread = String(j.maxSpread)
  return p
}

// 30 分钟后价格守住了跳动的多少：1 = 全守住，0 = 全回去了
export function kept(s: Signal): number | null {
  if (s.mid_after_30m == null || s.mid_peak === s.mid_before) return null
  return (s.mid_after_30m - s.mid_before) / (s.mid_peak - s.mid_before)
}

// 点一行跳到那个市场的回放：从开始前 10 分钟放，够看到跳完再 5 分钟；播放头停在开始前 30 秒
const LEAD_MS = 10 * 60000
const AFTER_MS = 5 * 60000
const PLAYHEAD_LEAD_MS = 30000

export function replayLink(s: Signal): string {
  const startAt = Date.parse(s.start_at)
  const from = minuteFloor(startAt - LEAD_MS)
  const need = Date.parse(s.last_at) + AFTER_MS - from
  const len = LENGTHS.find((l) => l * 60000 >= need) ?? LENGTHS[LENGTHS.length - 1]
  const q = new URLSearchParams({ replay: new Date(from).toISOString(), len: String(len), at: String(startAt - PLAYHEAD_LEAD_MS) })
  return `/markets/${s.market_id}?${q}`
}
