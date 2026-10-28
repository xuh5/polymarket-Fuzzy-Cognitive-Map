import type { Level } from './api'

export interface Fill {
  spent: number        // 实际成交的美元金额
  shares: number       // 买到 / 卖出的份数
  avgPrice: number     // 成交均价
  worstPrice: number   // 吃到的最差一档
  levels: number       // 吃了几档
  complete: boolean    // 盘口够不够吃完这个金额
}

// 模拟按金额主动吃单：买就从卖一往上吃，卖就从买一往下吃（levels 要按最优价在前排好）。
// 不含手续费；假设下单这一刻盘口不变
export function simulateFill(levels: Level[], amountUsd: number): Fill | null {
  if (!(amountUsd > 0) || levels.length === 0) return null
  let left = amountUsd
  let shares = 0
  let used = 0
  let worst = levels[0][0]
  for (const [price, size] of levels) {
    if (left <= 1e-9) break
    const take = Math.min(left, price * size)
    shares += take / price
    left -= take
    worst = price
    used += 1
  }
  const spent = amountUsd - left
  return { spent, shares, avgPrice: spent / shares, worstPrice: worst, levels: used, complete: left <= 1e-9 }
}
