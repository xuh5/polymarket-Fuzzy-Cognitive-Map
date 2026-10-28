// 图表共用：坐标轴刻度取「好看」的数（1、2、5 × 10 的整数次方）
export function niceStep(range: number, count: number) {
  const raw = range / count
  const pow = 10 ** Math.floor(Math.log10(raw))
  return [1, 2, 5, 10].map((m) => m * pow).find((s) => s >= raw) ?? raw
}

// lo 到 hi 之间、大约 count 个刻度
export function ticks(lo: number, hi: number, count: number) {
  const step = niceStep(hi - lo, count)
  const out: number[] = []
  for (let v = Math.ceil(lo / step - 1e-9) * step; v <= hi + 1e-9; v += step) out.push(Math.round(v / step) * step + 0)   // + 0 把 -0 变成 0，免得显示成 "$-0"
  return out
}
