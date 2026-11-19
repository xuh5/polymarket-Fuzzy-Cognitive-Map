import { useState } from 'react'

// Status 页的完整架构图：每个服务、Kafka topic 和存储一个方块，箭头 = 数据从哪流到哪。
// 坐标手工排好（从左到右：交易所 → 采集 → 原始 topic → 处理服务 → 指标 topic → 存储 → 对外），
// 窄屏时卡片里横向滚动。鼠标移到一个方块上，和它相连的线高亮，其余变淡

type Kind = 'external' | 'service' | 'topic' | 'store'
interface Node { id: string; x: number; y: number; w: number; label: string; sub?: string; kind: Kind }

const H = 44   // 方块高度
const NODES: Node[] = [
  { id: 'gamma', x: 0, y: 20, w: 124, label: 'Gamma API', sub: 'market list', kind: 'external' },
  { id: 'ws', x: 0, y: 200, w: 124, label: 'CLOB WebSocket', sub: 'books and trades', kind: 'external' },
  { id: 'comm', x: 0, y: 430, w: 124, label: 'Comments API', sub: 'polled', kind: 'external' },

  { id: 'fetch', x: 154, y: 20, w: 124, label: 'fetch-markets', sub: 'hourly', kind: 'service' },
  { id: 'clob', x: 154, y: 200, w: 124, label: 'collect-clob', sub: 'gap markers', kind: 'service' },
  { id: 'ccom', x: 154, y: 430, w: 124, label: 'collect-comments', sub: 'every 10 min', kind: 'service' },

  { id: 'tmk', x: 308, y: 20, w: 136, label: 'pm.raw.markets', sub: 'compacted', kind: 'topic' },
  { id: 'tclob', x: 308, y: 200, w: 136, label: 'pm.raw.clob', sub: 'kept 7 days', kind: 'topic' },
  { id: 'tcom', x: 308, y: 430, w: 136, label: 'pm.raw.comments', sub: 'compacted', kind: 'topic' },

  { id: 'metrics', x: 476, y: 90, w: 124, label: 'metrics', sub: 'per-second stats', kind: 'service' },
  { id: 'depth', x: 476, y: 170, w: 124, label: 'depth', sub: 'vanishing depth', kind: 'service' },
  { id: 'exec', x: 476, y: 250, w: 124, label: 'execution', sub: 'trade quality', kind: 'service' },
  { id: 'health', x: 476, y: 360, w: 124, label: 'health', sub: 'lag, latency', kind: 'service' },
  { id: 'archiver', x: 476, y: 430, w: 124, label: 'archiver', sub: 'Parquet every 5 min', kind: 'service' },

  { id: 'tbook', x: 636, y: 20, w: 150, label: 'pm.metrics.book', kind: 'topic' },
  { id: 'ttr', x: 636, y: 74, w: 150, label: 'pm.metrics.trades', kind: 'topic' },
  { id: 'signals', x: 636, y: 136, w: 150, label: 'signals', sub: 'price jumps', kind: 'service' },
  { id: 'tev', x: 636, y: 198, w: 150, label: 'pm.events', sub: 'compacted', kind: 'topic' },
  { id: 'tdep', x: 636, y: 260, w: 150, label: 'pm.metrics.depth', kind: 'topic' },
  { id: 'texec', x: 636, y: 314, w: 150, label: 'pm.metrics.execution', kind: 'topic' },

  { id: 'redis', x: 826, y: 20, w: 124, label: 'Redis', sub: 'live order books', kind: 'store' },
  { id: 'pgsink', x: 826, y: 168, w: 124, label: 'pg-sink', sub: 'idempotent upsert', kind: 'service' },
  { id: 'pg', x: 826, y: 250, w: 124, label: 'Postgres', sub: 'metrics, signals', kind: 'store' },
  { id: 'bronze', x: 826, y: 400, w: 124, label: 'Parquet archive', sub: 'every raw event', kind: 'store' },
  { id: 'dbt', x: 826, y: 476, w: 124, label: 'dbt + DuckDB', sub: 'daily, via Airflow', kind: 'store' },

  { id: 'site', x: 990, y: 20, w: 140, label: 'depthscope.app', sub: 'React, Cloudflare', kind: 'external' },
  { id: 'api', x: 990, y: 120, w: 140, label: 'api', sub: 'FastAPI, via Tunnel', kind: 'service' },
  { id: 'grafana', x: 990, y: 300, w: 140, label: 'Grafana', sub: 'dashboard, alerts', kind: 'service' },
  { id: 'discord', x: 990, y: 380, w: 140, label: 'Discord', sub: 'alert channel', kind: 'external' },
]

// [从, 到, 走法]。走法：默认从右边出、左边进；down / up = 同一列上下相连；loop = 同一列绕右边连；
// over / under = 从上方 / 下方绕过中间那一列 topic，不从方块后面穿过
const EDGES: [string, string, ('down' | 'up' | 'loop' | 'dashed' | 'over' | 'under')?][] = [
  ['gamma', 'fetch'], ['ws', 'clob'], ['comm', 'ccom'],
  ['fetch', 'tmk'], ['clob', 'tclob'], ['ccom', 'tcom'],
  ['tmk', 'clob', 'dashed'],
  ['tclob', 'metrics'], ['tclob', 'depth'], ['tclob', 'exec'], ['tclob', 'health'],
  ['tmk', 'archiver'], ['tclob', 'archiver'], ['tcom', 'archiver'],
  ['metrics', 'tbook'], ['metrics', 'ttr'], ['depth', 'tdep'], ['exec', 'texec'],
  ['tbook', 'signals', 'loop'], ['ttr', 'signals', 'loop'], ['signals', 'tev', 'down'],
  ['metrics', 'redis', 'over'],
  ['tbook', 'pgsink'], ['ttr', 'pgsink'], ['tev', 'pgsink'], ['tdep', 'pgsink'], ['texec', 'pgsink'],
  ['pgsink', 'pg', 'down'], ['health', 'pg', 'under'],
  ['archiver', 'bronze'], ['bronze', 'dbt', 'down'],
  ['redis', 'api'], ['pg', 'api'], ['bronze', 'api'], ['api', 'site', 'up'],
  ['pg', 'grafana'], ['grafana', 'discord', 'down'],
]

const COLUMNS = [
  { x: 0, label: 'Polymarket' }, { x: 154, label: 'Collect' }, { x: 308, label: 'Kafka' },
  { x: 476, label: 'Process' }, { x: 636, label: 'Kafka' }, { x: 826, label: 'Store' }, { x: 990, label: 'Serve' },
]
const TOP = 26           // 列标题占的高度，方块整体往下挪
const W = 1130
const HEIGHT = 476 + H + TOP + 8

const byId = Object.fromEntries(NODES.map((n) => [n.id, n]))

function path(a: Node, b: Node, how?: string) {
  const ay = a.y + TOP
  const by = b.y + TOP
  if (how === 'down') return `M${a.x + a.w / 2},${ay + H} L${b.x + b.w / 2},${by}`
  if (how === 'up') return `M${a.x + a.w / 2},${ay} L${b.x + b.w / 2},${by + H}`
  if (how === 'loop') {
    const x = a.x + a.w
    const r = x + 14
    return `M${x},${ay + H / 2} C${r},${ay + H / 2} ${r},${by + H / 2} ${b.x + b.w},${by + H / 2}`
  }
  if (how === 'over') {     // 从 topic 列上方的空白走过去，再落到目标左边
    const x1 = a.x + a.w
    const y1 = ay + H / 2
    const lane = TOP + 8
    const turn = b.x - 12
    return `M${x1},${y1} C${x1 + 20},${y1} ${x1 + 16},${lane} ${x1 + 36},${lane} H${turn - 8} ` +
      `Q${turn},${lane} ${turn},${lane + 8} V${by + H / 2 - 8} Q${turn},${by + H / 2} ${b.x},${by + H / 2}`
  }
  if (how === 'under') {    // 水平走到目标列前面，再拐上去
    const x1 = a.x + a.w
    const y1 = ay + H / 2
    const turn = b.x - 14
    return `M${x1},${y1} H${turn - 10} Q${turn},${y1} ${turn},${y1 - 10} V${by + H / 2 + 10} ` +
      `Q${turn},${by + H / 2} ${b.x},${by + H / 2}`
  }
  if (how === 'dashed') {   // 采集器从 pm.raw.markets 读订阅名单：从 topic 底部弯回采集器顶部
    return `M${a.x + 30},${ay + H} C${a.x + 30},${ay + H + 70} ${b.x + b.w - 30},${by - 70} ${b.x + b.w - 30},${by}`
  }
  const x1 = a.x + a.w
  const y1 = ay + H / 2
  const x2 = b.x
  const y2 = by + H / 2
  const mid = (x1 + x2) / 2
  return `M${x1},${y1} C${mid},${y1} ${mid},${y2} ${x2},${y2}`
}

const KINDS: { kind: Kind; label: string }[] = [
  { kind: 'service', label: 'Service' }, { kind: 'topic', label: 'Kafka topic' },
  { kind: 'store', label: 'Storage' }, { kind: 'external', label: 'Outside' },
]

export default function ArchitectureDiagram() {
  const [hover, setHover] = useState<string | null>(null)
  const linked = (e: (typeof EDGES)[number]) => hover != null && (e[0] === hover || e[1] === hover)
  const near = new Set(hover ? [hover, ...EDGES.filter(linked).flatMap((e) => [e[0], e[1]])] : [])

  return (
    <div className="card arch-card">
      <div className="legend arch-legend">
        {KINDS.map((k) => <span key={k.kind}><span className={`arch-key ${k.kind}`} />{k.label}</span>)}
        <span><span className="arch-key-line dashed" />Reads the market list</span>
      </div>
      <div className="arch-scroll">
        <svg viewBox={`-2 0 ${W + 4} ${HEIGHT}`} className="arch" role="img"
          aria-label="Architecture: Polymarket feeds go through collectors into Kafka, stream services compute metrics into more Kafka topics, then Redis, Postgres and a Parquet archive serve the API, the website and Grafana">
          <defs>
            <marker id="arch-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
              <path d="M0,0 L8,4 L0,8 z" className="arch-arrowhead" />
            </marker>
          </defs>
          {COLUMNS.map((c, i) => <text key={i} x={c.x} y={12} className="arch-col">{c.label}</text>)}
          {EDGES.map((e, i) => (
            <path key={i} d={path(byId[e[0]], byId[e[1]], e[2])} markerEnd="url(#arch-arrow)"
              className={`arch-edge${e[2] === 'dashed' ? ' dashed' : ''}${linked(e) ? ' on' : hover ? ' dim' : ''}`} />
          ))}
          {NODES.map((n) => (
            <g key={n.id} className={`arch-node ${n.kind}${hover && !near.has(n.id) ? ' dim' : ''}`}
              onPointerEnter={() => setHover(n.id)} onPointerLeave={() => setHover(null)}>
              <rect x={n.x} y={n.y + TOP} width={n.w} height={H} rx={8} />
              <text x={n.x + 10} y={n.y + TOP + (n.sub ? 18 : 27)} className="arch-label">{n.label}</text>
              {n.sub && <text x={n.x + 10} y={n.y + TOP + 34} className="arch-sub">{n.sub}</text>}
            </g>
          ))}
        </svg>
      </div>
      <p className="fine-print">
        Every raw event lands in Kafka unchanged; each stream service keeps its own order books in memory and works on
        exchange time, so replaying the Parquet archive reproduces the live output. Hover a box to see its connections.
      </p>
    </div>
  )
}
