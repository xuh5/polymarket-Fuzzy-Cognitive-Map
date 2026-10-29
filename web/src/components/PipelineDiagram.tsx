import { LEVEL_TEXT, type HealthLevel, type StatusSummary } from '../api'
import { ms, rate } from '../format'

// 管道示意图：交易所 → 采集器 → Kafka → 指标计算 → 数据库，每个环节一个状态灯，箭头上标这一段的延迟（过去 1 小时中位数）
export default function PipelineDiagram({ s }: { s: StatusSummary }) {
  const lat = s.latency_1h
  const behind = s.lag_seconds['pm-metrics']
  // 交易所 → 收到这一段受本机时钟误差影响（约 ±0.3 秒），算出来可能是负数，只说「不到 0.1 秒」
  const network = lat.exch_recv_p50 == null ? '—' : lat.exch_recv_p50 < 100 ? '< 100 ms' : ms(lat.exch_recv_p50)
  const nodes: { name: string; sub: string; level: HealthLevel | null }[] = [
    { name: 'Polymarket', sub: 'WebSocket feed', level: null },
    { name: 'Collector', sub: rate(s.throughput.ingest_per_s), level: s.stages.collector },
    { name: 'Kafka', sub: 'Raw event log', level: s.stages.kafka },
    { name: 'Metrics engine', sub: behind == null ? '—' : `${behind.toFixed(1)} s behind`, level: s.stages.metrics },
    { name: 'Database', sub: s.live_age_seconds == null ? '—' : `updated ${Math.round(s.live_age_seconds)} s ago`,
      level: s.stages.database },
  ]
  const hops = [network, '', ms(lat.recv_comp_p50), ms(lat.comp_ins_p50)]
  return (
    <div className="card pipeline">
      <div className="pipe-row">
        {nodes.map((n, i) => (
          <div key={n.name} className="pipe-step">
            {i > 0 && <div className="pipe-hop"><span>{hops[i - 1]}</span></div>}
            <div className="pipe-node">
              <div className="pipe-name">
                {n.level && <span className={`status-dot ${n.level}`} title={LEVEL_TEXT[n.level]} />}
                {n.name}
              </div>
              <div className="pipe-sub">{n.sub}</div>
            </div>
          </div>
        ))}
      </div>
      <div className="pipe-archive">
        <span className={`status-dot ${s.stages.archive}`} title={LEVEL_TEXT[s.stages.archive]} />
        Kafka also feeds the <b>Parquet archive</b> (flushed every 5 minutes)
      </div>
    </div>
  )
}
