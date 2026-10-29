import { useState } from 'react'
import { fetchStatus, fetchStatusCharts, type StatusRange, type StatusSummary } from '../api'
import PipelineDiagram from '../components/PipelineDiagram'
import { SkeletonBlock } from '../components/Skeleton'
import TimeSeriesChart, { type Series } from '../components/TimeSeriesChart'
import UptimeBar from '../components/UptimeBar'
import { ago, clock, day, dayClock, duration, ms, rate } from '../format'
import { usePolling } from '../usePolling'

const REFRESH_MS = 15_000
const RANGES: StatusRange[] = ['1h', '24h', '7d']

const LATENCY: Series[] = [
  { key: 'network', label: 'Network', color: 'var(--s1)', stack: true },
  { key: 'processing', label: 'Processing', color: 'var(--s2)', stack: true },
  { key: 'storage', label: 'Storage', color: 'var(--s3)', stack: true },
  { key: 'p99', label: 'p99 total', color: 'var(--ink-2)', dashed: true },
]
const THROUGHPUT: Series[] = [
  { key: 'ingest', label: 'Raw events', color: 'var(--s1)' },
  { key: 'metrics', label: 'Metrics', color: 'var(--s2)' },
]
const LAG: Series[] = [
  { key: 'pm-metrics', label: 'Metrics', color: 'var(--s1)' },
  { key: 'pm-pg-sink', label: 'DB writer', color: 'var(--s2)' },
]
const COMPONENTS = [
  { key: 'collector', name: 'Exchange feed collector' },
  { key: 'live', name: 'Live metrics' },
  { key: 'archive', name: 'Parquet archive' },
] as const

const pct2 = (x: number | null) => (x == null ? '—' : `${(x * 100).toFixed(2)}%`)
// 纵轴刻度：2 s、500 ms，不带多余的小数
const msAxis = (v: number) => (v < 1000 ? `${v} ms` : `${+(v / 1000).toFixed(1)} s`)
const secs = (v: number) => (v < 10 ? `${v.toFixed(1)} s` : `${Math.round(v)} s`)
const perSec = (v: number) => v.toLocaleString('en-US', { maximumFractionDigits: 0 })

export default function Status() {
  const summary = usePolling(fetchStatus, REFRESH_MS, 'status')
  const [range, setRange] = useState<StatusRange>('24h')
  const charts = usePolling(() => fetchStatusCharts(range), REFRESH_MS * 2, range)
  const s = summary.data
  const timeLabel = range === '7d' ? day : clock
  const tipLabel = range === '7d' ? dayClock : clock
  const bucketMs = (charts.data?.bucket_seconds ?? 60) * 1000

  return (
    <>
      <section className="hero">
        <h1>Status</h1>
        <p>Live health of the pipeline behind Depthscope, from the exchange feed to this page.</p>
      </section>

      {summary.failed && !s && (
        <div className="card notice">The status service is offline right now. Please check back in a few minutes.</div>
      )}
      {!s && !summary.failed && <SkeletonBlock w="100%" h={88} />}

      {s && (
        <>
          <Banner s={s} />

          <section>
            <div className="section-head"><h2>Pipeline</h2></div>
            <PipelineDiagram s={s} />
          </section>

          <section>
            <div className="section-head"><h2>Last hour</h2></div>
            <div className="tiles">
              <div className="card tile">
                <div className="tile-label">End-to-end latency</div>
                <div className="tile-value">{ms(s.latency_1h.end_to_end_p50)}</div>
                <div className="tile-note">median · p99 {ms(s.latency_1h.end_to_end_p99)}</div>
              </div>
              <div className="card tile">
                <div className="tile-label">Throughput</div>
                <div className="tile-value">{rate(s.throughput.ingest_per_s)}</div>
                <div className="tile-note">raw events · {rate(s.throughput.metrics_per_s)} metrics written</div>
              </div>
              <div className="card tile">
                <div className="tile-label">Processing lag</div>
                <div className="tile-value">{lagText(s)}</div>
                <div className="tile-note">how far the slowest live stage is behind</div>
              </div>
              <div className="card tile">
                <div className="tile-label">Order book accuracy</div>
                <div className="tile-value">{pct2(s.quality_1h.accuracy)}</div>
                <div className="tile-note">
                  {s.quality_1h.checked.toLocaleString('en-US')} checks against the exchange's best bid/ask
                </div>
              </div>
            </div>
          </section>

          <section>
            <div className="section-head">
              <h2>Uptime <span className="muted">· last 7 days</span></h2>
              <div className="legend">
                <span><span className="status-dot ok" />Operational</span>
                <span><span className="status-dot degraded" />Degraded</span>
                <span><span className="status-dot down" />Outage</span>
                <span><span className="status-dot none" />No data</span>
              </div>
            </div>
            <div className="card uptime">
              {COMPONENTS.map((c) => (
                <div key={c.key} className="uptime-row">
                  <div className="uptime-head">
                    <span>{c.name}</span>
                    <span className="muted">{pct2(s.uptime[c.key].uptime)}</span>
                  </div>
                  <UptimeBar hours={s.uptime[c.key].hours} label={c.name} />
                </div>
              ))}
              <div className="uptime-axis muted"><span>7 days ago</span><span>Now</span></div>
            </div>
          </section>
        </>
      )}

      <section>
        <div className="section-head">
          <h2>Charts</h2>
          <div className="segmented" role="tablist" aria-label="Time range">
            {RANGES.map((r) => (
              <button key={r} role="tab" aria-selected={range === r} className={range === r ? 'active' : ''}
                onClick={() => setRange(r)}>
                {r}
              </button>
            ))}
          </div>
        </div>
        {!charts.data && <SkeletonBlock w="100%" h={260} />}
        {charts.data && (
          <div className="status-charts">
            <div className="card chart-card wide">
              <h3>Latency by stage <span className="muted">· median, stacked</span></h3>
              <TimeSeriesChart rows={charts.data.latency} series={LATENCY} format={(v) => ms(v)} axisFormat={msAxis}
                robust timeLabel={timeLabel} tipLabel={tipLabel} bucketMs={bucketMs} label="Latency by stage over time" />
            </div>
            <div className="card chart-card">
              <h3>Throughput <span className="muted">· per second</span></h3>
              <TimeSeriesChart rows={charts.data.throughput} series={THROUGHPUT} format={perSec}
                timeLabel={timeLabel} tipLabel={tipLabel} bucketMs={bucketMs} label="Messages per second over time" />
            </div>
            <div className="card chart-card">
              <h3>Processing lag <span className="muted">· seconds behind</span></h3>
              <TimeSeriesChart rows={charts.data.lag} series={LAG} format={secs}
                timeLabel={timeLabel} tipLabel={tipLabel} bucketMs={bucketMs} label="Seconds behind per consumer over time" />
            </div>
          </div>
        )}
        <p className="fine-print">
          Times are medians of per-minute values. The network hop compares the exchange's clock with ours, so it
          carries about ±0.3 s of clock error. The Parquet archive is left out of the lag chart: it writes a file every
          5 minutes, so it is always a few minutes behind by design. All times are in your local time zone.
        </p>
      </section>

      {s && (
        <section>
          <div className="section-head"><h2>Incidents <span className="muted">· longer than 1 minute</span></h2></div>
          <div className="card incidents">
            {s.incidents.length === 0 && <div className="empty">No incidents in the last 7 days.</div>}
            {s.incidents.map((i) => (
              <div key={i.start} className="incident">
                <div className="inc-when">
                  <b>{day(i.start)}</b>
                  <span className="muted">{clock(i.start)} – {clock(i.end)}</span>
                </div>
                <div className="inc-what">
                  {i.title}
                  {i.count > 1 && <span className="muted"> · {i.count} outages</span>}
                </div>
                <div className="inc-dur">{duration(i.down_seconds)}</div>
              </div>
            ))}
          </div>
        </section>
      )}
    </>
  )
}

function lagText(s: StatusSummary) {
  const live = [s.lag_seconds['pm-metrics'], s.lag_seconds['pm-pg-sink']]
  if (live.some((v) => v === null)) return '—'
  const worst = Math.max(...live.filter((v): v is number => v != null))
  return Number.isFinite(worst) ? secs(worst) : '—'
}

function Banner({ s }: { s: StatusSummary }) {
  const icon = { ok: '✓', degraded: '!', down: '✕' }[s.level]
  return (
    <div className={`card status-banner ${s.level}`} role="status">
      <span className="banner-icon" aria-hidden="true">{icon}</span>
      <div className="banner-text">
        <div className="banner-title">{s.title}</div>
        <div className="muted">
          {s.reasons.length > 0
            ? s.reasons.join(' ')
            : `Median end-to-end latency ${ms(s.latency_24h.end_to_end_p50)} over the last 24 hours.`}
        </div>
      </div>
      <span className="muted banner-updated">Updated {ago(s.generated_at)}</span>
    </div>
  )
}
