import { Link } from 'react-router'

// About 页：只讲网站是干什么的、能看什么。管道和架构放在 Status 页。全是静态内容，不调接口。

// 只有自己有页面的才做成链接；深度图、回放、成交、深度质量在市场详情页里
const FEATURES: { name: string; text: string; to?: string }[] = [
  {
    to: '/',
    name: 'Markets',
    text: 'The most active tech and AI markets, with live prices and how far each has moved in the last hour.',
  },
  {
    name: 'Depth chart and slippage',
    text: 'The full order book, and what an order of any size would actually pay.',
  },
  {
    name: 'Replay',
    text: 'Play back any recorded window, every order book change and trade, to the millisecond.',
  },
  {
    name: 'Trades',
    text: 'How much each trade paid to go now, and whether the price kept moving its way afterwards.',
  },
  {
    name: 'Depth quality',
    text: 'Large orders near the best price that keep appearing and getting pulled before they trade.',
  },
  {
    to: '/signals',
    name: 'Signals',
    text: 'Sudden price moves, with the trades behind them and where the price went next.',
  },
  {
    to: '/status',
    name: 'Status',
    text: 'Live health of the data pipeline, and how it is built.',
  },
]

export default function About() {
  return (
    <>
      <section className="hero">
        <h1>About</h1>
        <p>
          Depthscope watches the order books of Polymarket's most active tech and AI markets in real time. It shows
          how deep a market really is, who paid what to trade, which depth keeps vanishing, and lets you replay big
          price moves tick by tick.
        </p>
      </section>

      <section>
        <div className="about-grid">
          {FEATURES.map((f) => {
            const body = <><h3>{f.name}{f.to && <span aria-hidden="true"> →</span>}</h3><p>{f.text}</p></>
            return f.to
              ? <Link key={f.name} to={f.to} className="card about-feature link">{body}</Link>
              : <div key={f.name} className="card about-feature">{body}</div>
          })}
        </div>
      </section>

      <section>
        <div className="card about-text">
          <p>
            Depthscope is an independent project, not affiliated with Polymarket. It doesn't trade, take payment or
            have accounts, and nothing here is investment advice. The source code is on{' '}
            <a href="https://github.com/xuh5/polymarket-Fuzzy-Cognitive-Map" target="_blank" rel="noreferrer">GitHub</a>.
          </p>
        </div>
      </section>
    </>
  )
}
