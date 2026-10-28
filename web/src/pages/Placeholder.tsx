// 还没做的页面先放一个占位，导航能点、页面结构先定下来
export default function Placeholder({ title, description }: { title: string; description: string }) {
  return (
    <section className="hero">
      <h1>{title}</h1>
      <p>{description}</p>
      <div className="card notice placeholder">Coming soon.</div>
    </section>
  )
}
