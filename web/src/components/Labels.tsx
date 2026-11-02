// 市场的话题和公司标签（后端表 market_labels）。话题是 Other 的不显示
export default function Labels({ topic, company }: { topic: string | null; company: string | null }) {
  return (
    <>
      {topic && topic !== 'Other' && <span className="tag">{topic}</span>}
      {company && <span className="tag company">{company}</span>}
    </>
  )
}
