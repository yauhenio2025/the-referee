export default function Stats({ stats }) {
  if (!stats) return null

  return (
    <div className="stats">
      <div className="stat">
        <span className="stat-value">{stats.papers || 0}</span>
        <span className="stat-label">Papers</span>
      </div>
      <div className="stat">
        <span className="stat-value">{stats.editions || 0}</span>
        <span className="stat-label">Editions</span>
      </div>
      <div className="stat">
        <span className="stat-value">{stats.citations || 0}</span>
        <span className="stat-label">Citations</span>
      </div>
    </div>
  )
}
