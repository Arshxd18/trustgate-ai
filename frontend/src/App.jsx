import { useEffect, useState } from 'react'
import './App.css'

const demoTraffic = {
  src_port: 443,
  dst_port: 55555,
  proto: 'tcp',
  service: 'ssl',
  duration: 1.2,
  orig_bytes: 500,
  resp_bytes: 1200,
  conn_state: 'SF',
  local_orig: 'unknown',
  local_resp: 'unknown',
  missed_bytes: 0,
  history: 'unknown',
  orig_pkts: 5,
  orig_ip_bytes: 780,
  resp_pkts: 6,
  resp_ip_bytes: 1450,
  event_hour: 12,
  event_day_of_week: 2,
}

function score(value) {
  return Number(value ?? 0).toFixed(1)
}

function Metric({ label, value, note, tone = '' }) {
  return (
    <div className="metric">
      <span className="muted">{label}</span>
      <strong className={tone}>{value}</strong>
      <small>{note}</small>
    </div>
  )
}

function App() {
  const [page, setPage] = useState('Overview')
  const [health, setHealth] = useState(null)
  const [analysis, setAnalysis] = useState(null)
  const [audit, setAudit] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [incidentId, setIncidentId] = useState('TG-INC-001')
  const [operator, setOperator] = useState('Security Analyst')
  const [reason, setReason] = useState('')
  const [traffic, setTraffic] = useState(demoTraffic)
  const [financial, setFinancial] = useState({
    historical_accuracy: 75,
    category_accuracy: 70,
    operator_acceptance: 80,
    data_quality: 90,
    model_freshness: 85,
    feedback_sample_size: 5,
    average_downtime_minutes: 60,
    cost_per_minute: 1000,
    sla_penalty: 10000,
    false_positives: 0,
    operator_overrides: 0,
    total_alerts: 0,
  })

  async function api(path, options = {}) {
    const response = await fetch(`/api${path}`, {
      ...options,
      headers: { 'Content-Type': 'application/json', ...options.headers },
    })
    const data = await response.json()
    if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`)
    return data
  }

  async function refresh() {
    try {
      const status = await api('/health')
      setHealth(status)
    } catch {
      setHealth({ status: 'unreachable', model_loaded: false })
    }
    try {
      const data = await api('/audit?limit=50')
      setAudit(data.events || [])
    } catch {
      // Audit history may be unavailable until the API is restarted.
    }
  }

  useEffect(() => {
    refresh()
    const timer = setInterval(refresh, 15000)
    return () => clearInterval(timer)
  }, [])

  async function analyze(event) {
    event?.preventDefault()
    setLoading(true)
    setError('')
    try {
      const payload = {
        ...traffic,
        ...financial,
        incident_id: incidentId,
      }
      const result = await api('/analyze', {
        method: 'POST',
        body: JSON.stringify(payload),
      })
      setAnalysis(result)
      setPage('Incident Analysis')
    } catch (e) {
      setError(`Analysis failed: ${e.message}. Check the API log and request schema.`)
    } finally {
      setLoading(false)
      refresh()
    }
  }

  async function review(decision) {
    setError('')
    try {
      const result = await api('/review', {
        method: 'POST',
        body: JSON.stringify({
          incident_id: analysis?.incident_id || incidentId,
          decision,
          operator,
          reason: decision === 'reject' ? reason : '',
          trust_score: analysis?.trust?.trust_score ?? null,
          risk_level: analysis?.risk?.risk_level ?? null,
        }),
      })
      setReason('')
      setError(result.message || `Review recorded: ${decision}`)
      refresh()
    } catch (e) {
      setError(`Could not record review: ${e.message}`)
    }
  }

  const trust = analysis?.trust
  const risk = analysis?.risk
  const debt = analysis?.trust_debt
  const trustScore = Number(trust?.trust_score ?? 0)
  const riskValue = Number(risk?.expected_loss_if_ignored ?? risk?.expected_loss ?? risk?.expected_loss_inr ?? 0)

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-icon">T</div>
          <div><b>TrustGate</b><span>AI GOVERNANCE PLATFORM</span></div>
        </div>
        <div className="nav-label">WORKSPACE</div>
        {[
          ['Overview', '◫'],
          ['Incident Analysis', '⌁'],
          ['Audit History', '▤'],
        ].map(([name, icon]) => (
          <button key={name} className={`nav-item ${page === name ? 'active' : ''}`}
            onClick={() => setPage(name)}>
            <span>{icon}</span>{name}
          </button>
        ))}
        <div className="sidebar-bottom">
          <div className="status-dot" />
          <div><b>Trust engine</b><span>{health?.model_loaded ? 'Model connected' : 'Checking connection'}</span></div>
        </div>
      </aside>

      <main className="main">
        <header className="topbar">
          <div><span className="eyebrow">AI OPERATIONS / GOVERNANCE</span><h1>{page}</h1></div>
          <div className="top-actions">
            <span className={`pill ${health?.model_loaded ? 'good' : 'warning'}`}>
              <i />{health?.model_loaded ? 'MODEL ONLINE' : 'API CHECK'}
            </span>
            <button className="button secondary" onClick={refresh}>↻ Refresh</button>
          </div>
        </header>

        {error && <div className="notice" role="status">{error}<button onClick={() => setError('')}>×</button></div>}

        {page === 'Overview' && (
          <>
            <section className="welcome panel">
              <div>
                <span className="eyebrow">TRUST BEFORE ACTION</span>
                <h2>Intelligence is a signal.<br /><em>Trust must be earned.</em></h2>
                <p>Evaluate AI predictions against operational trust, financial exposure and human oversight before taking action.</p>
                <button className="button primary" onClick={() => setPage('Incident Analysis')}>Open incident workspace <span>↗</span></button>
              </div>
              <div className="orbit">
                <div className="orbit-ring ring-one" /><div className="orbit-ring ring-two" />
                <div className="orbit-core">TG<span>TRUST<br/>ENGINE</span></div>
                <span className="orbit-node node-one">AI</span><span className="orbit-node node-two">✓</span>
              </div>
            </section>

            <div className="section-heading"><div><h3>Trust overview</h3><p>Latest analysis from this session</p></div><span className="pill neutral">DEMO ENVIRONMENT</span></div>
            <section className="metric-grid">
              <Metric label="Trust score" value={trust ? `${score(trust.trust_score)} / 100` : '—'} note={trust?.trust_level || 'Run an analysis'} tone="violet" />
              <Metric label="Expected loss" value={risk ? `₹${Math.round(riskValue).toLocaleString('en-IN')}` : '—'} note={risk?.risk_level ? `${risk.risk_level} financial risk` : 'Awaiting incident data'} tone="orange" />
              <Metric label="Trust debt" value={debt ? `${score(debt.trust_debt_score ?? debt.debt_score ?? debt.score)} / 100` : '—'} note={debt?.trust_debt_level || debt?.debt_level || debt?.level || 'Requires feedback history'} tone="pink" />
              <Metric label="Human oversight" value="Required" note="Automatic actions disabled" tone="green" />
            </section>

            <div className="content-grid">
              <section className="panel">
                <div className="panel-title"><div><h3>Latest model analysis</h3><p>Detection output and governance status</p></div><span className="live-mark">● LIVE API</span></div>
                {analysis ? (
                  <div className="result-block">
                    <div className="result-row"><span>Incident</span><b>{analysis.incident_id}</b></div>
                    <div className="result-row"><span>Model prediction</span><b>{analysis.prediction}</b></div>
                    <div className="result-row"><span>Trust level</span><b>{trust?.trust_level || 'N/A'}</b></div>
                    <div className="result-row"><span>Recommendation</span><b>{analysis.recommendation || 'Human review'}</b></div>
                    <button className="button secondary full" onClick={() => setPage('Incident Analysis')}>View full analysis →</button>
                  </div>
                ) : (
                  <div className="empty-state"><div className="empty-icon">⌁</div><b>No incident analyzed yet</b><p>Run the detection and trust pipeline to see the first result here.</p><button className="button secondary" onClick={() => setPage('Incident Analysis')}>Analyze an incident</button></div>
                )}
              </section>
              <section className="panel">
                <div className="panel-title"><div><h3>Governance guardrails</h3><p>Always-on safety controls</p></div></div>
                {[
                  ['Human-in-the-loop', 'Approval remains with the operator', 'green'],
                  ['Explainable trust', 'Weighted signals and visible evidence', 'violet'],
                  ['Audit trail', 'Review decisions are recorded', 'green'],
                  ['No auto-execution', 'Predictions never trigger actions', 'orange'],
                ].map(([title, desc, tone]) => <div className="guardrail" key={title}><span className={`guard-icon ${tone}`}>✓</span><div><b>{title}</b><p>{desc}</p></div><span className="guard-state">ON</span></div>)}
              </section>
            </div>
          </>
        )}

        {page === 'Incident Analysis' && (
          <>
            <section className="panel form-panel">
              <div className="panel-title"><div><h3>Analyze network incident</h3><p>Submit traffic features to the trained IoT-23 detection model and TrustGate engines.</p></div><span className="pill neutral">RANDOM FOREST</span></div>
              <form onSubmit={analyze}>
                <label>Incident ID<input value={incidentId} onChange={e => setIncidentId(e.target.value)} required /></label>
                <div className="form-section-title">Network features</div>
                <div className="form-grid">
                  {[
                    ['src_port', 'Source port'], ['dst_port', 'Destination port'],
                    ['proto', 'Protocol'], ['service', 'Service'],
                    ['duration', 'Duration (seconds)'], ['orig_bytes', 'Origin bytes'],
                    ['resp_bytes', 'Response bytes'], ['conn_state', 'Connection state'],
                    ['orig_pkts', 'Origin packets'], ['resp_pkts', 'Response packets'],
                    ['orig_ip_bytes', 'Origin IP bytes'], ['resp_ip_bytes', 'Response IP bytes'],
                    ['missed_bytes', 'Missed bytes'], ['event_hour', 'Event hour (0–23)'],
                    ['event_day_of_week', 'Day of week (0–6)'],
                    ['history', 'Connection history'],
                  ].map(([key, label]) => (
                    <label key={key}>{label}
                      <input type={['proto','service','conn_state','history'].includes(key) ? 'text' : 'number'}
                        step={['duration'].includes(key) ? 'any' : '1'}
                        value={traffic[key] ?? ''}
                        onChange={e => setTraffic(prev => ({ ...prev, [key]: ['proto','service','conn_state','history'].includes(key) ? e.target.value : Number(e.target.value) }))}
                        required />
                    </label>
                  ))}
                </div>
                <div className="form-section-title">Trust and risk assumptions</div>
                <div className="form-grid">
                  {[
                    ['historical_accuracy', 'Historical accuracy (%)'],
                    ['category_accuracy', 'Category accuracy (%)'],
                    ['operator_acceptance', 'Operator acceptance (%)'],
                    ['data_quality', 'Data quality (%)'],
                    ['model_freshness', 'Model freshness (%)'],
                    ['feedback_sample_size', 'Feedback sample size'],
                    ['average_downtime_minutes', 'Average downtime (minutes)'],
                    ['cost_per_minute', 'Downtime cost per minute (₹)'],
                    ['sla_penalty', 'SLA penalty (₹)'],
                    ['false_positives', 'Historical false positives'],
                    ['operator_overrides', 'Operator overrides'],
                    ['total_alerts', 'Total historical alerts'],
                  ].map(([key, label]) => (
                    <label key={key}>{label}
                      <input type="number" min="0" max={key.endsWith('_accuracy') || key.endsWith('_acceptance') || key === 'data_quality' || key === 'model_freshness' ? 100 : undefined}
                        value={financial[key]}
                        onChange={e => setFinancial(prev => ({ ...prev, [key]: Number(e.target.value) }))}
                        required />
                    </label>
                  ))}
                </div>
                <p className="form-hint">Demo inputs are illustrative. Replace them with validated operational metrics before making real-world decisions.</p>
                <button className="button primary" type="submit" disabled={loading}>{loading ? 'Analyzing…' : 'Run TrustGate analysis →'}</button>
              </form>
            </section>

            {analysis && (
              <>
                <div className="section-heading"><div><h3>Analysis results</h3><p>Incident {analysis.incident_id}</p></div><span className="pill warning">HUMAN REVIEW</span></div>
                <section className="metric-grid">
                  <Metric label="Prediction" value={analysis.prediction || 'N/A'} note={`Malicious probability ${score((analysis.prediction_details?.malicious_probability ?? analysis.malicious_probability ?? 0) * 100)}%`} tone="violet" />
                  <Metric label="Trust score" value={`${score(trust?.trust_score)} / 100`} note={trust?.trust_level || 'Unavailable'} tone="violet" />
                  <Metric label="Expected loss" value={`₹${Math.round(riskValue).toLocaleString('en-IN')}`} note={risk?.risk_level || 'Estimate'} tone="orange" />
                  <Metric label="Trust debt" value={debt ? `${score(debt.trust_debt_score ?? debt.debt_score ?? debt.score)} / 100` : 'N/A'} note={debt?.trust_debt_level || debt?.debt_level || debt?.level || 'No history'} tone="pink" />
                </section>
                <div className="content-grid">
                  <section className="panel">
                    <div className="panel-title"><div><h3>Trust signal breakdown</h3><p>Inputs used by the trust engine</p></div></div>
                    {Object.entries(trust?.signals || {}).map(([key, value]) => (
                      <div className="signal" key={key}><div><span>{key.replaceAll('_', ' ')}</span><b>{score(value)}%</b></div><div className="bar"><span style={{ width: `${Math.max(0, Math.min(100, Number(value) || 0))}%` }} /></div></div>
                    ))}
                    <p className="form-hint">Trust is distinct from model confidence. Review the individual signals before acting.</p>
                  </section>
                  <section className="panel">
                    <div className="panel-title"><div><h3>Human review</h3><p>Record an auditable decision</p></div></div>
                    <label>Operator<input value={operator} onChange={e => setOperator(e.target.value)} required /></label>
                    <label>Rejection reason<textarea value={reason} onChange={e => setReason(e.target.value)} placeholder="Explain why the alert is rejected or needs further investigation." rows="3" /></label>
                    <div className="review-actions">
                      <button type="button" className="button primary" onClick={() => review('approve')}>✓ Approve</button>
                      <button type="button" className="button danger" onClick={() => reason.trim() ? review('reject') : setError('Enter a rejection reason before rejecting this incident.')}>✕ Reject</button>
                    </div>
                    <p className="form-hint">This records a review decision only. It does not execute an operational action.</p>
                  </section>
                </div>
              </>
            )}
          </>
        )}

        {page === 'Audit History' && (
          <section className="panel">
            <div className="panel-title"><div><h3>Decision audit log</h3><p>Recorded human-review events from the backend</p></div><button className="button secondary" onClick={refresh}>↻ Refresh</button></div>
            {audit.length === 0 ? <div className="empty-state"><div className="empty-icon">▤</div><b>No audit events found</b><p>Analyze an incident and record a review to populate the audit trail.</p></div> : (
              <div className="table-wrap"><table><thead><tr><th>Time</th><th>Incident</th><th>Decision</th><th>Operator</th><th>Reason</th></tr></thead><tbody>
                {audit.map((event, i) => <tr key={event.event_id || i}>
                  <td>{event.timestamp ? new Date(event.timestamp).toLocaleString() : '—'}</td>
                  <td>{event.incident_id || '—'}</td>
                  <td><span className={`pill ${event.decision === 'approve' ? 'good' : 'warning'}`}>{event.decision || '—'}</span></td>
                  <td>{event.operator || '—'}</td><td>{event.reason || '—'}</td>
                </tr>)}
              </tbody></table></div>
            )}
          </section>
        )}

        <footer><span>TRUSTGATE <b>·</b> AI GOVERNANCE LAYER</span><span>Human oversight first <b>·</b> Demo environment</span></footer>
      </main>
    </div>
  )
}

export default App
