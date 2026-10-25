'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import styles from './page.module.css'

const API_URL = '/api/v1'

interface Scenario {
  key: string
  version: number
  name: string
  description: string
  parameters: {
    max_events: number
    max_retries: number
    timeout_ms: number
    concurrency: number
  }
}

interface Run {
  id: string
  scenario_id: string
  status: string
  attempt_count: number
  version: number
  created_at: string
  started_at?: string
  completed_at?: string
  cancellation_reason?: string
  failure_reason?: string
}

interface Report {
  schema_version: string
  run: Run
  scenario: { key: string; name: string; description: string }
  timestamps: { created_at: string; started_at?: string; completed_at?: string; updated_at: string }
  event_attempts: Array<{
    id: string
    event_id: string
    attempt_number: number
    delivered_at: string
    acknowledged_at?: string
    fault_injected?: string
  }>
  fault_points: Array<{
    step_id: string
    event_id: string
    attempt_number: number
    fault_type: string
    delivered_at: string
  }>
  side_effect_diff: Array<{
    step_id: string
    event_id: string
    aggregate_id: string
    before?: { state_summary?: string }
    after?: { state_summary?: string }
  }>
  invariant_results: Array<{
    invariant_key: string
    rule: string
    verdict: string
    expected: string
    actual: string
    evidence: string[]
  }>
  error_details?: { failure_reason?: string; cancellation_reason?: string }
  diagnosis?: {
    summary?: string
    suggestions?: string
    model?: string
    created_at: string
    source: string
  }
}

type View = 'scenarios' | 'running' | 'report' | 'error'

function formatEncodedJson(value?: string) {
  if (!value) return null
  try {
    const binary = atob(value)
    const bytes = Uint8Array.from(binary, character => character.charCodeAt(0))
    const text = new TextDecoder().decode(bytes)
    try {
      return JSON.stringify(JSON.parse(text), null, 2)
    } catch {
      return text
    }
  } catch {
    return null
  }
}

export default function ControlPanel() {
  const [scenarios, setScenarios] = useState<Scenario[]>([])
  const [scenarioLoading, setScenarioLoading] = useState(true)
  const [selectedScenario, setSelectedScenario] = useState<string>('')
  const [apiKey, setApiKey] = useState('')
  const [apiKeyInput, setApiKeyInput] = useState('')
  const [currentRun, setCurrentRun] = useState<Run | null>(null)
  const [report, setReport] = useState<Report | null>(null)
  const [view, setView] = useState<View>('scenarios')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const stopPolling = useCallback(() => {
    if (pollRef.current) {
      clearInterval(pollRef.current)
      pollRef.current = null
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    fetch(`${API_URL}/scenarios`, {
      headers: apiKey ? { Authorization: `Bearer ${apiKey}` } : undefined,
    })
      .then(async res => {
        if (res.status === 401 || res.status === 403) {
          throw new Error(
            'Authentication required. Enter an API key with read and run scopes.',
          )
        }
        if (!res.ok) throw new Error(`Failed to load scenarios (${res.status})`)
        return res.json()
      })
      .then(data => {
        if (cancelled) return
        const list: Scenario[] = data.scenarios || []
        setScenarios(list)
        if (list.length > 0) setSelectedScenario(list[0].key)
      })
      .catch(err => {
        if (cancelled) return
        setError(err instanceof Error ? err.message : 'Failed to load scenarios')
        setView('error')
      })
      .finally(() => {
        if (!cancelled) setScenarioLoading(false)
      })
    return () => { cancelled = true }
  }, [apiKey])

  const fetchRun = useCallback(async (id: string) => {
    try {
      const res = await fetch(`${API_URL}/runs/${id}`, {
        headers: { Authorization: `Bearer ${apiKey}` },
      })
      if (res.status === 401 || res.status === 403) {
        throw new Error('Authentication required')
      }
      if (!res.ok) {
        if (res.status === 404) throw new Error('Run not found')
        throw new Error(`Failed to load run (${res.status})`)
      }
      const data = await res.json()
      setCurrentRun(data.run)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
      setView('error')
    }
  }, [apiKey])

  const fetchReport = useCallback(async (id: string) => {
    try {
      const res = await fetch(`${API_URL}/runs/${id}/report`, {
        headers: { Authorization: `Bearer ${apiKey}` },
      })
      if (res.status === 401 || res.status === 403) {
        throw new Error('Authentication required')
      }
      if (!res.ok) {
        if (res.status === 404) throw new Error('Report not found')
        throw new Error(`Failed to load report (${res.status})`)
      }
      const payload = await res.json()
      const data: Report = {
        ...payload,
        event_attempts: payload.event_attempts ?? [],
        fault_points: payload.fault_points ?? [],
        side_effect_diff: payload.side_effect_diff ?? [],
        invariant_results: payload.invariant_results ?? [],
      }
      setReport(data)
      setView('report')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
      setView('error')
    }
  }, [apiKey])

  useEffect(() => {
    if (!currentRun) return
    if (
      currentRun.status === 'completed' ||
      currentRun.status === 'failed' ||
      currentRun.status === 'cancelled' ||
      currentRun.status === 'timed_out'
    ) {
      stopPolling()
      fetchReport(currentRun.id)
      return
    }
    pollRef.current = setInterval(() => {
      fetchRun(currentRun.id)
    }, 2000)
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [currentRun, fetchReport, fetchRun, stopPolling])

  const handleRun = async (scenarioKey = selectedScenario) => {
    if (!scenarioKey) return
    setError(null)
    setReport(null)
    setLoading(true)
    try {
      const res = await fetch(`${API_URL}/runs`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({ scenario_key: scenarioKey }),
      })
      if (!res.ok) {
        if (res.status === 401 || res.status === 403) {
          throw new Error('Authentication required to create runs')
        }
        if (res.status === 404) throw new Error('Scenario not found')
        throw new Error(`Failed to create run (${res.status})`)
      }
      const data = await res.json()
      const run: Run = data.run
      setCurrentRun(run)
      setView('running')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create run')
      setView('error')
    } finally {
      setLoading(false)
    }
  }

  const handleCancel = async () => {
    if (!currentRun) return
    try {
      const res = await fetch(`${API_URL}/runs/${currentRun.id}/cancel`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${apiKey}` },
      })
      if (!res.ok) {
        if (res.status === 409) throw new Error('Run is already terminal')
        throw new Error(`Failed to cancel (${res.status})`)
      }
      const data = await res.json()
      setCurrentRun(data.run)
      setView('running')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to cancel run')
    }
  }

  const scenario = scenarios.find(s => s.key === selectedScenario)
  const timeline = report
    ? [...report.event_attempts].sort(
      (left, right) => Date.parse(left.delivered_at) - Date.parse(right.delivered_at),
    )
    : []
  return (
    <main className={styles.page}>
      {view === 'scenarios' && (
        <section className={styles.content} aria-labelledby="page-title">
          <p className={styles.brand}>GenesisGo</p>
          <h1 id="page-title">Event reliability testing</h1>
          <p className={styles.description}>
            Choose a scenario and start a controlled run to observe fault-injection
            behaviour and deterministic invariant results.
          </p>

          {scenarioLoading && <p className={styles.status} role="status">Loading scenarios...</p>}
          {!scenarioLoading && scenarios.length === 0 && (
            <p className={styles.status} role="status">No scenarios are available.</p>
          )}

          {error && (
            <div className={styles.alert}>
              <p className={styles.alertMessage}>{error}</p>
              <button
                type="button"
                className={styles.alertDismiss}
                onClick={() => {
                  setError(null)
                  setView('scenarios')
                }}
              >
                Dismiss
              </button>
            </div>
          )}

          <div className={styles.selectorCard}>
            <label className={styles.label} htmlFor="scenario-select">Scenario</label>
            <select
              id="scenario-select"
              className={styles.select}
              value={selectedScenario}
              onChange={e => setSelectedScenario(e.target.value)}
              disabled={loading || scenarioLoading || scenarios.length === 0}
            >
              {scenarios.map(s => (
                <option key={s.key} value={s.key}>
                  {s.name} (v{s.version})
                </option>
              ))}
            </select>
            {scenario && (
              <div className={styles.scenarioMeta}>
                <p className={styles.scenarioDescription}>{scenario.description}</p>
                <div className={styles.paramRow}>
                  <span className={styles.paramLabel}>Max events:</span>{' '}
                  <span className={styles.paramValue}>{scenario.parameters.max_events}</span>
                </div>
                <div className={styles.paramRow}>
                  <span className={styles.paramLabel}>Max retries:</span>{' '}
                  <span className={styles.paramValue}>{scenario.parameters.max_retries}</span>
                </div>
                <div className={styles.paramRow}>
                  <span className={styles.paramLabel}>Timeout (ms):</span>{' '}
                  <span className={styles.paramValue}>{scenario.parameters.timeout_ms}</span>
                </div>
                <div className={styles.paramRow}>
                  <span className={styles.paramLabel}>Concurrency:</span>{' '}
                  <span className={styles.paramValue}>{scenario.parameters.concurrency}</span>
                </div>
              </div>
            )}
          </div>

          <div className={styles.actions}>
            <button
              type="button"
              className={styles.runButton}
              onClick={() => handleRun()}
              disabled={loading || !selectedScenario || !scenario}
            >
              {loading ? 'Starting run...' : 'Run scenario'}
            </button>
          </div>
        </section>
      )}


      {view === 'running' && currentRun && (
        <section className={styles.content} aria-labelledby="run-title">
          <p className={styles.brand}>GenesisGo</p>
          <h1 id="run-title">Run {currentRun.id.slice(0, 8)}</h1>
          <p className={styles.description}>
            Scenario: <b>{scenario?.name ?? currentRun.scenario_id}</b>
          </p>

          <div className={styles.statusCard}>
            <span className={styles.label}>Status</span>
            <div className={styles.statusRow}>
              <span
                className={[
                  styles.statusValue,
                  currentRun.status === 'running' ? styles.statusRunning : '',
                  currentRun.status === 'completed' ? styles.statusCompleted : '',
                  currentRun.status === 'failed' ? styles.statusFailed : '',
                  currentRun.status === 'cancelled' ? styles.statusCancelled : '',
                ].join(' ')}
              >
                {currentRun.status}
              </span>
              {currentRun.status === 'running' && (
                <span className={styles.statusSpinner}>⟳</span>
              )}
            </div>
            <div className={styles.metaRow}>
              <span className={styles.metaLabel}>Attempts:</span>{' '}
              <span className={styles.metaValue}>{currentRun.attempt_count}</span>
            </div>
            {currentRun.started_at && (
              <div className={styles.metaRow}>
                <span className={styles.metaLabel}>Started:</span>{' '}
                <span className={styles.metaValue}>{currentRun.started_at}</span>
              </div>
            )}
            {currentRun.cancellation_reason && (
              <div className={styles.metaRow}>
                <span className={styles.metaLabel}>Reason:</span>{' '}
                <span className={styles.metaValue}>{currentRun.cancellation_reason}</span>
              </div>
            )}
          </div>

          <div className={styles.actions}>
            <button
              type="button"
              className={styles.cancelButton}
              onClick={handleCancel}
              disabled={
                currentRun.status === 'completed' ||
                currentRun.status === 'failed' ||
                currentRun.status === 'cancelled'
              }
            >
              Cancel run
            </button>
            <button
              type="button"
              className={styles.secondaryButton}
              onClick={() => setView('scenarios')}
            >
              Back to scenarios
            </button>
          </div>
        </section>
      )}

      {view === 'report' && report && (
        <section className={styles.content} aria-labelledby="report-title">
          <p className={styles.brand}>GenesisGo</p>
          <h1 id="report-title">Run report</h1>
          <p className={styles.description}>
            Scenario: <b>{report.scenario.name}</b> &mdash; {report.run.status}
          </p>

          <div className={styles.reportCard}>
            <section className={styles.reportSection} aria-labelledby="timeline-title">
              <h2 id="timeline-title" className={styles.sectionTitle}>Event timeline</h2>
              {timeline.length > 0 ? (
                <div className={styles.tableWrap}>
                  <table className={styles.table} aria-label="Event timeline">
                    <thead>
                      <tr>
                        <th>Delivered</th>
                        <th>Event</th>
                        <th>Attempt</th>
                        <th>Acknowledgement</th>
                        <th>Fault injection</th>
                      </tr>
                    </thead>
                    <tbody>
                      {timeline.map(attempt => {
                        const fault = report.fault_points.find(point =>
                          point.event_id === attempt.event_id &&
                          point.attempt_number === attempt.attempt_number,
                        )
                        return (
                          <tr key={attempt.id}>
                            <td><time dateTime={attempt.delivered_at}>{attempt.delivered_at}</time></td>
                            <td>{attempt.event_id}</td>
                            <td>{attempt.attempt_number}</td>
                            <td>{attempt.acknowledged_at ? 'Acknowledged' : 'Not acknowledged'}</td>
                            <td>
                              {attempt.fault_injected ?? fault?.fault_type ?? 'None'}
                              {fault && <small className={styles.faultStep}> at {fault.step_id}</small>}
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              ) : <p className={styles.emptyEvidence}>No delivery attempts were recorded.</p>}
            </section>

            <section className={styles.reportSection} aria-labelledby="effects-title">
              <h2 id="effects-title" className={styles.sectionTitle}>Before / after state</h2>
              {report.side_effect_diff.length > 0 ? (
                report.side_effect_diff.map(diff => (
                  <article className={styles.effectDiff} key={`${diff.step_id}-${diff.event_id}`}>
                    <h3>{diff.step_id} · {diff.event_id} · {diff.aggregate_id}</h3>
                    <div className={styles.diffGrid}>
                      <div>
                        <h4>Before</h4>
                        <pre>{formatEncodedJson(diff.before?.state_summary) ?? 'No before snapshot'}</pre>
                      </div>
                      <div>
                        <h4>After</h4>
                        <pre>{formatEncodedJson(diff.after?.state_summary) ?? 'No after snapshot'}</pre>
                      </div>
                    </div>
                  </article>
                ))
              ) : <p className={styles.emptyEvidence}>No state snapshots were recorded.</p>}
            </section>

            <div className={styles.reportSection}>
              <h2 className={styles.sectionTitle}>Verdicts</h2>
              <table className={styles.table}>
                <thead>
                  <tr>
                    <th>Invariant</th>
                    <th>Rule</th>
                    <th>Result</th>
                    <th>Expected</th>
                    <th>Actual</th>
                    <th>Evidence</th>
                  </tr>
                </thead>
                <tbody>
                  {report.invariant_results.map(inv => (
                    <tr key={inv.invariant_key}>
                      <td>{inv.invariant_key}</td>
                      <td>{inv.rule || '—'}</td>
                      <td className={inv.verdict === 'pass' ? styles.cellPass : inv.verdict === 'fail' ? styles.cellFail : styles.cellUnknown}>
                        {inv.verdict}
                      </td>
                      <td>{inv.expected || '—'}</td>
                      <td>{inv.actual || '—'}</td>
                      <td>{inv.evidence.length > 0 ? inv.evidence.join(', ') : '—'}</td>
                    </tr>
                  ))}
                  {report.invariant_results.length === 0 && (
                    <tr>
                      <td colSpan={6}>No invariant results</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            {report.error_details && (
              <section className={styles.reportSection} aria-labelledby="run-details-title">
                <h2 id="run-details-title" className={styles.sectionTitle}>Run details</h2>
                {report.error_details.failure_reason && <p>{report.error_details.failure_reason}</p>}
                {report.error_details.cancellation_reason && <p>{report.error_details.cancellation_reason}</p>}
              </section>
            )}

            {report.diagnosis && (
              <section className={styles.reportSection} aria-labelledby="diagnosis-title">
                <h2 id="diagnosis-title" className={styles.sectionTitle}>AI-assisted diagnosis</h2>
                <p>{report.diagnosis.summary || 'No diagnosis summary provided.'}</p>
                {report.diagnosis.model && <p>Model: {report.diagnosis.model}</p>}
                <p>Source: {report.diagnosis.source}</p>
                {formatEncodedJson(report.diagnosis.suggestions) && (
                  <pre className={styles.suggestions}>{formatEncodedJson(report.diagnosis.suggestions)}</pre>
                )}
              </section>
            )}

            <div className={styles.actions}>
              <button
                type="button"
                className={styles.secondaryButton}
                disabled={loading}
                onClick={() => handleRun(report.scenario.key)}
              >
                Run again
              </button>
              <button
                type="button"
                className={styles.secondaryButton}
                onClick={() => setView('scenarios')}
              >
                Back to scenarios
              </button>
            </div>
          </div>
        </section>
      )}

      {view === 'error' && (
        <section className={styles.content} aria-labelledby="error-title">
          <p className={styles.brand}>GenesisGo</p>
          <h1 id="error-title">Error</h1>
          <p className={styles.alertMessage}>{error}</p>
          {error?.includes('Authentication required') && (
            <form
              className={styles.keyForm}
              onSubmit={event => {
                event.preventDefault()
                setApiKey(apiKeyInput.trim())
                setApiKeyInput('')
                setError(null)
                setView('scenarios')
              }}
            >
              <label className={styles.label} htmlFor="api-key">API key</label>
              <input
                id="api-key"
                className={styles.select}
                type="password"
                autoComplete="off"
                value={apiKeyInput}
                onChange={event => setApiKeyInput(event.target.value)}
              />
              <button type="submit" className={styles.secondaryButton} disabled={!apiKeyInput.trim()}>
                Connect
              </button>
            </form>
          )}
          <div className={styles.actions}>
            <button
              type="button"
              className={styles.secondaryButton}
              onClick={() => {
                setError(null)
                setView('scenarios')
              }}
            >
              Back to scenarios
            </button>
          </div>
        </section>
      )}
    </main>
  )
}
