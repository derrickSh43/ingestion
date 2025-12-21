import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { useMutation, useQuery } from '@tanstack/react-query'

import { HeaderBar } from '../components/HeaderBar'
import { useAuth } from '../auth/useAuth'
import {
  getIngestionEvents,
  getIngestionMetrics,
  ingestionEventsQueryKey,
  ingestionMetricsQueryKey,
  postIngestionRawCapture,
  postIngestionRun,
  postQuarantineCapture,
  type IngestionRawCaptureResponse,
  type IngestionRunResponse,
} from '../api/ingestion'
import { postRetrievalQuery, type RetrievalQueryRequest, type RetrievalQueryResponse } from '../api/retrieval'
import { getReleaseAudit, getReleaseStatus, postReleasePromote, releaseAuditQueryKey, releaseStatusQueryKey } from '../api/releases'

export function IngestionPage() {
  const { displayName } = useAuth()
  const [selectedDomain, setSelectedDomain] = useState<string>('')
  const domain = useMemo(() => selectedDomain.trim(), [selectedDomain])

  const [retrievalQuery, setRetrievalQuery] = useState<string>('')
  const [retrievalTopK, setRetrievalTopK] = useState<number>(5)
  const [retrievalFiltersJson, setRetrievalFiltersJson] = useState<string>('')
  const [retrievalFiltersError, setRetrievalFiltersError] = useState<string | null>(null)

  const [runnerSourceId, setRunnerSourceId] = useState<string>(() => `src_${Date.now()}`)
  const [runnerReleaseId, setRunnerReleaseId] = useState<string>('')
  const [runnerUrl, setRunnerUrl] = useState<string>('')
  const [runnerQuarantineOnFail, setRunnerQuarantineOnFail] = useState<boolean>(true)
  const [runnerRawHtmlPath, setRunnerRawHtmlPath] = useState<string>('')
  const [runnerRawHtml, setRunnerRawHtml] = useState<string>('')
  const [runnerError, setRunnerError] = useState<string | null>(null)

  const firstLine = (value: unknown): string => {
    if (typeof value === 'string' && value.trim()) return value.trim().split('\n')[0]
    if (value && typeof value === 'object') {
      const maybeMessage = (value as any)?.message
      if (typeof maybeMessage === 'string' && maybeMessage.trim()) return maybeMessage.trim().split('\n')[0]
      const maybeDetail = (value as any)?.detail
      if (typeof maybeDetail === 'string' && maybeDetail.trim()) return maybeDetail.trim().split('\n')[0]
    }
    return 'Request failed.'
  }

  const errorDetails = (err: unknown) => {
    const responseData = (err as any)?.response?.data
    if (responseData !== undefined) return responseData
    const details = (err as any)?.details
    if (details !== undefined) return details
    return err
  }

  const retrievalMutation = useMutation({
    mutationFn: (req: RetrievalQueryRequest) => postRetrievalQuery(req),
  })

  const rawCaptureMutation = useMutation({
    mutationFn: (args: { domain: string; url: string; sourceId: string }) =>
      postIngestionRawCapture({
        source_id: args.sourceId,
        domain: args.domain,
        url: args.url,
        quarantine_suspicious: runnerQuarantineOnFail,
      }),
    onSuccess: () => {
      void eventsQuery.refetch()
      void metricsQuery.refetch()
    },
  })

  const quarantineCaptureMutation = useMutation({
    mutationFn: async (args: { domain: string; capture_id: string; reason?: string | null }) => {
      return await postQuarantineCapture({
        domain: args.domain,
        capture_id: args.capture_id,
        reason: args.reason ?? null,
      })
    },
    onError: (err) => {
      setRunnerError(firstLine(err))
    },
  })

  const ingestionRunMutation = useMutation({
    mutationFn: (req: {
      domain: string
      source_id: string
      release_id: string
      raw_html?: string | null
      raw_html_path?: string | null
      capture_id?: string | null
    }) =>
      postIngestionRun({
        domain: req.domain,
        source_id: req.source_id,
        release_id: req.release_id,
        raw_html: req.raw_html ?? null,
        raw_html_path: req.raw_html_path ?? null,
        capture_id: req.capture_id ?? null,
        created_by: displayName ?? null,
      }),
    onSuccess: () => {
      void eventsQuery.refetch()
      void metricsQuery.refetch()
      void auditQuery.refetch()
    },
  })

  const releasePromoteMutation = useMutation({
    mutationFn: (args: { domain: string; releaseId: string; reason?: string }) =>
      postReleasePromote(args.domain, args.releaseId, { reason: args.reason ?? null, promoted_by: displayName ?? null }),
  })

  const releaseStatusQuery = useQuery({
    queryKey: releaseStatusQueryKey(domain),
    queryFn: () => getReleaseStatus(domain),
    enabled: Boolean(domain),
  })

  const metricsQuery = useQuery({
    queryKey: ingestionMetricsQueryKey(domain, 24),
    queryFn: () => getIngestionMetrics(domain, 24),
    enabled: Boolean(domain),
  })

  const eventsQuery = useQuery({
    queryKey: ingestionEventsQueryKey(domain, 100),
    queryFn: () => getIngestionEvents(domain, 100),
    enabled: Boolean(domain),
  })

  const auditQuery = useQuery({
    queryKey: releaseAuditQueryKey(domain, 50),
    queryFn: () => getReleaseAudit(domain, 50),
    enabled: Boolean(domain),
  })

  const retrievalResults = useMemo(() => {
    const data = retrievalMutation.data as RetrievalQueryResponse | undefined
    const results = data?.results
    return Array.isArray(results) ? results : []
  }, [retrievalMutation.data])

  const retrievalWarnings = useMemo(() => {
    const data = retrievalMutation.data as RetrievalQueryResponse | undefined
    const warnings = data?.warnings
    return Array.isArray(warnings) ? warnings : []
  }, [retrievalMutation.data])

  const activeReleaseId = useMemo(() => {
    const data = releaseStatusQuery.data
    const rid = data?.active_release
    return typeof rid === 'string' && rid.trim() ? rid : null
  }, [releaseStatusQuery.data])

  const candidateReleaseId = useMemo(() => {
    const data = ingestionRunMutation.data as IngestionRunResponse | undefined
    const rid = data?.release_id
    return typeof rid === 'string' && rid.trim() ? rid : null
  }, [ingestionRunMutation.data])

  const rawCaptureResult = useMemo(() => {
    return rawCaptureMutation.data as IngestionRawCaptureResponse | undefined
  }, [rawCaptureMutation.data])

  const capture = useMemo(() => {
    const r: any = rawCaptureResult
    return (r?.capture?.capture ?? r?.capture ?? r) as any
  }, [rawCaptureResult])

  const countsByEvent = useMemo(() => {
    const raw = (metricsQuery.data as any)?.counts_by_event
    if (!raw || typeof raw !== 'object') return [] as Array<{ key: string; count: number }>
    return Object.entries(raw as Record<string, unknown>)
      .map(([key, value]) => ({ key, count: typeof value === 'number' ? value : Number(value) }))
      .filter((row) => row.key && Number.isFinite(row.count))
      .sort((a, b) => b.count - a.count)
      .slice(0, 8)
  }, [metricsQuery.data])

  const countsByStatus = useMemo(() => {
    const raw = (metricsQuery.data as any)?.counts_by_status
    if (!raw || typeof raw !== 'object') return [] as Array<{ key: string; count: number }>
    return Object.entries(raw as Record<string, unknown>)
      .map(([key, value]) => ({ key, count: typeof value === 'number' ? value : Number(value) }))
      .filter((row) => row.key && Number.isFinite(row.count))
      .sort((a, b) => b.count - a.count)
  }, [metricsQuery.data])

  const allEvents = useMemo(() => {
    const events = (eventsQuery.data as any)?.events
    return Array.isArray(events) ? (events as any[]) : []
  }, [eventsQuery.data])

  const auditEvents = useMemo(() => {
    const events = (auditQuery.data as any)?.events
    return Array.isArray(events) ? (events as any[]) : []
  }, [auditQuery.data])

  const lastEventTimestamp = useMemo(() => {
    if (allEvents.length === 0) return null
    const ts = (allEvents[0] as any)?.timestamp
    return typeof ts === 'string' && ts.trim() ? ts : null
  }, [allEvents])

  const runRetrieval = () => {
    if (!domain) return

    const q = retrievalQuery.trim()
    if (!q) {
      setRetrievalFiltersError('Query is required.')
      return
    }

    let filters: Record<string, unknown> | null | undefined = null
    const rawFilters = retrievalFiltersJson.trim()
    if (rawFilters) {
      try {
        const parsed = JSON.parse(rawFilters)
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          filters = parsed as Record<string, unknown>
        } else {
          setRetrievalFiltersError('Filters must be a JSON object.')
          return
        }
      } catch {
        setRetrievalFiltersError('Filters JSON is invalid.')
        return
      }
    }

    setRetrievalFiltersError(null)
    retrievalMutation.mutate({
      domain,
      query: q,
      top_k: Number.isFinite(retrievalTopK) ? Math.max(1, Math.min(50, Math.floor(retrievalTopK))) : 5,
      filters: filters ?? null,
      release_id: activeReleaseId ?? candidateReleaseId ?? null,
    })
  }

  const getResultScore = (r: Record<string, unknown>): string => {
    const score = (r as any)?.score
    if (typeof score === 'number' && Number.isFinite(score)) return score.toFixed(4)
    if (typeof score === 'string' && score.trim()) return score
    return '--'
  }

  const getResultChunkId = (r: Record<string, unknown>): string => {
    const chunkId = (r as any)?.chunk_id ?? (r as any)?.id
    return typeof chunkId === 'string' && chunkId.trim() ? chunkId : '--'
  }

  const getResultExcerpt = (r: Record<string, unknown>): string => {
    const candidates = [(r as any)?.chunk_text, (r as any)?.text, (r as any)?.excerpt, (r as any)?.content]
    for (const c of candidates) {
      if (typeof c === 'string' && c.trim()) return c.slice(0, 300)
    }
    const meta = (r as any)?.metadata
    if (typeof meta === 'string' && meta.trim()) return meta.slice(0, 300)
    return ''
  }

  const runCapture = async () => {
    if (!domain) return
    const url = runnerUrl.trim()
    if (!url) {
      setRunnerError('URL is required.')
      return
    }
    if (!runnerSourceId.trim()) {
      setRunnerError('Source id is required.')
      return
    }
    setRunnerError(null)
    try {
      await rawCaptureMutation.mutateAsync({ domain, url, sourceId: runnerSourceId.trim() })
    } catch (e) {
      setRunnerError(firstLine(e))
    }
  }

  const runIngestion = async () => {
    if (!domain) return
    if (!runnerSourceId.trim()) {
      setRunnerError('Source id is required.')
      return
    }
    if (!runnerReleaseId.trim()) {
      setRunnerError('Release id is required.')
      return
    }
    const hasInline = runnerRawHtml.trim().length > 0
    const hasPath = runnerRawHtmlPath.trim().length > 0
    const hasCapture = Boolean(capture?.source_id)
    if (!hasInline && !hasPath && !hasCapture) {
      setRunnerError('Provide raw HTML, a raw HTML path, or capture a URL first.')
      return
    }
    setRunnerError(null)
    try {
      await ingestionRunMutation.mutateAsync({
        domain,
        source_id: runnerSourceId.trim(),
        release_id: runnerReleaseId.trim(),
        raw_html: hasInline ? runnerRawHtml.trim() : null,
        raw_html_path: !hasInline && hasPath ? runnerRawHtmlPath.trim() : null,
        capture_id: !hasInline && !hasPath && hasCapture ? capture.source_id : null,
      })
      void releaseStatusQuery.refetch()
    } catch (e) {
      setRunnerError(firstLine(e))
    }
  }

  const counts = (ingestionRunMutation.data as IngestionRunResponse | undefined)?.counts ?? {}

  return (
    <div className="min-h-screen bg-slate-100 p-6">
      <div className="mx-auto max-w-6xl">
        <HeaderBar name={displayName} subtitle="Admin Ingestion" />

        <div className="mt-6 rounded-xl border border-slate-200 bg-white p-6">
          <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
            <div className="w-full sm:max-w-md">
              <div className="text-xs font-semibold text-slate-500">Domain</div>
              <input
                value={selectedDomain}
                onChange={(e) => setSelectedDomain(e.target.value)}
                placeholder="terraform"
                className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400"
                aria-label="Domain"
              />
              <div className="mt-2 text-xs text-slate-500">
                {domain ? `Selected domain: ${domain}` : 'Select a domain to scope ingestion + retrieval.'}
              </div>
              {domain && activeReleaseId ? <div className="mt-1 text-xs text-slate-500">Active release: {activeReleaseId}</div> : null}
              {domain && candidateReleaseId ? (
                <div className="mt-1 text-xs text-slate-500">Candidate release (last run): {candidateReleaseId}</div>
              ) : null}
            </div>

            <div className="flex flex-wrap gap-3">
              <Link className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white" to="/admin">
                Back to admin
              </Link>
              <Link className="rounded-lg border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700" to="/dashboard">
                Back to dashboard
              </Link>
            </div>
          </div>
        </div>

        <div className="mt-6 rounded-xl border border-slate-200 bg-white p-6">
          <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <div className="text-xs font-semibold text-slate-500">Ops</div>
              <h2 className="mt-1 text-lg font-semibold text-slate-900">Capture URL</h2>
              <div className="mt-1 text-sm text-slate-600">Fetch a page and store the raw HTML for ingestion.</div>
            </div>
          </div>

          {!domain ? (
            <div className="mt-4 text-sm text-slate-600">Select a domain to capture.</div>
          ) : (
            <div className="mt-4 space-y-4">
              <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
                <div className="lg:col-span-2 space-y-4">
                  <div>
                    <div className="text-xs font-semibold text-slate-500">URL</div>
                    <input
                      value={runnerUrl}
                      onChange={(e) => setRunnerUrl(e.target.value)}
                      placeholder="https://developer.hashicorp.com/terraform/language/providers/requirements"
                      className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400"
                    />
                  </div>
                  <label className="flex items-center gap-2 text-sm text-slate-700">
                    <input
                      type="checkbox"
                      checked={runnerQuarantineOnFail}
                      onChange={(e) => setRunnerQuarantineOnFail(e.target.checked)}
                    />
                    Quarantine on failure
                  </label>
                </div>
                <div className="flex flex-col justify-end gap-2">
                  <button
                    type="button"
                    disabled={rawCaptureMutation.isPending || !domain || !runnerUrl.trim()}
                    className="w-full rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                    onClick={() => void runCapture()}
                  >
                    {rawCaptureMutation.isPending ? 'Capturing...' : 'Capture URL'}
                  </button>
                  <button
                    type="button"
                    disabled={!rawCaptureResult?.source_id || quarantineCaptureMutation.isPending}
                    className="w-full rounded-lg border border-rose-200 bg-rose-50 px-4 py-2 text-sm font-semibold text-rose-800 hover:bg-rose-100 disabled:cursor-not-allowed disabled:opacity-60"
                    onClick={async () => {
                      const cid = rawCaptureResult?.source_id
                      if (!cid || !domain) return
                      await quarantineCaptureMutation.mutateAsync({ domain, capture_id: cid, reason: 'manual quarantine from UI' })
                    }}
                  >
                    {quarantineCaptureMutation.isPending ? 'Quarantining...' : 'Quarantine capture'}
                  </button>
                </div>
              </div>

              {rawCaptureResult ? (
                <div className="rounded-lg border border-slate-200 bg-white p-3">
                  <div className="text-xs font-semibold text-slate-500">Latest capture</div>
                  <div className="mt-2 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    <div>
                      <div className="text-xs font-semibold text-slate-500">Status</div>
                      <div className="mt-1 text-sm font-semibold text-slate-900">
                        {rawCaptureResult.capture_ok ? 'captured' : 'failed'}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs font-semibold text-slate-500">HTTP</div>
                      <div className="mt-1 text-sm font-semibold text-slate-900">{rawCaptureResult.http_status}</div>
                    </div>
                    <div>
                      <div className="text-xs font-semibold text-slate-500">Capture id</div>
                      <div className="mt-1 text-sm font-semibold text-slate-900">{rawCaptureResult.source_id}</div>
                    </div>
                    <div>
                      <div className="text-xs font-semibold text-slate-500">Quarantined</div>
                      <div className="mt-1 text-sm font-semibold text-slate-900">
                        {rawCaptureResult.quarantined ? 'yes' : 'no'}
                      </div>
                    </div>
                  </div>
                  <div className="mt-3 text-xs text-slate-600 break-all">raw_html_path: {rawCaptureResult.raw_html_path}</div>
                </div>
              ) : null}
            </div>
          )}
        </div>

        <div className="mt-6 rounded-xl border border-slate-200 bg-white p-6">
          <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <div className="text-xs font-semibold text-slate-500">Ops</div>
              <h2 className="mt-1 text-lg font-semibold text-slate-900">Run Ingestion</h2>
              <div className="mt-1 text-sm text-slate-600">Run the pipeline against raw HTML or a local HTML file.</div>
            </div>
            <div className="text-xs text-slate-500">Admin-only</div>
          </div>

          {!domain ? (
            <div className="mt-4 text-sm text-slate-600">Select a domain to run ingestion.</div>
          ) : (
            <div className="mt-4 space-y-4">
              {(runnerError || ingestionRunMutation.isError) && (
                <div className="rounded-lg border border-rose-200 bg-rose-50 p-3">
                  <div className="text-sm font-semibold text-rose-800">Last run failed</div>
                  <div className="mt-1 text-xs text-rose-800">
                    {runnerError ?? firstLine(ingestionRunMutation.error) ?? 'Request failed.'}
                  </div>
                  <details className="mt-3">
                    <summary className="cursor-pointer text-xs font-semibold text-rose-800">Show details</summary>
                    <pre className="mt-2 overflow-auto rounded-lg bg-white p-3 text-xs text-slate-700">
                      {JSON.stringify(
                        {
                          ingestion_error: ingestionRunMutation.isError ? errorDetails(ingestionRunMutation.error) : null,
                          last_ingestion_response: ingestionRunMutation.data ?? null,
                        },
                        null,
                        2,
                      )}
                    </pre>
                  </details>
                </div>
              )}

              <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
                <div className="lg:col-span-2 space-y-4">
                  <div>
                    <div className="text-xs font-semibold text-slate-500">Source id</div>
                    <input
                      value={runnerSourceId}
                      onChange={(e) => setRunnerSourceId(e.target.value)}
                      placeholder="src_example_001"
                      className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400"
                    />
                  </div>

                  <div>
                    <div className="text-xs font-semibold text-slate-500">Release id</div>
                    <input
                      value={runnerReleaseId}
                      onChange={(e) => setRunnerReleaseId(e.target.value)}
                      placeholder="2025-12-demo"
                      className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400"
                    />
                  </div>

                  <div>
                    <div className="text-xs font-semibold text-slate-500">Raw HTML path (optional)</div>
                    <input
                      value={runnerRawHtmlPath}
                      onChange={(e) => setRunnerRawHtmlPath(e.target.value)}
                      placeholder="D:\\data\\captures\\page.html"
                      className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400"
                    />
                  </div>

                  <div>
                    <div className="text-xs font-semibold text-slate-500">Raw HTML (optional)</div>
                    <textarea
                      value={runnerRawHtml}
                      onChange={(e) => setRunnerRawHtml(e.target.value)}
                      rows={6}
                      placeholder="<html>...</html>"
                      className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400"
                    />
                    <div className="mt-1 text-xs text-slate-500">If both are provided, raw HTML takes priority.</div>
                  </div>
                </div>

                <div className="flex flex-col justify-end gap-2">
                  <button
                    type="button"
                    disabled={ingestionRunMutation.isPending || !domain}
                    className="w-full rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                    onClick={() => void runIngestion()}
                  >
                    {ingestionRunMutation.isPending ? 'Running...' : 'Run Ingestion'}
                  </button>
                </div>
              </div>

              <div className="mt-6">
                <div className="text-xs font-semibold text-slate-500">Latest Result</div>

                {!ingestionRunMutation.data ? (
                  <div className="mt-2 text-sm text-slate-600">Run ingestion to see results.</div>
                ) : (
                  <div className="mt-3 space-y-4">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                        <div className="text-xs font-semibold text-slate-500">Status</div>
                        <div className="mt-1 text-sm font-semibold text-slate-900">{ingestionRunMutation.data.status}</div>
                      </div>
                      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                        <div className="text-xs font-semibold text-slate-500">Release</div>
                        <div className="mt-1 text-sm font-semibold text-slate-900">{ingestionRunMutation.data.release_id}</div>
                      </div>
                      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                        <div className="text-xs font-semibold text-slate-500">Canonical objects</div>
                        <div className="mt-1 text-sm font-semibold text-slate-900">{counts.canonical_objects ?? 0}</div>
                      </div>
                      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                        <div className="text-xs font-semibold text-slate-500">Chunks</div>
                        <div className="mt-1 text-sm font-semibold text-slate-900">{counts.chunks ?? 0}</div>
                      </div>
                    </div>

                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                        <div className="text-xs font-semibold text-slate-500">Sections total</div>
                        <div className="mt-1 text-sm font-semibold text-slate-900">{counts.sections_total ?? 0}</div>
                      </div>
                      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                        <div className="text-xs font-semibold text-slate-500">Sections kept</div>
                        <div className="mt-1 text-sm font-semibold text-slate-900">{counts.sections_kept ?? 0}</div>
                      </div>
                      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                        <div className="text-xs font-semibold text-slate-500">Embeddings</div>
                        <div className="mt-1 text-sm font-semibold text-slate-900">{counts.embeddings ?? 0}</div>
                      </div>
                      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                        <div className="text-xs font-semibold text-slate-500">Source id</div>
                        <div className="mt-1 text-sm font-semibold text-slate-900">{runnerSourceId || '--'}</div>
                      </div>
                    </div>

                    <details className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                      <summary className="cursor-pointer text-xs font-semibold text-slate-700">Raw JSON</summary>
                      <pre className="mt-3 overflow-auto text-xs text-slate-700">
                        {JSON.stringify(ingestionRunMutation.data ?? null, null, 2)}
                      </pre>
                    </details>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        <div className="mt-6 rounded-xl border border-slate-200 bg-white p-6">
          <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <div className="text-xs font-semibold text-slate-500">Validation</div>
              <h2 className="mt-1 text-lg font-semibold text-slate-900">Retrieval Smoke Test</h2>
              <div className="mt-1 text-sm text-slate-600">Run a query against the active release for the selected domain.</div>
            </div>
          </div>

          {!domain ? (
            <div className="mt-4 text-sm text-slate-600">Select a domain to run retrieval.</div>
          ) : (
            <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-3">
              <div className="lg:col-span-2">
                <div className="text-xs font-semibold text-slate-500">Query</div>
                <textarea
                  value={retrievalQuery}
                  onChange={(e) => setRetrievalQuery(e.target.value)}
                  rows={4}
                  placeholder="e.g. How do I declare required_providers in Terraform?"
                  className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400"
                />
              </div>

              <div>
                <div className="text-xs font-semibold text-slate-500">TopK</div>
                <input
                  type="number"
                  min={1}
                  max={50}
                  value={retrievalTopK}
                  onChange={(e) => setRetrievalTopK(Number(e.target.value))}
                  className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900"
                />

                <div className="mt-4 text-xs font-semibold text-slate-500">Filters (JSON, optional)</div>
                <textarea
                  value={retrievalFiltersJson}
                  onChange={(e) => setRetrievalFiltersJson(e.target.value)}
                  rows={4}
                  placeholder='{ "level": "beginner" }'
                  className="mt-2 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400"
                />

                {retrievalFiltersError ? <div className="mt-2 text-xs font-semibold text-rose-700">{retrievalFiltersError}</div> : null}

                <button
                  type="button"
                  disabled={retrievalMutation.isPending || !domain || !retrievalQuery.trim()}
                  className="mt-4 w-full rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                  onClick={runRetrieval}
                >
                  {retrievalMutation.isPending ? 'Running...' : 'Run Query'}
                </button>

                {retrievalMutation.isError ? (
                  <div className="mt-2 text-xs text-slate-600">Query failed.</div>
                ) : null}
                {retrievalWarnings.length > 0 ? (
                  <div className="mt-2 text-xs text-amber-700">{retrievalWarnings.join(' ')}</div>
                ) : null}
              </div>
            </div>
          )}

          {domain ? (
            <div className="mt-6">
              <div className="text-xs font-semibold text-slate-500">Results</div>

              {retrievalMutation.isPending ? (
                <div className="mt-2 text-sm text-slate-600">Loading...</div>
              ) : retrievalResults.length === 0 ? (
                <div className="mt-2 text-sm text-slate-600">Run a query to see results.</div>
              ) : (
                <div className="mt-3 space-y-2">
                  {retrievalResults.map((r, idx) => {
                    const rr = (r as any) as Record<string, unknown>
                    const chunkId = getResultChunkId(rr)
                    const excerpt = getResultExcerpt(rr)
                    return (
                      <details key={`${chunkId}-${idx}`} className="rounded-lg border border-slate-200 bg-white px-3 py-2">
                        <summary className="cursor-pointer list-none">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <div className="truncate text-sm font-semibold text-slate-900">{chunkId}</div>
                              <div className="mt-1 text-xs text-slate-500">score: {getResultScore(rr)}</div>
                              {excerpt ? <div className="mt-2 text-xs text-slate-700">{excerpt}{excerpt.length >= 300 ? '...' : ''}</div> : null}
                            </div>
                            <div className="shrink-0 text-xs font-semibold text-slate-500">Metadata</div>
                          </div>
                        </summary>
                        <pre className="mt-3 overflow-auto rounded-lg bg-slate-50 p-3 text-xs text-slate-700">
                          {JSON.stringify(rr ?? null, null, 2)}
                        </pre>
                      </details>
                    )
                  })}
                </div>
              )}
            </div>
          ) : null}
        </div>

        <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-2">
          <div className="rounded-xl border border-slate-200 bg-white p-6">
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="text-xs font-semibold text-slate-500">Live Health</div>
                <h2 className="mt-1 text-lg font-semibold text-slate-900">Metrics</h2>
              </div>
              <button
                type="button"
                disabled={!domain || metricsQuery.isFetching}
                className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                onClick={() => void metricsQuery.refetch()}
              >
                {metricsQuery.isFetching ? 'Refreshing...' : 'Refresh'}
              </button>
            </div>

            {!domain ? (
              <div className="mt-4 text-sm text-slate-600">Select a domain to load metrics.</div>
            ) : metricsQuery.isLoading ? (
              <div className="mt-4 text-sm text-slate-600">Loading...</div>
            ) : metricsQuery.isError ? (
              <div className="mt-4 text-sm text-slate-600">Failed to load metrics.</div>
            ) : (
              <div className="mt-4 space-y-4">
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                    <div className="text-xs font-semibold text-slate-500">Window</div>
                    <div className="mt-1 text-sm font-semibold text-slate-900">
                      {(metricsQuery.data as any)?.window_hours ?? 24}h
                    </div>
                  </div>
                  <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                    <div className="text-xs font-semibold text-slate-500">Events</div>
                    <div className="mt-1 text-sm font-semibold text-slate-900">
                      {(metricsQuery.data as any)?.event_count ?? 0}
                    </div>
                  </div>
                  <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                    <div className="text-xs font-semibold text-slate-500">Last Event</div>
                    <div className="mt-1 text-sm font-semibold text-slate-900">{lastEventTimestamp ?? '--'}</div>
                  </div>
                  <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                    <div className="text-xs font-semibold text-slate-500">Alerts</div>
                    <div className="mt-1 text-sm font-semibold text-slate-900">
                      {Array.isArray((metricsQuery.data as any)?.alerts) ? ((metricsQuery.data as any)?.alerts as any[]).length : 0}
                    </div>
                  </div>
                </div>

                {countsByStatus.length > 0 ? (
                  <div>
                    <div className="text-xs font-semibold text-slate-500">Status counts</div>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {countsByStatus.map((row) => (
                        <div
                          key={row.key}
                          className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-semibold text-slate-700"
                        >
                          {row.key}: {row.count}
                        </div>
                      ))}
                    </div>
                  </div>
                ) : null}

                <div>
                  <div className="text-xs font-semibold text-slate-500">Top events</div>
                  {countsByEvent.length === 0 ? (
                    <div className="mt-2 text-sm text-slate-600">No events in window.</div>
                  ) : (
                    <div className="mt-2 overflow-hidden rounded-lg border border-slate-200">
                      <div className="divide-y divide-slate-200">
                        {countsByEvent.map((row) => (
                          <div key={row.key} className="flex items-center justify-between bg-white px-3 py-2">
                            <div className="truncate text-sm text-slate-700">{row.key}</div>
                            <div className="ml-3 text-sm font-semibold text-slate-900">{row.count}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>

                <details className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                  <summary className="cursor-pointer text-xs font-semibold text-slate-700">Raw metrics JSON</summary>
                  <pre className="mt-3 overflow-auto text-xs text-slate-700">
                    {JSON.stringify(metricsQuery.data ?? null, null, 2)}
                  </pre>
                </details>
              </div>
            )}
          </div>

          <div className="rounded-xl border border-slate-200 bg-white p-6">
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="text-xs font-semibold text-slate-500">Live Health</div>
                <h2 className="mt-1 text-lg font-semibold text-slate-900">Events</h2>
              </div>
              <button
                type="button"
                disabled={!domain || eventsQuery.isFetching}
                className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                onClick={() => void eventsQuery.refetch()}
              >
                {eventsQuery.isFetching ? 'Refreshing...' : 'Refresh'}
              </button>
            </div>

            {!domain ? (
              <div className="mt-4 text-sm text-slate-600">Select a domain to load events.</div>
            ) : eventsQuery.isLoading ? (
              <div className="mt-4 text-sm text-slate-600">Loading...</div>
            ) : eventsQuery.isError ? (
              <div className="mt-4 text-sm text-slate-600">Failed to load events.</div>
            ) : (
              <div className="mt-4 space-y-2">
                {allEvents.length > 0 ? (
                  <>
                    <div className="text-xs text-slate-500">
                      Showing latest {Math.min(10, allEvents.length)} of {allEvents.length} events.
                    </div>
                    {allEvents.slice(0, 10).map((evt, idx) => {
                      const ts = typeof evt?.timestamp === 'string' ? evt.timestamp : ''
                      const name = typeof evt?.event === 'string' ? evt.event : 'event'
                      const status = typeof evt?.status === 'string' ? evt.status : ''
                      const msg =
                        typeof evt?.message === 'string'
                          ? evt.message
                          : typeof evt?.url === 'string'
                            ? evt.url
                            : typeof evt?.detail === 'string'
                              ? evt.detail
                              : ''

                      return (
                        <details key={`${ts}-${name}-${idx}`} className="rounded-lg border border-slate-200 bg-white px-3 py-2">
                          <summary className="cursor-pointer list-none">
                            <div className="flex items-start justify-between gap-3">
                              <div className="min-w-0">
                                <div className="truncate text-sm font-semibold text-slate-900">
                                  {name}
                                  {status ? <span className="ml-2 text-xs font-semibold text-slate-500">{status}</span> : null}
                                </div>
                                <div className="truncate text-xs text-slate-500">{ts || '--'}</div>
                                {msg ? <div className="mt-1 truncate text-xs text-slate-600">{msg}</div> : null}
                              </div>
                              <div className="shrink-0 text-xs font-semibold text-slate-500">Details</div>
                            </div>
                          </summary>
                          <pre className="mt-3 overflow-auto rounded-lg bg-slate-50 p-3 text-xs text-slate-700">
                            {JSON.stringify(evt ?? null, null, 2)}
                          </pre>
                        </details>
                      )
                    })}
                  </>
                ) : (
                  <div className="text-sm text-slate-600">No events yet.</div>
                )}
                <details className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                  <summary className="cursor-pointer text-xs font-semibold text-slate-700">Raw events JSON</summary>
                  <pre className="mt-3 overflow-auto text-xs text-slate-700">
                    {JSON.stringify(allEvents ?? null, null, 2)}
                  </pre>
                </details>
              </div>
            )}
          </div>
        </div>

        <div className="mt-6 rounded-xl border border-slate-200 bg-white p-6">
          <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <div className="text-xs font-semibold text-slate-500">Ops</div>
              <h2 className="mt-1 text-lg font-semibold text-slate-900">Release Controls</h2>
              <div className="mt-1 text-sm text-slate-600">Promote candidate releases for the selected domain.</div>
            </div>
          </div>

          {!domain ? (
            <div className="mt-4 text-sm text-slate-600">Select a domain to manage releases.</div>
          ) : (
            <div className="mt-4 space-y-4">
              {releasePromoteMutation.isError ? (
                <div className="rounded-lg border border-rose-200 bg-rose-50 p-3">
                  <div className="text-sm font-semibold text-rose-800">Promotion failed</div>
                  <div className="mt-1 text-xs text-rose-800">{firstLine(releasePromoteMutation.error)}</div>
                  <details className="mt-3">
                    <summary className="cursor-pointer text-xs font-semibold text-rose-800">Show details</summary>
                    <pre className="mt-2 overflow-auto rounded-lg bg-white p-3 text-xs text-slate-700">
                      {JSON.stringify(errorDetails(releasePromoteMutation.error), null, 2)}
                    </pre>
                  </details>
                </div>
              ) : null}

              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                  <div className="text-xs font-semibold text-slate-500">Active</div>
                  <div className="mt-1 break-all text-sm font-semibold text-slate-900">{activeReleaseId ?? '--'}</div>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                  <div className="text-xs font-semibold text-slate-500">Candidate (last run)</div>
                  <div className="mt-1 break-all text-sm font-semibold text-slate-900">{candidateReleaseId ?? '--'}</div>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                  <div className="text-xs font-semibold text-slate-500">Release count</div>
                  <div className="mt-1 text-sm font-semibold text-slate-900">
                    {Array.isArray(releaseStatusQuery.data?.releases) ? releaseStatusQuery.data?.releases.length : 0}
                  </div>
                </div>
              </div>

              <div className="flex flex-col gap-2 sm:flex-row">
                <button
                  type="button"
                  disabled={!domain || !candidateReleaseId || releasePromoteMutation.isPending}
                  className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                  onClick={async () => {
                    if (!domain || !candidateReleaseId) return
                    await releasePromoteMutation.mutateAsync({ domain, releaseId: candidateReleaseId, reason: 'promote candidate from ingestion page' })
                    await releaseStatusQuery.refetch()
                    await auditQuery.refetch()
                  }}
                >
                  {releasePromoteMutation.isPending ? 'Promoting...' : 'Promote candidate'}
                </button>
              </div>

              <div className="rounded-lg border border-slate-200 bg-white">
                <div className="border-b border-slate-200 px-3 py-2 text-xs font-semibold text-slate-500">Releases</div>
                {releaseStatusQuery.isError ? (
                  <div className="px-3 py-3 text-sm text-slate-600">Failed to load releases.</div>
                ) : releaseStatusQuery.isLoading ? (
                  <div className="px-3 py-3 text-sm text-slate-600">Loading...</div>
                ) : (releaseStatusQuery.data?.releases ?? []).length === 0 ? (
                  <div className="px-3 py-3 text-sm text-slate-600">No releases yet.</div>
                ) : (
                  <div className="divide-y divide-slate-200">
                    {(releaseStatusQuery.data?.releases ?? []).map((rid) => (
                      <div key={rid} className="flex items-center justify-between px-3 py-2">
                        <div className="truncate text-sm text-slate-700">{rid}</div>
                        {rid === activeReleaseId ? (
                          <div className="text-xs font-semibold text-emerald-700">active</div>
                        ) : null}
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div className="rounded-lg border border-slate-200 bg-white">
                <div className="border-b border-slate-200 px-3 py-2 text-xs font-semibold text-slate-500">Release audit (latest)</div>
                {auditQuery.isError ? (
                  <div className="px-3 py-3 text-sm text-slate-600">Failed to load audit.</div>
                ) : auditQuery.isLoading ? (
                  <div className="px-3 py-3 text-sm text-slate-600">Loading...</div>
                ) : auditEvents.length === 0 ? (
                  <div className="px-3 py-3 text-sm text-slate-600">No promotions yet.</div>
                ) : (
                  <div className="divide-y divide-slate-200">
                    {auditEvents.slice(0, 10).map((evt, idx) => {
                      const ts = typeof evt?.timestamp === 'string' ? evt.timestamp : ''
                      const rid = typeof evt?.release_id === 'string' ? evt.release_id : ''
                      const prev = typeof evt?.previous_release_id === 'string' ? evt.previous_release_id : ''
                      return (
                        <details key={`${ts}-${rid}-${idx}`} className="bg-white px-3 py-2">
                          <summary className="cursor-pointer list-none">
                            <div className="flex items-start justify-between gap-3">
                              <div className="min-w-0">
                                <div className="truncate text-sm font-semibold text-slate-900">{rid || '--'}</div>
                                <div className="truncate text-xs text-slate-500">{ts || '--'}</div>
                                {prev ? <div className="mt-1 truncate text-xs text-slate-600">previous: {prev}</div> : null}
                              </div>
                              <div className="shrink-0 text-xs font-semibold text-slate-500">Details</div>
                            </div>
                          </summary>
                          <pre className="mt-3 overflow-auto rounded-lg bg-slate-50 p-3 text-xs text-slate-700">
                            {JSON.stringify(evt ?? null, null, 2)}
                          </pre>
                        </details>
                      )
                    })}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
