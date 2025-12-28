import { api } from './client'

export type DomainsResponse = {
  domains: string[]
}

export const domainsQueryKey = () => ['domains'] as const

export async function getDomains(): Promise<DomainsResponse> {
  const res = await api.get<DomainsResponse>('/domains')
  const domains = Array.isArray(res.data?.domains) ? res.data.domains.filter((d) => typeof d === 'string') : []
  return { domains }
}

export type IngestionMetricsResponse = {
  domain: string
  [key: string]: unknown
}

export type IngestionEvent = Record<string, unknown>

export type IngestionEventsResponse = {
  domain: string
  events: IngestionEvent[]
}

export type IngestionRawCaptureRequest = {
  source_id: string
  domain: string
  url: string
  timeout?: number
  persist_to_db?: boolean
  clean?: boolean
  quarantine_suspicious?: boolean
}

export type IngestionRawCaptureResponse = {
  source_id: string
  domain?: string | null
  url?: string | null
  http_status: number
  headers: Record<string, unknown>
  raw_html_path: string
  content_hash: string
  content_signature: string
  retrieved_at: string
  capture_ok: boolean
  cleaned_text?: string | null
  quarantined: boolean
  quarantine_reason?: string | null
  quarantined_at?: string | null
  db_persisted: boolean
  db_error?: string | null
}

export type IngestionRunRequest = {
  source_id: string
  capture_id?: string | null
  domain: string
  release_id: string
  raw_html?: string | null
  raw_html_path?: string | null
  created_by?: string | null
}

export type IngestionRunResponse = {
  status: string
  domain: string
  release_id: string
  release: Record<string, unknown>
  counts: Record<string, number>
}

export type IngestionRawCaptureBatchItem = {
  source_id: string
  url: string
  timeout?: number
  persist_to_db?: boolean
  clean?: boolean
  quarantine_suspicious?: boolean
}

export type IngestionRawCaptureBatchRequest = {
  domain: string
  continue_on_error?: boolean
  items: IngestionRawCaptureBatchItem[]
}

export type IngestionRawCaptureBatchResponse = {
  domain: string
  summary: Record<string, number>
  results: Array<Record<string, unknown>>
}

export type IngestionRunBatchItem = {
  source_id: string
  raw_html?: string | null
  raw_html_path?: string | null
  capture_id?: string | null
}

export type IngestionRunBatchRequest = {
  domain: string
  release_id?: string | null
  created_by?: string | null
  continue_on_error?: boolean
  force?: boolean
  items: IngestionRunBatchItem[]
}

export type IngestionRunBatchResponse = {
  domain: string
  release_id: string
  release: Record<string, unknown>
  summary: Record<string, unknown>
  results: Array<Record<string, unknown>>
}

export type IngestionIngestBatchItem = {
  source_id: string
  url: string
  timeout?: number
  clean?: boolean
  quarantine_suspicious?: boolean
}

export type IngestionIngestBatchRequest = {
  domain: string
  release_id?: string | null
  created_by?: string | null
  continue_on_error?: boolean
  force?: boolean
  items: IngestionIngestBatchItem[]
}

export type IngestionIngestBatchResponse = {
  domain: string
  release_id: string
  release: Record<string, unknown>
  summary: Record<string, unknown>
  results: Array<Record<string, unknown>>
}

export type QuarantineCaptureRequest = {
  domain: string
  capture_id: string
  reason?: string | null
}

export const ingestionMetricsQueryKey = (domain: string, hours = 24) => ['ingestionMetrics', domain, hours] as const
export const ingestionEventsQueryKey = (domain: string, limit = 100) => ['ingestionEvents', domain, limit] as const
export const ingestionRawCaptureMutationKey = () => ['ingestionRawCapture'] as const

export async function getIngestionMetrics(domain: string, hours = 24): Promise<IngestionMetricsResponse> {
  const res = await api.get<IngestionMetricsResponse>(`/ingestion/${encodeURIComponent(domain)}/metrics`, {
    params: { hours },
  })
  return res.data
}

export async function getIngestionEvents(domain: string, limit = 100): Promise<IngestionEventsResponse> {
  const res = await api.get<IngestionEventsResponse>(`/ingestion/${encodeURIComponent(domain)}/events`, {
    params: { limit },
  })
  return res.data
}

export async function postIngestionRawCapture(body: IngestionRawCaptureRequest): Promise<IngestionRawCaptureResponse> {
  const res = await api.post<IngestionRawCaptureResponse>('/ingestion/raw-capture', {
    source_id: body.source_id,
    domain: body.domain,
    url: body.url,
    timeout: body.timeout ?? 10,
    persist_to_db: body.persist_to_db ?? false,
    clean: body.clean ?? false,
    quarantine_suspicious: body.quarantine_suspicious ?? true,
  })
  return res.data
}

export async function postIngestionRawCaptureBatch(
  body: IngestionRawCaptureBatchRequest,
): Promise<IngestionRawCaptureBatchResponse> {
  const res = await api.post<IngestionRawCaptureBatchResponse>('/ingestion/raw-capture/batch', {
    domain: body.domain,
    continue_on_error: body.continue_on_error ?? false,
    items: body.items.map((it) => ({
      source_id: it.source_id,
      url: it.url,
      timeout: it.timeout ?? 10,
      persist_to_db: it.persist_to_db ?? false,
      clean: it.clean ?? false,
      quarantine_suspicious: it.quarantine_suspicious ?? true,
    })),
  })
  return res.data
}

export async function postQuarantineCapture(body: QuarantineCaptureRequest): Promise<IngestionRawCaptureResponse> {
  const res = await api.post<IngestionRawCaptureResponse>('/ingestion/quarantine', {
    domain: body.domain,
    capture_id: body.capture_id,
    reason: body.reason ?? null,
  })
  return res.data
}

export async function postIngestionRun(body: IngestionRunRequest): Promise<IngestionRunResponse> {
  const res = await api.post<IngestionRunResponse>(
    '/ingestion/run',
    {
      source_id: body.source_id,
      capture_id: body.capture_id ?? null,
      domain: body.domain,
      release_id: body.release_id,
      raw_html: body.raw_html ?? null,
      raw_html_path: body.raw_html_path ?? null,
      created_by: body.created_by ?? null,
    },
    {
      timeout: 180000,
    },
  )
  return res.data
}

export async function postIngestionRunBatch(body: IngestionRunBatchRequest): Promise<IngestionRunBatchResponse> {
  const res = await api.post<IngestionRunBatchResponse>(
    '/ingestion/run/batch',
    {
      domain: body.domain,
      release_id: body.release_id ?? null,
      created_by: body.created_by ?? null,
      continue_on_error: body.continue_on_error ?? false,
      force: body.force ?? false,
      items: body.items.map((it) => ({
        source_id: it.source_id,
        raw_html: it.raw_html ?? null,
        raw_html_path: it.raw_html_path ?? null,
        capture_id: it.capture_id ?? null,
      })),
    },
    { timeout: 180000 },
  )
  return res.data
}

export async function postIngestionIngestBatch(body: IngestionIngestBatchRequest): Promise<IngestionIngestBatchResponse> {
  const res = await api.post<IngestionIngestBatchResponse>(
    '/ingestion/ingest/batch',
    {
      domain: body.domain,
      release_id: body.release_id ?? null,
      created_by: body.created_by ?? null,
      continue_on_error: body.continue_on_error ?? false,
      force: body.force ?? false,
      items: body.items.map((it) => ({
        source_id: it.source_id,
        url: it.url,
        timeout: it.timeout ?? 10,
        clean: it.clean ?? false,
        quarantine_suspicious: it.quarantine_suspicious ?? true,
      })),
    },
    { timeout: 180000 },
  )
  return res.data
}

export async function postIngestionFileCapture(args: {
  domain: string
  source_id: string
  file: File
  clean?: boolean
  quarantine_suspicious?: boolean
}): Promise<IngestionRawCaptureResponse> {
  const form = new FormData()
  form.append('domain', args.domain)
  form.append('source_id', args.source_id)
  form.append('file', args.file)
  if (args.clean !== undefined) form.append('clean', String(args.clean))
  if (args.quarantine_suspicious !== undefined) form.append('quarantine_suspicious', String(args.quarantine_suspicious))
  const res = await api.post<IngestionRawCaptureResponse>('/ingestion/file-capture', form, {
    headers: { 'Content-Type': 'multipart/form-data' },
    timeout: 180000,
  })
  return res.data
}
