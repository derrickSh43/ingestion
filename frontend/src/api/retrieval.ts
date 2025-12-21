import { api } from './client'

export type RetrievalQueryRequest = {
  domain: string
  query: string
  filters?: Record<string, unknown> | null
  top_k?: number
  release_id?: string | null
}

export type RetrievalQueryResult = Record<string, unknown>

export type RetrievalQueryResponse = {
  domain: string
  release_id: string
  results: RetrievalQueryResult[]
  warnings?: string[]
}

function stableKey(value: unknown): string {
  if (value === null) return 'null'
  if (value === undefined) return 'undefined'
  if (typeof value !== 'object') return String(value)
  if (Array.isArray(value)) return `[${value.map(stableKey).join(',')}]`

  const obj = value as Record<string, unknown>
  const keys = Object.keys(obj).sort()
  return `{${keys.map((k) => `${k}:${stableKey(obj[k])}`).join(',')}}`
}

export const retrievalQueryKey = (req: RetrievalQueryRequest) =>
  [
    'retrievalQuery',
    req.domain,
    req.release_id ?? null,
    req.top_k ?? 5,
    stableKey(req.filters ?? null),
    req.query,
  ] as const

export async function postRetrievalQuery(body: RetrievalQueryRequest): Promise<RetrievalQueryResponse> {
  const res = await api.post<RetrievalQueryResponse>('/retrieve', {
    domain: body.domain,
    query: body.query,
    filters: body.filters ?? null,
    top_k: body.top_k ?? 5,
    release_id: body.release_id ?? null,
  })
  return res.data
}
