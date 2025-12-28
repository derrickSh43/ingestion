import { api } from './client'

export type ReleaseStatusResponse = {
  domain: string
  active_release?: string | null
  releases: string[]
}

export type ReleaseAuditEvent = Record<string, unknown>

export type ReleaseAuditResponse = {
  domain: string
  events: ReleaseAuditEvent[]
}

export const releaseStatusQueryKey = (domain: string) => ['releaseStatus', domain] as const
export const releaseAuditQueryKey = (domain: string, limit = 100) => ['releaseAudit', domain, limit] as const

export type ReleasePromoteRequest = {
  reason?: string | null
  promoted_by?: string | null
}

export type ReleasePromoteResponse = Record<string, unknown>

export type ReleaseMergeRequest = {
  source_release_ids: string[]
  target_release_id?: string | null
  created_by?: string | null
}

export type ReleaseMergeResponse = {
  domain: string
  target_release_id: string
  source_release_ids: string[]
  summary: Record<string, unknown>
  release: Record<string, unknown>
}

export async function getReleaseStatus(domain: string): Promise<ReleaseStatusResponse> {
  const res = await api.get<ReleaseStatusResponse>(`/releases/${encodeURIComponent(domain)}`)
  return res.data
}

export async function getReleaseAudit(domain: string, limit = 100): Promise<ReleaseAuditResponse> {
  const res = await api.get<ReleaseAuditResponse>(`/releases/${encodeURIComponent(domain)}/audit`, {
    params: { limit },
  })
  return res.data
}

export async function postReleasePromote(
  domain: string,
  releaseId: string,
  body: ReleasePromoteRequest,
): Promise<ReleasePromoteResponse> {
  const res = await api.post<ReleasePromoteResponse>(`/releases/${encodeURIComponent(domain)}/${encodeURIComponent(releaseId)}/promote`, {
    reason: body.reason ?? null,
    promoted_by: body.promoted_by ?? null,
  })
  return res.data
}

export async function postReleaseMerge(domain: string, body: ReleaseMergeRequest): Promise<ReleaseMergeResponse> {
  const res = await api.post<ReleaseMergeResponse>(`/releases/${encodeURIComponent(domain)}/merge`, {
    source_release_ids: body.source_release_ids,
    target_release_id: body.target_release_id ?? null,
    created_by: body.created_by ?? null,
  })
  return res.data
}
