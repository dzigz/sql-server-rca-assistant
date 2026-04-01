const API_PREFIX = '/api'

export function normalizeApiBase(rawBase?: string | null): string {
  if (!rawBase) return ''

  let normalized = rawBase.trim()
  if (!normalized) return ''

  normalized = normalized.replace(/\/+$/, '')
  if (normalized.endsWith(API_PREFIX)) {
    normalized = normalized.slice(0, -API_PREFIX.length)
  }

  return normalized
}

export function buildApiUrl(path: string, apiBase = ''): string {
  const normalizedPath = path.startsWith('/') ? path : `/${path}`
  const normalizedBase = normalizeApiBase(apiBase)
  return `${normalizedBase}${normalizedPath}`
}
