'use client'

import { useState, memo, useEffect, useRef, useCallback } from 'react'
import { Loader2, ExternalLink, Maximize2, Minimize2 } from 'lucide-react'
import { iframeRegistry } from '@/lib/iframe-registry'

interface GrafanaEmbedProps {
  dashboardUid: string
  panelId?: number
  title: string
  height?: number
  from?: string
  to?: string
  theme?: 'light' | 'dark'
  showFullDashboard?: boolean
  source?: string  // DEBUG: identifies where this embed is rendered from
}

const GRAFANA_BASE_URL = process.env.NEXT_PUBLIC_GRAFANA_URL || 'http://localhost:3001'

/**
 * GrafanaEmbed uses an iframe registry to preserve iframes across React remounts.
 * This prevents the iframe from reloading when ReactMarkdown recreates components
 * during streaming updates.
 */
export type { GrafanaEmbedProps }

export const GrafanaEmbed = memo(function GrafanaEmbed({
  dashboardUid,
  panelId,
  title,
  height = 300,
  from = 'now-1h',
  to = 'now',
  theme,
  showFullDashboard = false,
  source = 'unknown',
}: GrafanaEmbedProps) {
  const [isLoading, setIsLoading] = useState(true)
  const [hasError, setHasError] = useState(false)
  const [isExpanded, setIsExpanded] = useState(false)
  const [resolvedTheme, setResolvedTheme] = useState<'light' | 'dark'>(theme || 'light')
  const containerRef = useRef<HTMLDivElement>(null)
  const iframeKeyRef = useRef<string | null>(null)

  useEffect(() => {
    if (theme) {
      setResolvedTheme(theme)
      return
    }

    const media = window.matchMedia('(prefers-color-scheme: dark)')
    const syncTheme = () => setResolvedTheme(media.matches ? 'dark' : 'light')
    syncTheme()
    media.addEventListener('change', syncTheme)
    return () => media.removeEventListener('change', syncTheme)
  }, [theme])

  // Build the embed URL
  const buildUrl = useCallback(() => {
    const params = new URLSearchParams({
      orgId: '1',
      from,
      to,
      theme: resolvedTheme,
    })

    if (showFullDashboard || !panelId) {
      return `${GRAFANA_BASE_URL}/d/${dashboardUid}?${params.toString()}&kiosk`
    } else {
      params.append('panelId', panelId.toString())
      return `${GRAFANA_BASE_URL}/d-solo/${dashboardUid}?${params.toString()}`
    }
  }, [dashboardUid, panelId, from, resolvedTheme, showFullDashboard, to])

  const embedUrl = buildUrl()
  const fullUrl = `${GRAFANA_BASE_URL}/d/${dashboardUid}?orgId=1&from=${from}&to=${to}`

  useEffect(() => {
    setIsLoading(true)
    setHasError(false)
  }, [embedUrl])

  // Acquire iframe from registry on mount, release on unmount
  // Use source as unique key so same chart can render in multiple places
  useEffect(() => {
    if (!containerRef.current) return

    // Use source as key for unique instance, fallback to dashboard/panel for legacy
    const key = source !== 'unknown' ? source : iframeRegistry.getKey(dashboardUid, panelId)
    iframeKeyRef.current = key

    const iframe = iframeRegistry.acquire(
      key,
      embedUrl,
      () => setIsLoading(false),
      () => {
        setIsLoading(false)
        setHasError(true)
      }
    )

    // Check if iframe is already loaded (reused from registry)
    if (iframe.contentDocument?.readyState === 'complete') {
      setIsLoading(false)
    }

    // Move iframe into our container
    containerRef.current.appendChild(iframe)

    // Apply styles
    iframe.className = isLoading ? 'opacity-0' : 'opacity-100 transition-opacity duration-300'

    return () => {
      // Release iframe back to registry (doesn't destroy it)
      if (iframeKeyRef.current) {
        iframeRegistry.release(iframeKeyRef.current)
      }
    }
  }, [dashboardUid, panelId, embedUrl, source])

  // Update iframe opacity when loading state changes
  useEffect(() => {
    if (!containerRef.current) return
    const iframe = containerRef.current.querySelector('iframe')
    if (iframe) {
      iframe.className = isLoading ? 'opacity-0' : 'opacity-100 transition-opacity duration-300'
    }
  }, [isLoading])

  const actualHeight = isExpanded ? 600 : height

  return (
    <div className="rounded-lg border border-[var(--border)] overflow-hidden bg-[var(--background)]">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-[var(--border)] bg-[var(--muted)]">
        <h3 className="text-sm font-medium">{title}</h3>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setIsExpanded(!isExpanded)}
            className="p-1 rounded hover:bg-[var(--border)] text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-colors"
            title={isExpanded ? 'Minimize' : 'Expand'}
            aria-label={isExpanded ? 'Collapse chart panel' : 'Expand chart panel'}
          >
            {isExpanded ? (
              <Minimize2 className="w-4 h-4" />
            ) : (
              <Maximize2 className="w-4 h-4" />
            )}
          </button>
          <a
            href={fullUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="p-1 rounded hover:bg-[var(--border)] text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-colors"
            title="Open in Grafana"
            aria-label="Open chart in Grafana"
          >
            <ExternalLink className="w-4 h-4" />
          </a>
        </div>
      </div>

      {/* Embed Container */}
      <div className="relative" style={{ height: actualHeight }}>
        {isLoading && (
          <div className="absolute inset-0 flex items-center justify-center bg-[var(--muted)]">
            <Loader2 className="w-6 h-6 animate-spin text-[var(--muted-foreground)]" />
          </div>
        )}

        {hasError && (
          <div className="absolute inset-0 flex flex-col items-center justify-center bg-[var(--muted)] text-[var(--muted-foreground)]">
            <p className="text-sm mb-2">Failed to load chart</p>
            <a
              href={fullUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="text-sm text-[var(--accent)] hover:underline flex items-center gap-1"
            >
              Open in Grafana <ExternalLink className="w-3 h-3" />
            </a>
          </div>
        )}

        {/* Container for the registry-managed iframe */}
        <div
          ref={containerRef}
          className="w-full h-full"
          style={{ display: hasError ? 'none' : 'block' }}
          aria-label={title ? `${title} chart` : 'Grafana chart'}
        />
      </div>
    </div>
  )
})

// Predefined dashboard configurations
export const DASHBOARDS = {
  overview: {
    uid: 'sql-server-overview',
    title: 'SQL Server Overview',
    panels: [
      { id: 1, title: 'Wait Time by Type' },
      { id: 2, title: 'Blocked Sessions' },
      { id: 3, title: 'Active Requests' },
      { id: 4, title: 'Memory Grant Status' },
      { id: 5, title: 'Top 10 Wait Types' },
    ],
  },
  waitStats: {
    uid: 'wait-stats',
    title: 'Wait Statistics',
    panels: [
      { id: 1, title: 'Wait Time Trend' },
      { id: 2, title: 'Wait Time by Category' },
      { id: 3, title: 'Top 15 Wait Types (Detailed)' },
    ],
  },
  queryPerformance: {
    uid: 'query-performance',
    title: 'Query Performance',
    panels: [
      { id: 1, title: 'Top 10 Queries by CPU' },
      { id: 2, title: 'Top 10 Queries by Logical Reads' },
      { id: 3, title: 'Query Details' },
      { id: 4, title: 'Query Execution Trend' },
    ],
  },
}
