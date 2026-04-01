'use client'

import { useState, useMemo } from 'react'
import { Wrench, ChevronDown, ChevronRight, Check, Loader2, BarChart3 } from 'lucide-react'
import { GrafanaEmbed } from './dashboard/GrafanaEmbed'

type BlitzInstallStatus = 'idle' | 'installing' | 'installed' | 'declined' | 'error'

interface BlitzInstallState {
  status: BlitzInstallStatus
  message?: string
}

interface ToolCallProps {
  toolCall: {
    id: string
    name: string
    arguments: Record<string, unknown>
    result?: string
    completed?: boolean  // Whether the tool has finished executing
  }
  isComplete?: boolean  // Whether the parent message has finished streaming
  sessionId?: string | null
  blitzInstallState?: BlitzInstallState
  onInstallBlitz?: () => Promise<void>
  onDeclineBlitz?: () => Promise<void>
}

// Format tool name for display
function formatToolName(name: string): string {
  return name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())
}

// Get tool icon/color based on name
function getToolStyle(name: string): { color: string; icon: string } {
  if (name.includes('chart') || name.includes('embed')) {
    return { color: 'text-cyan-500', icon: 'chart' }
  }
  if (name.includes('baseline') || name.includes('compare')) {
    return { color: 'text-blue-500', icon: 'compare' }
  }
  if (name.includes('blitz')) {
    return { color: 'text-orange-500', icon: 'blitz' }
  }
  if (name.includes('query') || name.includes('clickhouse')) {
    return { color: 'text-green-500', icon: 'query' }
  }
  if (name.includes('code') || name.includes('analyze')) {
    return { color: 'text-purple-500', icon: 'code' }
  }
  return { color: 'text-[var(--muted-foreground)]', icon: 'default' }
}

// Parse tool result and check if it's a chart embed
interface ChartEmbedResult {
  type: 'chart_embed'
  title?: string
  embed_url: string
  dashboard_uid?: string
  panel_id?: number
  time_range?: string
  sql_query?: string
}

interface BlitzInstallOffer {
  type: 'blitz_install_offer'
  title?: string
  description?: string
  target_host?: string
  target_database?: string
  procedures?: string[]
}

function parseChartEmbed(result: string | undefined): ChartEmbedResult | null {
  if (!result) return null

  try {
    // Try to parse as JSON
    const parsed = JSON.parse(result)

    // Check if it's wrapped in a success/data structure
    const data = parsed.data || parsed

    if (data.type === 'chart_embed' && data.embed_url) {
      return data as ChartEmbedResult
    }
  } catch {
    // Not JSON or parsing failed
  }

  return null
}

function parseBlitzInstallOffer(result: string | undefined): BlitzInstallOffer | null {
  if (!result) return null

  try {
    const parsed = JSON.parse(result)
    const payload = (parsed && typeof parsed === 'object' && 'data' in parsed)
      ? (parsed as { data?: unknown }).data
      : parsed
    if (!payload || typeof payload !== 'object') return null

    const payloadObj = payload as { status?: string; install_offer?: unknown }
    if (payloadObj.status !== 'install_required') return null
    if (!payloadObj.install_offer || typeof payloadObj.install_offer !== 'object') return null

    const offer = payloadObj.install_offer as Partial<BlitzInstallOffer>
    if (offer.type !== 'blitz_install_offer') return null
    return offer as BlitzInstallOffer
  } catch {
    return null
  }
}

export function ToolCall({
  toolCall,
  isComplete,
  sessionId,
  blitzInstallState,
  onInstallBlitz,
  onDeclineBlitz,
}: ToolCallProps) {
  const style = getToolStyle(toolCall.name)

  // Check if result is a chart embed
  const chartEmbed = useMemo(
    () => parseChartEmbed(toolCall.result),
    [toolCall.result]
  )
  const blitzInstallOffer = useMemo(
    () => parseBlitzInstallOffer(toolCall.result),
    [toolCall.result]
  )

  // Charts should be expanded by default
  const [isExpanded, setIsExpanded] = useState(!!chartEmbed || !!blitzInstallOffer)

  // Tool is done if marked completed, has a result, or if the parent message is complete
  const isDone = toolCall.completed || toolCall.result !== undefined || isComplete

  // Format arguments for display
  const argsPreview = Object.entries(toolCall.arguments)
    .slice(0, 2)
    .map(([key, value]) => {
      const strValue = typeof value === 'string' ? value : JSON.stringify(value)
      const truncated = strValue.length > 30 ? strValue.slice(0, 30) + '...' : strValue
      return `${key}: ${truncated}`
    })
    .join(', ')

  if (blitzInstallOffer && isDone) {
    const status = blitzInstallState?.status || 'idle'
    const statusMessage = blitzInstallState?.message || ''
    const isInstalling = status === 'installing'
    const isInstalled = status === 'installed'
    const isDeclined = status === 'declined'
    const installDisabled = !sessionId || !onInstallBlitz || isInstalling || isInstalled
    const declineDisabled = !sessionId || !onDeclineBlitz || isInstalling || isDeclined

    return (
      <div className="rounded-lg border border-amber-300 bg-amber-50/60 dark:bg-amber-900/20 overflow-hidden">
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="w-full flex items-center gap-2 px-3 py-2 text-left text-sm hover:bg-amber-100/60 dark:hover:bg-amber-900/30 transition-colors"
        >
          <Wrench className={`w-4 h-4 flex-shrink-0 ${style.color}`} />
          <span className="font-medium">{blitzInstallOffer.title || 'Install Blitz scripts'}</span>
          {isInstalled ? (
            <Check className="w-3.5 h-3.5 text-green-500" />
          ) : isInstalling ? (
            <Loader2 className="w-3.5 h-3.5 animate-spin text-[var(--muted-foreground)]" />
          ) : null}
          <span className="flex-1 text-xs text-[var(--muted-foreground)] truncate">
            {blitzInstallOffer.target_host || 'Target SQL Server'}
          </span>
          {isExpanded ? (
            <ChevronDown className="w-4 h-4 text-[var(--muted-foreground)]" />
          ) : (
            <ChevronRight className="w-4 h-4 text-[var(--muted-foreground)]" />
          )}
        </button>

        {isExpanded && (
          <div className="px-3 pb-3 pt-2 border-t border-amber-300/70 text-sm space-y-3">
            <p className="text-[var(--foreground)]">
              {blitzInstallOffer.description || 'Install First Responder Kit procedures to continue diagnostics.'}
            </p>
            {blitzInstallOffer.target_database && (
              <p className="text-xs text-[var(--muted-foreground)]">
                Install target database: <code>{blitzInstallOffer.target_database}</code>
              </p>
            )}
            {blitzInstallOffer.procedures && blitzInstallOffer.procedures.length > 0 && (
              <p className="text-xs text-[var(--muted-foreground)]">
                Procedures: {blitzInstallOffer.procedures.join(', ')}
              </p>
            )}

            {isInstalled ? (
              <p className="text-sm text-green-700 dark:text-green-400">
                {statusMessage || 'Installation completed. Ask the assistant to run diagnostics again.'}
              </p>
            ) : isDeclined ? (
              <p className="text-sm text-[var(--muted-foreground)]">
                {statusMessage || 'Installation declined for this session.'}
              </p>
            ) : (
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => onInstallBlitz?.()}
                  disabled={installDisabled}
                  className="px-3 py-1.5 rounded bg-amber-600 text-white hover:bg-amber-700 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {isInstalling ? 'Installing...' : 'Install scripts'}
                </button>
                <button
                  type="button"
                  onClick={() => onDeclineBlitz?.()}
                  disabled={declineDisabled}
                  className="px-3 py-1.5 rounded border border-[var(--border)] hover:bg-[var(--muted)] disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Decline
                </button>
              </div>
            )}

            {status === 'error' && statusMessage && (
              <p className="text-sm text-red-600 dark:text-red-400">{statusMessage}</p>
            )}
          </div>
        )}
      </div>
    )
  }

  // If this is a chart embed and it's complete, render it inline
  if (chartEmbed && isDone) {
    return (
      <div className="rounded-lg border border-[var(--border)] bg-[var(--background)] overflow-hidden">
        {/* Header */}
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="w-full flex items-center gap-2 px-3 py-2 text-left text-sm hover:bg-[var(--muted)]/50 transition-colors"
        >
          <BarChart3 className={`w-4 h-4 flex-shrink-0 ${style.color}`} />
          <span className="font-medium">{chartEmbed.title || formatToolName(toolCall.name)}</span>
          <Check className="w-3.5 h-3.5 text-green-500" />
          <span className="flex-1 text-xs text-[var(--muted-foreground)] truncate">
            {chartEmbed.time_range ? `Last ${chartEmbed.time_range}` : ''}
          </span>
          {isExpanded ? (
            <ChevronDown className="w-4 h-4 text-[var(--muted-foreground)]" />
          ) : (
            <ChevronRight className="w-4 h-4 text-[var(--muted-foreground)]" />
          )}
        </button>

        {/* Chart embed */}
        {isExpanded && (
          <div className="border-t border-[var(--border)]">
            {chartEmbed.dashboard_uid && chartEmbed.panel_id ? (
              <GrafanaEmbed
                dashboardUid={chartEmbed.dashboard_uid}
                panelId={chartEmbed.panel_id}
                title=""
                height={300}
                from={`now-${chartEmbed.time_range || '1h'}`}
                to="now"
                source={`toolcall-${toolCall.id}`}
              />
            ) : (
              // Fallback to direct iframe if only embed_url is provided
              <iframe
                src={chartEmbed.embed_url}
                width="100%"
                height={300}
                frameBorder="0"
                className="bg-[var(--background)]"
              />
            )}

            {/* Show SQL query if available */}
            {chartEmbed.sql_query && (
              <div className="px-3 py-2 border-t border-[var(--border)]">
                <details className="text-xs">
                  <summary className="cursor-pointer text-[var(--muted-foreground)] hover:text-[var(--foreground)]">
                    View SQL Query
                  </summary>
                  <pre className="mt-2 font-mono bg-[var(--muted)] p-2 rounded overflow-x-auto">
                    {chartEmbed.sql_query}
                  </pre>
                </details>
              </div>
            )}
          </div>
        )}
      </div>
    )
  }

  // Default tool call rendering
  return (
    <div className="rounded-lg border border-[var(--border)] bg-[var(--background)] overflow-hidden">
      {/* Header */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center gap-2 px-3 py-2 text-left text-sm hover:bg-[var(--muted)]/50 transition-colors"
      >
        <Wrench className={`w-4 h-4 flex-shrink-0 ${style.color}`} />
        <span className="font-medium">{formatToolName(toolCall.name)}</span>
        {isDone ? (
          <Check className="w-3.5 h-3.5 text-green-500" />
        ) : (
          <Loader2 className="w-3.5 h-3.5 animate-spin text-[var(--muted-foreground)]" />
        )}
        <span className="flex-1 text-xs text-[var(--muted-foreground)] truncate">
          {argsPreview}
        </span>
        {isExpanded ? (
          <ChevronDown className="w-4 h-4 text-[var(--muted-foreground)]" />
        ) : (
          <ChevronRight className="w-4 h-4 text-[var(--muted-foreground)]" />
        )}
      </button>

      {/* Expanded content */}
      {isExpanded && (
        <div className="px-3 pb-3 pt-1 border-t border-[var(--border)]">
          {/* Arguments */}
          <div className="mb-2">
            <div className="text-xs font-medium text-[var(--muted-foreground)] mb-1">
              Arguments
            </div>
            <pre className="text-xs font-mono bg-[var(--muted)] p-2 rounded overflow-x-auto">
              {JSON.stringify(toolCall.arguments, null, 2)}
            </pre>
          </div>

          {/* Result */}
          {toolCall.result && (
            <div>
              <div className="text-xs font-medium text-[var(--muted-foreground)] mb-1">
                Result
              </div>
              <pre className="text-xs font-mono bg-[var(--muted)] p-2 rounded overflow-x-auto max-h-48 overflow-y-auto">
                {typeof toolCall.result === 'string'
                  ? toolCall.result
                  : JSON.stringify(toolCall.result, null, 2)}
              </pre>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
