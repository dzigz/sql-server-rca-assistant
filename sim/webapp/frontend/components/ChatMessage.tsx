'use client'

import { Loader2, FileText } from 'lucide-react'
import { useState } from 'react'
import { ThinkingBlock } from './ThinkingBlock'
import { ToolCall } from './ToolCall'
import { AnalysisResult, isAnalysisResult } from './AnalysisResult'
import { GrafanaEmbed } from './dashboard/GrafanaEmbed'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { useMemo, useRef, memo } from 'react'

interface ToolCallData {
  id: string
  name: string
  arguments: Record<string, unknown>
  result?: string
  completed?: boolean
}

type BlitzInstallStatus = 'idle' | 'installing' | 'installed' | 'declined' | 'error'

interface BlitzInstallState {
  status: BlitzInstallStatus
  message?: string
}

interface MessageBlock {
  type: 'thinking' | 'tool' | 'text'
  id: string
  content?: string
  toolCall?: ToolCallData
  isComplete?: boolean
}

// File attachment in user messages
interface MessageFile {
  name: string
  type: 'csv' | 'image'
  mediaType: string
  data: string  // base64 for images, raw text for CSV
}

interface Message {
  id: string
  role: 'user' | 'assistant' | 'system'
  content: string
  blocks: MessageBlock[]
  isStreaming?: boolean
  files?: MessageFile[]  // Attached files (for user messages)
}

interface ChatMessageProps {
  message: Message
  hideInternalBlocks?: boolean
  sessionId?: string | null
  blitzInstallState?: BlitzInstallState
  onInstallBlitz?: () => Promise<void>
  onDeclineBlitz?: () => Promise<void>
}

// Chart name to dashboard/panel mapping
const CHART_CONFIG: Record<string, { dashboardUid: string; panelId: number; title: string }> = {
  wait_time_trend: { dashboardUid: 'sql-server-overview', panelId: 1, title: 'Wait Time by Type' },
  blocked_sessions: { dashboardUid: 'sql-server-overview', panelId: 2, title: 'Blocked Sessions' },
  active_requests: { dashboardUid: 'sql-server-overview', panelId: 3, title: 'Active Requests' },
  memory_grants: { dashboardUid: 'sql-server-overview', panelId: 4, title: 'Memory Grant Status' },
  top_waits: { dashboardUid: 'sql-server-overview', panelId: 5, title: 'Top 10 Wait Types' },
  wait_stats_trend: { dashboardUid: 'wait-stats', panelId: 1, title: 'Wait Time Trend by Type' },
  wait_by_category: { dashboardUid: 'wait-stats', panelId: 2, title: 'Wait Time by Category' },
  top_queries_cpu: { dashboardUid: 'query-performance', panelId: 1, title: 'Top 10 Queries by CPU' },
  top_queries_reads: { dashboardUid: 'query-performance', panelId: 2, title: 'Top 10 Queries by Logical Reads' },
  query_details: { dashboardUid: 'query-performance', panelId: 3, title: 'Query Details Table' },
}

type ChartEmbedRef =
  | { kind: 'chartName'; chartName: keyof typeof CHART_CONFIG }
  | { kind: 'grafana'; dashboardUid: string; panelId?: number; from?: string; to?: string }

function getChartEmbedRef(src?: string | null): ChartEmbedRef | null {
  if (!src) return null

  const trimmed = src.trim()
  if (trimmed.toLowerCase().startsWith('chart:')) {
    const chartName = trimmed.slice('chart:'.length).trim().toLowerCase()
    if (chartName in CHART_CONFIG) {
      return { kind: 'chartName', chartName: chartName as keyof typeof CHART_CONFIG }
    }
  }

  try {
    const url = new URL(trimmed)
    const segments = url.pathname.split('/').filter(Boolean)
    const soloIndex = segments.indexOf('d-solo')
    const dashboardIndex = segments.indexOf('d')
    const uidIndex = soloIndex >= 0 ? soloIndex + 1 : dashboardIndex >= 0 ? dashboardIndex + 1 : -1
    if (uidIndex > 0 && segments.length > uidIndex) {
      const dashboardUid = segments[uidIndex]
      const panelId = url.searchParams.get('panelId')
      return {
        kind: 'grafana',
        dashboardUid,
        panelId: panelId ? Number(panelId) : undefined,
        from: url.searchParams.get('from') || undefined,
        to: url.searchParams.get('to') || undefined,
      }
    }
  } catch {
    return null
  }

  return null
}

// Check if a tool result contains a chart embed
function isChartEmbed(result: string | undefined): boolean {
  if (!result) return false
  try {
    const parsed = JSON.parse(result)
    const data = parsed.data || parsed
    return data.type === 'chart_embed' && data.embed_url
  } catch {
    return false
  }
}

// Segment type for splitting content at chart positions
type ContentSegment =
  | { type: 'text'; content: string }
  | { type: 'grafana'; key: string; dashboardUid: string; panelId?: number; from?: string; to?: string; alt?: string }
  | { type: 'chartName'; chartName: keyof typeof CHART_CONFIG }

// Split markdown content into segments at Grafana chart positions
// This allows rendering charts outside ReactMarkdown while keeping them inline
function splitContentAtCharts(text: string): ContentSegment[] {
  const segments: ContentSegment[] = []
  let chartIndex = 0  // Unique index for each chart occurrence

  // Pattern to match markdown images: ![alt](url)
  const imgPattern = /!\[([^\]]*)\]\(([^)]+)\)/g

  let lastIndex = 0
  let match

  while ((match = imgPattern.exec(text)) !== null) {
    const [fullMatch, alt, url] = match
    const chartRef = getChartEmbedRef(url)

    if (chartRef) {
      // Add text before this chart (if any)
      if (match.index > lastIndex) {
        const textContent = text.slice(lastIndex, match.index)
        if (textContent.trim()) {
          segments.push({ type: 'text', content: textContent })
        }
      }

      // Add the chart segment (each occurrence gets a unique key)
      if (chartRef.kind === 'grafana') {
        segments.push({
          type: 'grafana',
          key: `${chartRef.dashboardUid}-${chartRef.panelId ?? 'full'}-${chartIndex++}`,
          dashboardUid: chartRef.dashboardUid,
          panelId: chartRef.panelId,
          from: chartRef.from,
          to: chartRef.to,
          alt,
        })
      } else if (chartRef.kind === 'chartName') {
        segments.push({ type: 'chartName', chartName: chartRef.chartName })
      }

      lastIndex = match.index + fullMatch.length
    }
  }

  // Add remaining text after last chart
  if (lastIndex < text.length) {
    const textContent = text.slice(lastIndex)
    if (textContent.trim()) {
      segments.push({ type: 'text', content: textContent })
    }
  }

  // If no charts found, return original text
  if (segments.length === 0 && text.trim()) {
    segments.push({ type: 'text', content: text })
  }

  return segments
}

// Memoized to prevent re-renders of completed messages during streaming
export const ChatMessage = memo(function ChatMessage({
  message,
  hideInternalBlocks = false,
  sessionId,
  blitzInstallState,
  onInstallBlitz,
  onDeclineBlitz,
}: ChatMessageProps) {
  const isUser = message.role === 'user'

  // Filter blocks based on hideInternalBlocks setting
  // Keep: text blocks, AND tool blocks that have chart embeds (even when hiding internals)
  const visibleBlocks = useMemo(() => {
    if (!hideInternalBlocks) return message.blocks

    return message.blocks?.filter(b => {
      if (b.type === 'text') return true
      // Keep tool blocks that have chart embeds
      if (b.type === 'tool' && b.toolCall?.result && isChartEmbed(b.toolCall.result)) {
        return true
      }
      return false
    })
  }, [message.blocks, hideInternalBlocks])

  // Cache content segments per block to render charts outside ReactMarkdown
  // This prevents iframe reload when ReactMarkdown recreates its component tree
  type SegmentCache = Record<string, {
    content: string
    segments: ContentSegment[]
  }>
  const segmentCacheRef = useRef<SegmentCache>({})

  const blockSegments = useMemo(() => {
    const cache = segmentCacheRef.current
    const result: Record<string, ContentSegment[]> = {}

    for (const block of message.blocks || []) {
      if (block.type === 'text') {
        const content = block.content || ''
        const cached = cache[block.id]

        // Reuse cached segments if content hasn't changed
        if (cached && cached.content === content) {
          result[block.id] = cached.segments
        } else {
          const segments = splitContentAtCharts(content)
          cache[block.id] = { content, segments }
          result[block.id] = segments
        }
      }
    }

    return result
  }, [message.blocks])

  // Check if we're processing but have nothing visible to show
  const isProcessingHidden = useMemo(() => {
    if (!hideInternalBlocks || !message.isStreaming) return false

    // Check if there are any non-complete thinking or tool blocks
    const hasHiddenProcessing = message.blocks?.some(b => {
      if (b.type === 'thinking' && !b.isComplete) return true
      if (b.type === 'tool' && !b.isComplete && !isChartEmbed(b.toolCall?.result)) return true
      return false
    })

    // Only show indicator if we have hidden processing AND no visible text blocks streaming
    const hasVisibleTextStreaming = visibleBlocks?.some(b => b.type === 'text' && !b.isComplete)

    return hasHiddenProcessing && !hasVisibleTextStreaming
  }, [message.blocks, message.isStreaming, hideInternalBlocks, visibleBlocks])

  // State for expanded image modal
  const [expandedImage, setExpandedImage] = useState<MessageFile | null>(null)

  // User messages: right-aligned blue bubble
  if (isUser) {
    return (
      <>
        {/* Image modal */}
        {expandedImage && (
          <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-black/80"
            onClick={() => setExpandedImage(null)}
          >
            <img
              src={`data:${expandedImage.mediaType};base64,${expandedImage.data}`}
              alt={expandedImage.name}
              className="max-w-[90vw] max-h-[90vh] object-contain rounded-lg"
            />
          </div>
        )}

        <div className="py-4 flex justify-end">
          <div className="max-w-[80%]">
            <div className="rounded-2xl border border-[var(--border)] bg-[var(--accent)]/10 px-4 py-2">
              {/* Message text */}
              {message.content && (
                <div className="prose prose-sm dark:prose-invert max-w-none">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>{message.content}</ReactMarkdown>
                </div>
              )}

              {/* Attached files */}
              {message.files && message.files.length > 0 && (
                <div className={`flex flex-wrap gap-2 ${message.content ? 'mt-2' : ''}`}>
                  {message.files.map((file, idx) => (
                    file.type === 'image' ? (
                      <img
                        key={idx}
                        src={`data:${file.mediaType};base64,${file.data}`}
                        alt={file.name}
                        className="max-w-[200px] max-h-[200px] object-cover rounded-lg cursor-pointer hover:opacity-80 transition-opacity"
                        onClick={() => setExpandedImage(file)}
                      />
                    ) : (
                      <div
                        key={idx}
                        className="flex items-center gap-1.5 rounded-lg border border-[var(--accent)]/30 bg-[var(--accent)]/12 px-2.5 py-1.5"
                      >
                        <FileText className="w-4 h-4 text-[var(--accent)]" />
                        <span className="text-sm font-medium text-[var(--foreground)]">{file.name}</span>
                      </div>
                    )
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      </>
    )
  }

  // Assistant messages: left-aligned
  return (
    <div className="py-4">
      <div className="max-w-[90%]">
        {/* Assistant message - render blocks in order */}
        {(
            <>
              {/* Render blocks in the order they appeared */}
              {visibleBlocks && visibleBlocks.length > 0 && (
                <div className="space-y-4 mb-4">
                  {visibleBlocks.map((block) => {
                    switch (block.type) {
                      case 'thinking':
                        return (
                          <ThinkingBlock
                            key={block.id}
                            content={block.content || ''}
                            isStreaming={!block.isComplete && message.isStreaming}
                          />
                        )
                      case 'tool':
                        return block.toolCall ? (
                          <ToolCall
                            key={block.id}
                            toolCall={block.toolCall}
                            isComplete={block.isComplete || !message.isStreaming}
                            sessionId={sessionId}
                            blitzInstallState={blitzInstallState}
                            onInstallBlitz={onInstallBlitz}
                            onDeclineBlitz={onDeclineBlitz}
                          />
                        ) : null
                      case 'text':
                        // Get segments (text and charts) for this block
                        const segments = blockSegments[block.id] || []

                        // Markdown components for text segments (no chart handling needed - charts are separate)
                        const markdownComponents = {
                          h1: ({ children }: { children: React.ReactNode }) => (
                            <h1 className="text-2xl font-bold mt-6 mb-4">{children}</h1>
                          ),
                          h2: ({ children }: { children: React.ReactNode }) => (
                            <h2 className="text-xl font-bold mt-5 mb-3">{children}</h2>
                          ),
                          h3: ({ children }: { children: React.ReactNode }) => (
                            <h3 className="text-lg font-semibold mt-4 mb-2">{children}</h3>
                          ),
                          h4: ({ children }: { children: React.ReactNode }) => (
                            <h4 className="text-base font-semibold mt-3 mb-2">{children}</h4>
                          ),
                          p: ({ children }: { children: React.ReactNode }) => (
                            <p className="mb-3 leading-relaxed">{children}</p>
                          ),
                          ul: ({ children }: { children: React.ReactNode }) => (
                            <ul className="list-disc pl-6 mb-3 space-y-1">{children}</ul>
                          ),
                          ol: ({ children }: { children: React.ReactNode }) => (
                            <ol className="list-decimal pl-6 mb-3 space-y-1">{children}</ol>
                          ),
                          li: ({ children }: { children: React.ReactNode }) => (
                            <li className="leading-relaxed">{children}</li>
                          ),
                          pre: ({ children }: { children: React.ReactNode }) => {
                            const codeContent = String((children as any)?.props?.children || '')
                            if (isAnalysisResult(codeContent)) {
                              return <AnalysisResult content={codeContent} />
                            }
                            return (
                              <pre className="bg-[var(--muted)] p-3 rounded-lg overflow-x-auto text-sm font-mono my-3">
                                {children}
                              </pre>
                            )
                          },
                          code: ({ className, children, ...props }: { className?: string; children: React.ReactNode }) => {
                            const isInline = !className
                            if (isInline) {
                              return (
                                <code className="bg-[var(--muted)] px-1.5 py-0.5 rounded text-sm font-mono" {...props}>
                                  {children}
                                </code>
                              )
                            }
                            const codeContent = String(children || '')
                            if (isAnalysisResult(codeContent)) {
                              return <AnalysisResult content={codeContent} />
                            }
                            return (
                              <code className={className} {...props}>
                                {children}
                              </code>
                            )
                          },
                          table: ({ children }: { children: React.ReactNode }) => (
                            <div className="overflow-x-auto my-4">
                              <table className="min-w-full border-collapse border border-[var(--border)] text-sm">
                                {children}
                              </table>
                            </div>
                          ),
                          thead: ({ children }: { children: React.ReactNode }) => (
                            <thead className="bg-[var(--muted)]">{children}</thead>
                          ),
                          th: ({ children }: { children: React.ReactNode }) => (
                            <th className="border border-[var(--border)] px-3 py-2 text-left font-medium">
                              {children}
                            </th>
                          ),
                          td: ({ children }: { children: React.ReactNode }) => (
                            <td className="border border-[var(--border)] px-3 py-2">{children}</td>
                          ),
                          strong: ({ children }: { children: React.ReactNode }) => (
                            <strong className="font-semibold">{children}</strong>
                          ),
                          blockquote: ({ children }: { children: React.ReactNode }) => (
                            <blockquote className="border-l-4 border-[var(--border)] pl-4 my-3 italic text-[var(--muted-foreground)]">
                              {children}
                            </blockquote>
                          ),
                          // Regular images only - charts are handled as separate segments
                          img: ({ src, alt }: { src?: string; alt?: string }) => {
                            if (!src) return null
                            return (
                              <img
                                src={src}
                                alt={alt || ''}
                                className="max-w-full rounded-lg border border-[var(--border)]"
                              />
                            )
                          },
                        }

                        return (
                          <div key={block.id} className="prose prose-sm dark:prose-invert max-w-none">
                            {block.isComplete && isAnalysisResult(block.content || '') ? (
                              <AnalysisResult content={block.content || ''} />
                            ) : (
                              <>
                                {/* Render segments in order: text via ReactMarkdown, charts directly */}
                                {segments.map((segment, idx) => {
                                  if (segment.type === 'text') {
                                    return (
                                      <ReactMarkdown
                                        key={`${block.id}-text-${idx}`}
                                        remarkPlugins={[remarkGfm]}
                                        components={markdownComponents as any}
                                      >
                                        {segment.content}
                                      </ReactMarkdown>
                                    )
                                  }
                                  if (segment.type === 'grafana') {
                                    // Render Grafana charts OUTSIDE ReactMarkdown (stable, won't remount)
                                    return (
                                      <div key={segment.key} className="my-4">
                                        <GrafanaEmbed
                                          dashboardUid={segment.dashboardUid}
                                          panelId={segment.panelId}
                                          title={segment.alt || 'Grafana Chart'}
                                          height={300}
                                          from={segment.from || 'now-1h'}
                                          to={segment.to || 'now'}
                                          source={`segment-${block.id}-${segment.key}`}
                                        />
                                      </div>
                                    )
                                  }
                                  if (segment.type === 'chartName') {
                                    const config = CHART_CONFIG[segment.chartName]
                                    return (
                                      <div key={`${block.id}-chart-${segment.chartName}`} className="my-4">
                                        <GrafanaEmbed
                                          dashboardUid={config.dashboardUid}
                                          panelId={config.panelId}
                                          title={config.title}
                                          height={300}
                                          from="now-1h"
                                          to="now"
                                          source={`segment-${block.id}-${segment.chartName}`}
                                        />
                                      </div>
                                    )
                                  }
                                  return null
                                })}
                                {!block.isComplete && message.isStreaming && (
                                  <span className="inline-block w-2 h-4 bg-[var(--foreground)] cursor-blink ml-0.5" />
                                )}
                              </>
                            )}
                          </div>
                        )
                      default:
                        return null
                    }
                  })}
                </div>
              )}

              {/* Streaming indicator - show when no visible blocks yet */}
              {message.isStreaming && (!visibleBlocks || visibleBlocks.length === 0) && (
                <div className="flex items-center gap-2 text-[var(--muted-foreground)]">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  <span className="text-sm">Analyzing...</span>
                </div>
              )}

              {/* Pulsating dots when internals are hidden but processing is happening */}
              {isProcessingHidden && (
                <div className="flex items-center gap-1.5 py-2">
                  <div className="flex gap-1">
                    <span className="w-2 h-2 bg-[var(--muted-foreground)] rounded-full animate-pulse" style={{ animationDelay: '0ms' }} />
                    <span className="w-2 h-2 bg-[var(--muted-foreground)] rounded-full animate-pulse" style={{ animationDelay: '150ms' }} />
                    <span className="w-2 h-2 bg-[var(--muted-foreground)] rounded-full animate-pulse" style={{ animationDelay: '300ms' }} />
                  </div>
                  <span className="text-xs text-[var(--muted-foreground)] ml-1">Processing...</span>
                </div>
              )}

              {/* Fallback: render message.content if it exists but isn't in any text block */}
              {!message.isStreaming && message.content && !visibleBlocks?.some(b => b.type === 'text') && (
                <div className="prose prose-sm dark:prose-invert max-w-none">
                  {isAnalysisResult(message.content) ? (
                    <AnalysisResult content={message.content} />
                  ) : (
                    <ReactMarkdown
                      remarkPlugins={[remarkGfm]}
                      components={{
                        h1: ({ children }) => <h1 className="text-2xl font-bold mt-6 mb-4">{children}</h1>,
                        h2: ({ children }) => <h2 className="text-xl font-bold mt-5 mb-3">{children}</h2>,
                        h3: ({ children }) => <h3 className="text-lg font-semibold mt-4 mb-2">{children}</h3>,
                        h4: ({ children }) => <h4 className="text-base font-semibold mt-3 mb-2">{children}</h4>,
                        p: ({ children }) => <p className="mb-3 leading-relaxed">{children}</p>,
                        img: ({ src, alt }) => {
                          const chartRef = getChartEmbedRef(src)
                          if (chartRef?.kind === 'chartName') {
                            const config = CHART_CONFIG[chartRef.chartName]
                            return (
                              <div className="my-4">
                                <GrafanaEmbed
                                  dashboardUid={config.dashboardUid}
                                  panelId={config.panelId}
                                  title={config.title}
                                  height={300}
                                  from="now-1h"
                                  to="now"
                                  source={`fallback-img-chartName-${message.id}`}
                                />
                              </div>
                            )
                          }
                          if (chartRef?.kind === 'grafana') {
                            return (
                              <div className="my-4">
                                <GrafanaEmbed
                                  dashboardUid={chartRef.dashboardUid}
                                  panelId={chartRef.panelId}
                                  title={alt || 'Grafana Chart'}
                                  height={300}
                                  from={chartRef.from || 'now-1h'}
                                  to={chartRef.to || 'now'}
                                  source={`fallback-img-grafana-${message.id}`}
                                />
                              </div>
                            )
                          }
                          if (!src) return null
                          return (
                            <img
                              src={src}
                              alt={alt || ''}
                              className="max-w-full rounded-lg border border-[var(--border)]"
                            />
                          )
                        },
                      }}
                    >
                      {message.content}
                    </ReactMarkdown>
                  )}
                </div>
              )}
            </>
          )}
      </div>
    </div>
  )
})
