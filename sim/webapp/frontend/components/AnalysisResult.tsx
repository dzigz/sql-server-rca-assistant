'use client'

import { useState } from 'react'
import {
  AlertTriangle,
  CheckCircle,
  XCircle,
  AlertCircle,
  ArrowRight,
  Shield,
  Activity,
  ChevronDown,
  ChevronRight
} from 'lucide-react'

interface RCAResult {
  summary?: string  // Top-level summary
  root_cause?: {
    category?: string
    summary?: string
    confidence?: number
    entity?: string
  }
  causal_chain?: Array<{
    event?: string
    description?: string
  }>
  evidence?: Array<{
    source?: string
    finding?: string
  }>
  mitigation?: string[]
  prevention?: string[]
}

interface HealthCheckResult {
  health_score?: number
  summary?: string
  critical_findings?: Array<{
    finding?: string
    impact?: string
    remediation?: string
  }>
  warnings?: Array<{
    finding?: string
    impact?: string
    remediation?: string
  }>
  recommendations?: Array<{
    finding?: string
    impact?: string
    remediation?: string
  }>
  healthy_areas?: string[]
}

interface AnalysisResultProps {
  content: string
}

function tryParseJSON(content: string): RCAResult | HealthCheckResult | null {
  const trimmed = content.trim()

  // Check if content is a markdown code block containing JSON
  const jsonCodeBlockMatch = trimmed.match(/^```(?:json)?\s*([\s\S]*?)```\s*$/)

  // Only proceed if:
  // 1. The entire content is a JSON code block (nothing before or after), OR
  // 2. The entire content starts with { and ends with } (raw JSON)
  let jsonStr: string | null = null

  if (jsonCodeBlockMatch) {
    // Entire content is a code block
    jsonStr = jsonCodeBlockMatch[1].trim()
  } else if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
    // Entire content is raw JSON object
    jsonStr = trimmed
  }

  if (!jsonStr) {
    return null
  }

  try {
    const parsed = JSON.parse(jsonStr)
    // Check if it looks like an analysis result
    if (
      parsed.root_cause ||
      parsed.health_score !== undefined ||
      parsed.causal_chain ||
      parsed.critical_findings ||
      parsed.evidence ||
      parsed.mitigation ||
      parsed.prevention ||
      parsed.warnings ||
      parsed.recommendations ||
      parsed.healthy_areas
    ) {
      return parsed
    }
  } catch {
    // Not valid JSON
  }
  return null
}

function isHealthCheck(result: RCAResult | HealthCheckResult): result is HealthCheckResult {
  return 'health_score' in result || 'critical_findings' in result || 'healthy_areas' in result
}

function HealthScoreBadge({ score }: { score: number }) {
  const color = score >= 70 ? 'text-green-500 bg-green-500/10' :
                score >= 50 ? 'text-yellow-500 bg-yellow-500/10' :
                'text-red-500 bg-red-500/10'
  return (
    <div className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-full ${color} font-semibold`}>
      <Activity className="w-4 h-4" />
      Health Score: {score}/100
    </div>
  )
}

function ConfidenceBadge({ confidence }: { confidence: number }) {
  const pct = typeof confidence === 'number' && confidence <= 1 ? Math.round(confidence * 100) : confidence
  const color = pct >= 70 ? 'text-green-500 bg-green-500/10' :
                pct >= 50 ? 'text-yellow-500 bg-yellow-500/10' :
                'text-red-500 bg-red-500/10'
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-sm ${color}`}>
      {pct}% confidence
    </span>
  )
}

function Section({ title, icon: Icon, children, defaultOpen = true }: {
  title: string
  icon: React.ElementType
  children: React.ReactNode
  defaultOpen?: boolean
}) {
  const [isOpen, setIsOpen] = useState(defaultOpen)

  return (
    <div className="border border-[var(--border)] rounded-lg overflow-hidden">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="w-full flex items-center gap-3 px-4 py-3 bg-[var(--muted)]/30 hover:bg-[var(--muted)]/50 transition-colors text-left"
      >
        <Icon className="w-5 h-5" />
        <span className="font-semibold text-base flex-1">{title}</span>
        {isOpen ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
      </button>
      {isOpen && (
        <div className="px-4 py-4 space-y-3">
          {children}
        </div>
      )}
    </div>
  )
}

function RCAResultDisplay({ result }: { result: RCAResult }) {
  const rootCause = result.root_cause || {}

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3 pb-4 mb-2 border-b border-[var(--border)]">
        <div className="p-2.5 rounded-lg bg-green-500/10">
          <CheckCircle className="w-6 h-6 text-green-500" />
        </div>
        <div>
          <h2 className="font-bold text-xl">RCA Analysis Complete</h2>
          {rootCause.confidence && (
            <ConfidenceBadge confidence={rootCause.confidence} />
          )}
        </div>
      </div>

      {/* Top-level Summary */}
      {result.summary && (
        <p className="text-[var(--muted-foreground)]">{result.summary}</p>
      )}

      {/* Root Cause */}
      {rootCause.summary && (
        <div className="p-4 rounded-lg bg-[var(--muted)]/50 border border-[var(--border)]">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-xs font-medium uppercase text-[var(--muted-foreground)]">Root Cause</span>
            {rootCause.category && (
              <span className="px-2 py-0.5 text-xs rounded bg-[var(--accent)]/10 text-[var(--accent)]">
                {rootCause.category.replace(/_/g, ' ')}
              </span>
            )}
          </div>
          <p className="font-medium">{rootCause.summary}</p>
          {rootCause.entity && (
            <p className="text-sm text-[var(--muted-foreground)] mt-1">
              Affected: {rootCause.entity}
            </p>
          )}
        </div>
      )}

      {/* Causal Chain */}
      {result.causal_chain && result.causal_chain.length > 0 && (
        <Section title="Causal Chain" icon={ArrowRight}>
          <div className="space-y-3">
            {result.causal_chain.map((step, i) => (
              <div key={i} className="flex gap-3">
                <div className="flex-shrink-0 w-6 h-6 rounded-full bg-[var(--accent)]/10 text-[var(--accent)] flex items-center justify-center text-sm font-medium">
                  {i + 1}
                </div>
                <div>
                  {step.event && (
                    <span className="font-medium text-[var(--accent)]">{step.event}: </span>
                  )}
                  <span>{step.description}</span>
                </div>
              </div>
            ))}
          </div>
        </Section>
      )}

      {/* Evidence */}
      {result.evidence && result.evidence.length > 0 && (
        <Section title="Key Evidence" icon={AlertCircle} defaultOpen={false}>
          <ul className="space-y-2">
            {result.evidence.slice(0, 5).map((ev, i) => (
              <li key={i} className="flex gap-2 text-sm">
                <span className="text-[var(--muted-foreground)]">[{ev.source}]</span>
                <span>{ev.finding}</span>
              </li>
            ))}
          </ul>
        </Section>
      )}

      {/* Mitigation */}
      {result.mitigation && result.mitigation.length > 0 && (
        <Section title="Mitigation Steps" icon={Shield}>
          <ul className="space-y-1.5">
            {result.mitigation.map((m, i) => (
              <li key={i} className="flex gap-2 text-sm">
                <ArrowRight className="w-4 h-4 flex-shrink-0 text-[var(--accent)]" />
                <span>{m}</span>
              </li>
            ))}
          </ul>
        </Section>
      )}

      {/* Prevention */}
      {result.prevention && result.prevention.length > 0 && (
        <Section title="Prevention" icon={Shield} defaultOpen={false}>
          <ul className="space-y-1.5">
            {result.prevention.map((p, i) => (
              <li key={i} className="flex gap-2 text-sm">
                <ArrowRight className="w-4 h-4 flex-shrink-0 text-green-500" />
                <span>{p}</span>
              </li>
            ))}
          </ul>
        </Section>
      )}
    </div>
  )
}

function HealthCheckResultDisplay({ result }: { result: HealthCheckResult }) {
  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between pb-4 mb-2 border-b border-[var(--border)]">
        <div className="flex items-center gap-3">
          <div className="p-2.5 rounded-lg bg-blue-500/10">
            <Activity className="w-6 h-6 text-blue-500" />
          </div>
          <h2 className="font-bold text-xl">Health Check Complete</h2>
        </div>
        {result.health_score !== undefined && (
          <HealthScoreBadge score={result.health_score} />
        )}
      </div>

      {/* Summary */}
      {result.summary && (
        <p className="text-[var(--muted-foreground)]">{result.summary}</p>
      )}

      {/* Critical Findings */}
      {result.critical_findings && result.critical_findings.length > 0 && (
        <Section title={`Critical Findings (${result.critical_findings.length})`} icon={XCircle}>
          <div className="space-y-3">
            {result.critical_findings.map((f, i) => (
              <div key={i} className="p-3 rounded-lg bg-red-500/5 border border-red-500/20">
                <div className="flex items-start gap-2">
                  <XCircle className="w-4 h-4 text-red-500 flex-shrink-0 mt-0.5" />
                  <div>
                    <p className="font-medium text-red-600 dark:text-red-400">{f.finding}</p>
                    {f.impact && <p className="text-sm text-[var(--muted-foreground)] mt-1">{f.impact}</p>}
                    {f.remediation && (
                      <p className="text-sm mt-2">
                        <span className="font-medium">Fix: </span>{f.remediation}
                      </p>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </Section>
      )}

      {/* Warnings */}
      {result.warnings && result.warnings.length > 0 && (
        <Section title={`Warnings (${result.warnings.length})`} icon={AlertTriangle} defaultOpen={false}>
          <div className="space-y-2">
            {result.warnings.map((w, i) => (
              <div key={i} className="flex items-start gap-2 p-2 rounded bg-yellow-500/5">
                <AlertTriangle className="w-4 h-4 text-yellow-500 flex-shrink-0 mt-0.5" />
                <div>
                  <p className="text-sm">{w.finding}</p>
                  {w.remediation && (
                    <p className="text-xs text-[var(--muted-foreground)] mt-1">{w.remediation}</p>
                  )}
                </div>
              </div>
            ))}
          </div>
        </Section>
      )}

      {/* Recommendations */}
      {result.recommendations && result.recommendations.length > 0 && (
        <Section title="Recommendations" icon={ArrowRight} defaultOpen={false}>
          <ul className="space-y-1.5">
            {result.recommendations.map((r, i) => (
              <li key={i} className="flex gap-2 text-sm">
                <ArrowRight className="w-4 h-4 flex-shrink-0 text-[var(--accent)]" />
                <span>{typeof r === 'string' ? r : r.finding}</span>
              </li>
            ))}
          </ul>
        </Section>
      )}

      {/* Healthy Areas */}
      {result.healthy_areas && result.healthy_areas.length > 0 && (
        <Section title="Healthy Areas" icon={CheckCircle} defaultOpen={false}>
          <ul className="space-y-1.5">
            {result.healthy_areas.map((h, i) => (
              <li key={i} className="flex gap-2 text-sm">
                <CheckCircle className="w-4 h-4 flex-shrink-0 text-green-500" />
                <span>{h}</span>
              </li>
            ))}
          </ul>
        </Section>
      )}
    </div>
  )
}

export function AnalysisResult({ content }: AnalysisResultProps) {
  const result = tryParseJSON(content)

  if (!result) {
    return null // Not a JSON result, render normally
  }

  if (isHealthCheck(result)) {
    return <HealthCheckResultDisplay result={result} />
  } else {
    return <RCAResultDisplay result={result as RCAResult} />
  }
}

// Helper to check if content contains an analysis result
export function isAnalysisResult(content: string): boolean {
  return tryParseJSON(content) !== null
}
