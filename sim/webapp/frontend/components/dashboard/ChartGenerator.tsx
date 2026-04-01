'use client'

import { useState } from 'react'
import { Sparkles, Loader2, X, BarChart3, LineChart, PieChart, Table2, Trash2 } from 'lucide-react'
import { LazyGrafanaEmbed } from './LazyGrafanaEmbed'
import { buildApiUrl, normalizeApiBase } from '@/lib/api'

const API_BASE = normalizeApiBase(process.env.NEXT_PUBLIC_API_URL || 'http://127.0.0.1:8000')

type ChartType = 'timeseries' | 'barchart' | 'piechart' | 'table'

interface GeneratedChart {
  panelId: number
  dashboardUid: string
  embedUrl: string
  sqlQuery: string
  title: string
}

interface ChartGeneratorProps {
  theme?: 'light' | 'dark'
}

const CHART_TYPES: { value: ChartType; label: string; icon: typeof LineChart }[] = [
  { value: 'timeseries', label: 'Time Series', icon: LineChart },
  { value: 'barchart', label: 'Bar Chart', icon: BarChart3 },
  { value: 'piechart', label: 'Pie Chart', icon: PieChart },
  { value: 'table', label: 'Table', icon: Table2 },
]

const EXAMPLE_PROMPTS = [
  'Show wait time by type over the last hour',
  'Top 10 queries by CPU time',
  'Memory grant status distribution',
  'Blocking sessions count over time',
  'File I/O latency by database',
]

export function ChartGenerator({ theme }: ChartGeneratorProps) {
  const [prompt, setPrompt] = useState('')
  const [chartType, setChartType] = useState<ChartType>('timeseries')
  const [isGenerating, setIsGenerating] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [generatedCharts, setGeneratedCharts] = useState<GeneratedChart[]>([])
  const [showSql, setShowSql] = useState<number | null>(null)

  const handleGenerate = async () => {
    const trimmedPrompt = prompt.trim()
    if (!trimmedPrompt) return

    setIsGenerating(true)
    setError(null)

    try {
      const response = await fetch(buildApiUrl('/api/grafana/generate', API_BASE), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          prompt: trimmedPrompt,
          chart_type: chartType,
          time_range: '1h',
        }),
      })

      const data = await response.json()
      if (!response.ok || !data.success) {
        throw new Error(data.error || 'Failed to generate chart')
      }

      setGeneratedCharts((prev) => [
        {
          panelId: data.panel_id,
          dashboardUid: data.dashboard_uid,
          embedUrl: data.embed_url,
          sqlQuery: data.sql_query,
          title: trimmedPrompt.slice(0, 70) + (trimmedPrompt.length > 70 ? '...' : ''),
        },
        ...prev,
      ])
      setPrompt('')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to connect to API')
    } finally {
      setIsGenerating(false)
    }
  }

  const handleRemoveChart = (index: number) => {
    setGeneratedCharts((prev) => prev.filter((_, i) => i !== index))
    if (showSql === index) setShowSql(null)
  }

  const handleClearCharts = () => {
    setGeneratedCharts([])
    setShowSql(null)
  }

  return (
    <section className="space-y-6">
      <div className="rounded-xl border border-[var(--border)] bg-[var(--card)] p-4">
        <div className="mb-4 flex items-center gap-2">
          <Sparkles className="h-5 w-5 text-[var(--accent)]" />
          <h3 className="font-heading text-lg font-semibold">Generate Chart from Prompt</h3>
        </div>

        <div className="space-y-4">
          <div>
            <label htmlFor="chart-prompt" className="mb-2 block text-sm text-[var(--muted-foreground)]">
              Describe the chart you want to create
            </label>
            <textarea
              id="chart-prompt"
              value={prompt}
              onChange={(e) => setPrompt(e.target.value)}
              placeholder="Show top wait categories over the last 60 minutes"
              className="w-full resize-none rounded-lg border border-[var(--border)] bg-[var(--background)] px-3 py-2 text-sm focus:border-transparent focus:outline-none focus:ring-2 focus:ring-[var(--accent)]"
              rows={2}
              disabled={isGenerating}
            />
          </div>

          <div className="flex flex-wrap gap-2">
            {EXAMPLE_PROMPTS.map((example) => (
              <button
                key={example}
                onClick={() => setPrompt(example)}
                disabled={isGenerating}
                className="rounded-full border border-[var(--border)] bg-[var(--surface)] px-3 py-1 text-xs text-[var(--muted-foreground)] transition-colors hover:bg-[var(--muted)] hover:text-[var(--foreground)] disabled:opacity-50"
              >
                {example}
              </button>
            ))}
          </div>

          <div>
            <span className="mb-2 block text-sm text-[var(--muted-foreground)]">Chart Type</span>
            <div className="flex flex-wrap gap-2">
              {CHART_TYPES.map((type) => {
                const Icon = type.icon
                return (
                  <button
                    key={type.value}
                    onClick={() => setChartType(type.value)}
                    disabled={isGenerating}
                    className={`inline-flex items-center gap-2 rounded-lg px-3 py-2 text-sm transition-colors ${
                      chartType === type.value
                        ? 'bg-[var(--accent)] text-[var(--accent-foreground)]'
                        : 'bg-[var(--muted)] text-[var(--muted-foreground)] hover:text-[var(--foreground)]'
                    }`}
                    aria-pressed={chartType === type.value}
                  >
                    <Icon className="h-4 w-4" />
                    {type.label}
                  </button>
                )
              })}
            </div>
          </div>

          {error && (
            <div className="rounded-lg border border-[var(--danger)]/30 bg-[var(--danger)]/10 p-3 text-sm text-[var(--danger)]">
              {error}
            </div>
          )}

          <button
            onClick={handleGenerate}
            disabled={!prompt.trim() || isGenerating}
            className={`inline-flex w-full items-center justify-center gap-2 rounded-lg px-4 py-2 font-medium transition-opacity ${
              !prompt.trim() || isGenerating
                ? 'cursor-not-allowed bg-[var(--muted)] text-[var(--muted-foreground)]'
                : 'bg-[var(--accent)] text-[var(--accent-foreground)] hover:opacity-90'
            }`}
          >
            {isGenerating ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                Generating...
              </>
            ) : (
              <>
                <Sparkles className="h-4 w-4" />
                Generate Chart
              </>
            )}
          </button>
        </div>
      </div>

      {generatedCharts.length > 0 && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="font-heading text-lg font-semibold">Generated Charts</h3>
            <button
              onClick={handleClearCharts}
              className="inline-flex items-center gap-1 rounded-md border border-[var(--border)] px-2 py-1 text-xs text-[var(--muted-foreground)] transition-colors hover:bg-[var(--muted)] hover:text-[var(--foreground)]"
            >
              <Trash2 className="h-3.5 w-3.5" />
              Clear
            </button>
          </div>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            {generatedCharts.map((chart, index) => (
              <article
                key={`${chart.dashboardUid}-${chart.panelId}-${index}`}
                className="overflow-hidden rounded-xl border border-[var(--border)] bg-[var(--card)]"
              >
                <header className="flex items-center justify-between border-b border-[var(--border)] px-4 py-2">
                  <h4 className="truncate text-sm font-medium">{chart.title}</h4>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => setShowSql(showSql === index ? null : index)}
                      className="text-xs text-[var(--muted-foreground)] hover:text-[var(--foreground)]"
                    >
                      {showSql === index ? 'Hide SQL' : 'Show SQL'}
                    </button>
                    <button
                      onClick={() => handleRemoveChart(index)}
                      className="rounded p-1 text-[var(--muted-foreground)] hover:bg-[var(--muted)] hover:text-[var(--foreground)]"
                      aria-label="Remove generated chart"
                    >
                      <X className="h-4 w-4" />
                    </button>
                  </div>
                </header>

                {showSql === index && (
                  <div className="border-b border-[var(--border)] bg-[var(--muted)] px-4 py-3">
                    <pre className="overflow-x-auto whitespace-pre-wrap text-xs text-[var(--muted-foreground)]">
                      {chart.sqlQuery}
                    </pre>
                  </div>
                )}

                <LazyGrafanaEmbed
                  dashboardUid={chart.dashboardUid}
                  panelId={chart.panelId}
                  title={chart.title}
                  height={300}
                  from="now-1h"
                  to="now"
                  theme={theme}
                  source={`chart-generator-${index}-${chart.dashboardUid}-${chart.panelId}`}
                />
              </article>
            ))}
          </div>
        </div>
      )}
    </section>
  )
}
