'use client'

import { useEffect, useState } from 'react'
import { GrafanaEmbed, DASHBOARDS, ChartGenerator, LazyGrafanaEmbed } from '@/components/dashboard'
import { BarChart3, Activity, Database, Clock, RefreshCw, ExternalLink, Sparkles, LayoutGrid, PanelTop } from 'lucide-react'

type DashboardKey = keyof typeof DASHBOARDS | 'generate'
type TimeRange = '15m' | '1h' | '6h' | '24h' | '7d'
type ViewMode = 'dashboard' | 'panels'

const TIME_RANGES: { value: TimeRange; label: string }[] = [
  { value: '15m', label: '15m' },
  { value: '1h', label: '1h' },
  { value: '6h', label: '6h' },
  { value: '24h', label: '24h' },
  { value: '7d', label: '7d' },
]

const GRAFANA_BASE_URL = process.env.NEXT_PUBLIC_GRAFANA_URL || 'http://localhost:3001'

export default function DashboardPage() {
  const [activeDashboard, setActiveDashboard] = useState<DashboardKey>('overview')
  const [timeRange, setTimeRange] = useState<TimeRange>('1h')
  const [refreshKey, setRefreshKey] = useState(0)
  const [viewMode, setViewMode] = useState<ViewMode>('dashboard')
  const [resolvedTheme, setResolvedTheme] = useState<'light' | 'dark'>('light')

  const dashboard = activeDashboard !== 'generate' ? DASHBOARDS[activeDashboard] : null

  useEffect(() => {
    const media = window.matchMedia('(prefers-color-scheme: dark)')
    const syncTheme = () => setResolvedTheme(media.matches ? 'dark' : 'light')
    syncTheme()
    media.addEventListener('change', syncTheme)
    return () => media.removeEventListener('change', syncTheme)
  }, [])

  const handleRefresh = () => {
    setRefreshKey((prev) => prev + 1)
  }

  return (
    <div className="min-h-screen p-6">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="flex items-center gap-2 font-heading text-2xl font-semibold">
              <BarChart3 className="w-6 h-6 text-[var(--accent)]" />
              Metrics Dashboard
            </h1>
            <p className="text-[var(--muted-foreground)] mt-1">
              Real-time SQL Server performance metrics from ClickHouse
            </p>
          </div>

          <div className="flex items-center gap-3">
            {/* Time range */}
            <div className="flex items-center gap-1 bg-[var(--muted)] rounded-lg p-1">
              {TIME_RANGES.map((range) => (
                <button
                  key={range.value}
                  onClick={() => setTimeRange(range.value)}
                  className={`
                    px-3 py-1.5 text-sm rounded-md transition-colors
                    ${timeRange === range.value
                      ? 'bg-[var(--accent)] text-[var(--accent-foreground)]'
                      : 'text-[var(--muted-foreground)] hover:text-[var(--foreground)]'
                    }
                  `}
                >
                  {range.label}
                </button>
              ))}
            </div>

            {/* Refresh */}
            <button
              onClick={handleRefresh}
              className="p-2 rounded-lg bg-[var(--muted)] text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-colors"
              title="Refresh dashboards"
              aria-label="Refresh dashboards"
            >
              <RefreshCw className="w-4 h-4" />
            </button>

            {/* Open Grafana */}
            <a
              href={GRAFANA_BASE_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-2 px-3 py-2 rounded-lg bg-[var(--muted)] text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-colors text-sm"
            >
              Open Grafana
              <ExternalLink className="w-3 h-3" />
            </a>
          </div>
        </div>
      </div>

      {/* Dashboard Tabs */}
      <div className="flex items-center gap-2 mb-6 border-b border-[var(--border)]">
        {Object.entries(DASHBOARDS).map(([key, dash]) => {
          const isActive = activeDashboard === key
          const Icon = key === 'overview' ? Database : key === 'waitStats' ? Clock : Activity

          return (
            <button
              key={key}
              onClick={() => setActiveDashboard(key as DashboardKey)}
              className={`
                flex items-center gap-2 px-4 py-3 text-sm font-medium
                border-b-2 transition-colors -mb-[2px]
                ${isActive
                  ? 'border-[var(--accent)] text-[var(--accent)]'
                  : 'border-transparent text-[var(--muted-foreground)] hover:text-[var(--foreground)]'
                }
              `}
            >
              <Icon className="w-4 h-4" />
              {dash.title}
            </button>
          )
        })}

        {/* Generate Tab */}
        <button
          onClick={() => setActiveDashboard('generate')}
          className={`
            flex items-center gap-2 px-4 py-3 text-sm font-medium
            border-b-2 transition-colors -mb-[2px]
            ${activeDashboard === 'generate'
              ? 'border-[var(--accent)] text-[var(--accent)]'
              : 'border-transparent text-[var(--muted-foreground)] hover:text-[var(--foreground)]'
            }
          `}
        >
                <Sparkles className="w-4 h-4" />
                Generate
              </button>
        {activeDashboard !== 'generate' && (
          <div className="ml-auto flex items-center gap-1 rounded-lg bg-[var(--muted)] p-1">
            <button
              onClick={() => setViewMode('dashboard')}
              className={`inline-flex items-center gap-1 rounded-md px-2.5 py-1.5 text-xs transition-colors ${
                viewMode === 'dashboard'
                  ? 'bg-[var(--accent)] text-[var(--accent-foreground)]'
                  : 'text-[var(--muted-foreground)] hover:text-[var(--foreground)]'
              }`}
              aria-pressed={viewMode === 'dashboard'}
            >
              <PanelTop className="h-3.5 w-3.5" />
              Full
            </button>
            <button
              onClick={() => setViewMode('panels')}
              className={`inline-flex items-center gap-1 rounded-md px-2.5 py-1.5 text-xs transition-colors ${
                viewMode === 'panels'
                  ? 'bg-[var(--accent)] text-[var(--accent-foreground)]'
                  : 'text-[var(--muted-foreground)] hover:text-[var(--foreground)]'
              }`}
              aria-pressed={viewMode === 'panels'}
            >
              <LayoutGrid className="h-3.5 w-3.5" />
              Panels
            </button>
          </div>
        )}
      </div>

      {/* Dashboard Content */}
      <div className="space-y-6">
        {activeDashboard === 'generate' ? (
          <ChartGenerator theme={resolvedTheme} />
        ) : dashboard ? (
          viewMode === 'dashboard' ? (
            <GrafanaEmbed
              key={`${activeDashboard}-${timeRange}-${refreshKey}-full`}
              dashboardUid={dashboard.uid}
              title={dashboard.title}
              height={680}
              from={`now-${timeRange}`}
              to="now"
              theme={resolvedTheme}
              showFullDashboard={true}
              source={`dashboard-${activeDashboard}-${timeRange}-${refreshKey}`}
            />
          ) : (
            <div>
              <h2 className="mb-4 font-heading text-lg font-semibold">Individual Panels</h2>
              <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                {dashboard.panels.map((panel) => (
                  <LazyGrafanaEmbed
                    key={`${activeDashboard}-${panel.id}-${timeRange}-${refreshKey}-panel`}
                    dashboardUid={dashboard.uid}
                    panelId={panel.id}
                    title={panel.title}
                    height={320}
                    from={`now-${timeRange}`}
                    to="now"
                    theme={resolvedTheme}
                    source={`dashboard-panel-${activeDashboard}-${panel.id}-${timeRange}-${refreshKey}`}
                  />
                ))}
              </div>
            </div>
          )
        ) : null}
      </div>
    </div>
  )
}
