'use client'

import { Loader2, X } from 'lucide-react'
import type { SessionSummary } from './types'

interface HistoryPaneProps {
  sessions: SessionSummary[]
  activeSessionId: string | null
  isLoading?: boolean
  error?: string | null
  onSelectSession: (sessionId: string) => void
  onClose?: () => void
  showHeader?: boolean
  className?: string
}

const formatSessionTimestamp = (timestamp: string): string => {
  const date = new Date(timestamp)
  if (Number.isNaN(date.getTime())) return 'Unknown time'
  return date.toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

export function HistoryPane({
  sessions,
  activeSessionId,
  isLoading = false,
  error,
  onSelectSession,
  onClose,
  showHeader = true,
  className = '',
}: HistoryPaneProps) {
  return (
    <aside className={`flex flex-col bg-[var(--surface)] ${className}`}>
      {showHeader && (
        <div className="px-4 pb-2 pt-6">
          <div className="flex items-center justify-between">
            <h2 className="font-heading text-xs font-semibold tracking-[0.08em] text-[var(--muted-foreground)] uppercase">
              Chat History
            </h2>
            {onClose && (
              <button
                onClick={onClose}
                className="rounded-md p-1 text-[var(--muted-foreground)] transition-colors hover:bg-[var(--muted)] hover:text-[var(--foreground)]"
                title="Close conversations"
                aria-label="Close conversations"
              >
                <X className="h-4 w-4" />
              </button>
            )}
          </div>
        </div>
      )}

      <div className="flex-1 overflow-y-auto px-2 pb-3">
        {error && (
          <div className="mb-2 rounded-md bg-[var(--danger)]/10 p-2 text-xs text-[var(--danger)]">
            {error}
          </div>
        )}

        {!error && sessions.length === 0 && !isLoading && (
          <p className="rounded-md bg-[var(--muted)]/60 px-3 py-4 text-center text-xs text-[var(--muted-foreground)]">
            No saved chats yet.
          </p>
        )}

        <div className="space-y-1.5">
          {sessions.map((session) => {
            const isActive = session.session_id === activeSessionId
            return (
              <button
                key={session.session_id}
                onClick={() => onSelectSession(session.session_id)}
                className={`w-full rounded-lg px-3 py-2 text-left transition-colors ${
                  isActive
                    ? 'bg-[var(--accent)]/12'
                    : 'hover:bg-[var(--muted)]/70'
                }`}
              >
                <div className="line-clamp-2 text-sm font-medium text-[var(--foreground)]">
                  {session.title || 'New analysis'}
                </div>
                <div className="mt-1 flex items-center justify-between text-[11px] text-[var(--muted-foreground)]">
                  <span>{session.message_count} msgs</span>
                  <span>{formatSessionTimestamp(session.last_message_at || session.created_at)}</span>
                </div>
              </button>
            )
          })}
        </div>

        {isLoading && (
          <div className="mt-3 flex items-center justify-center gap-2 py-2 text-xs text-[var(--muted-foreground)]">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading...
          </div>
        )}
      </div>
    </aside>
  )
}
