'use client'

import { Database, Eye, EyeOff, History, Loader2, Plus, Sparkles } from 'lucide-react'

interface ChatHeaderProps {
  activeTitle: string
  isConnecting: boolean
  isLoading: boolean
  hasSession: boolean
  hideInternalBlocks: boolean
  codeAnalysisEnabled?: boolean
  showControlActions?: boolean
  onToggleInternals: () => void
  onStartNewChat: () => void
  onToggleMobileHistory: () => void
}

export function ChatHeader({
  activeTitle,
  isConnecting,
  isLoading,
  hasSession,
  hideInternalBlocks,
  codeAnalysisEnabled = false,
  showControlActions = true,
  onToggleInternals,
  onStartNewChat,
  onToggleMobileHistory,
}: ChatHeaderProps) {
  return (
    <header className="flex items-center justify-between border-b border-[var(--border)] bg-[var(--surface)] px-4 py-3 sm:px-6">
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <Database className="h-5 w-5 text-[var(--accent)]" />
          <h1 className="truncate font-heading text-base font-semibold sm:text-lg">SQL Server Assistant</h1>
        </div>
        <p className="mt-1 truncate text-xs text-[var(--muted-foreground)] sm:text-sm">
          {activeTitle}
        </p>
      </div>

      <div className="ml-3 flex items-center gap-2 text-sm text-[var(--muted-foreground)]">
        <button
          onClick={onToggleMobileHistory}
          className="inline-flex items-center gap-1 rounded-md border border-[var(--border)] px-2 py-1 text-xs transition-colors hover:bg-[var(--muted)] md:hidden"
          aria-label="Toggle conversations panel"
        >
          <History className="h-3.5 w-3.5" />
          Chats
        </button>

        {showControlActions && (
          <button
            onClick={onToggleInternals}
            className={`inline-flex items-center gap-1.5 rounded-md border px-2 py-1 text-xs transition-colors ${
              hideInternalBlocks
                ? 'border-[var(--accent)] bg-[var(--accent)]/10 text-[var(--foreground)]'
                : 'border-[var(--border)] hover:bg-[var(--muted)]'
            }`}
            title={hideInternalBlocks ? 'Show thinking and tools' : 'Hide thinking and tools'}
            aria-pressed={hideInternalBlocks}
          >
            {hideInternalBlocks ? <EyeOff className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />}
            Internals
          </button>
        )}

        {showControlActions && codeAnalysisEnabled && (
          <span className="hidden items-center gap-1 rounded-md border border-[var(--accent-2)]/40 bg-[var(--accent-2)]/10 px-2 py-1 text-xs text-[var(--accent-2)] sm:inline-flex">
            <Sparkles className="h-3.5 w-3.5" />
            Code Analysis
          </span>
        )}

        {isConnecting ? (
          <span className="inline-flex items-center gap-1 rounded-md border border-[var(--border)] px-2 py-1 text-xs">
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
            Connecting
          </span>
        ) : isLoading ? (
          <span className="inline-flex items-center gap-1 rounded-md border border-[var(--border)] px-2 py-1 text-xs">
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
            Analyzing
          </span>
        ) : null}

        {showControlActions && (
          <button
            onClick={onStartNewChat}
            disabled={isLoading || !hasSession}
            className="inline-flex items-center gap-1 rounded-md border border-[var(--border)] px-2 py-1 text-xs transition-colors hover:bg-[var(--muted)] disabled:opacity-50"
            title="Start new chat"
          >
            <Plus className="h-3.5 w-3.5" />
            New
          </button>
        )}
      </div>
    </header>
  )
}
