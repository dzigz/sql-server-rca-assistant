'use client'

import { useState } from 'react'
import { Brain, ChevronDown, ChevronRight } from 'lucide-react'

interface ThinkingBlockProps {
  content: string
  isStreaming?: boolean
}

export function ThinkingBlock({ content, isStreaming }: ThinkingBlockProps) {
  const [isExpanded, setIsExpanded] = useState(false)

  if (!content) return null

  // Truncate content for preview
  const previewLength = 100
  const preview = content.length > previewLength
    ? content.slice(0, previewLength) + '...'
    : content

  return (
    <div className="mb-3 rounded-lg border border-[var(--border)] bg-[var(--muted)]/50 overflow-hidden">
      {/* Header - clickable to expand */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center gap-2 px-3 py-2 text-left text-sm text-[var(--muted-foreground)] hover:bg-[var(--muted)] transition-colors"
      >
        <Brain className="w-4 h-4 flex-shrink-0" />
        <span className="font-medium">Thinking</span>
        {isStreaming && (
          <span className="inline-block w-1.5 h-1.5 rounded-full bg-[var(--accent)] animate-pulse" />
        )}
        <span className="flex-1" />
        {isExpanded ? (
          <ChevronDown className="w-4 h-4" />
        ) : (
          <ChevronRight className="w-4 h-4" />
        )}
      </button>

      {/* Content */}
      {isExpanded ? (
        <div className="px-3 pb-3 pt-1 text-sm text-[var(--muted-foreground)] whitespace-pre-wrap font-mono">
          {content}
          {isStreaming && (
            <span className="inline-block w-1.5 h-3 bg-[var(--muted-foreground)] cursor-blink ml-0.5" />
          )}
        </div>
      ) : (
        <div className="px-3 pb-2 text-xs text-[var(--muted-foreground)] truncate">
          {preview}
        </div>
      )}
    </div>
  )
}
