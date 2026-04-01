'use client'

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Database, Square, X } from 'lucide-react'
import { ChatMessage } from '@/components/ChatMessage'
import { MessageInput } from '@/components/MessageInput'
import { ChatHeader, HistoryPane } from '@/components/chat'
import { useChatSession } from '@/hooks/use-chat-session'
import { normalizeApiBase } from '@/lib/api'

const API_BASE = normalizeApiBase(process.env.NEXT_PUBLIC_API_URL || 'http://127.0.0.1:8000')
const REPO_PATH = process.env.NEXT_PUBLIC_REPO_PATH || null
const SQLSERVER_HOST = process.env.NEXT_PUBLIC_SQLSERVER_HOST || null
const SQLSERVER_PORT = Number(process.env.NEXT_PUBLIC_SQLSERVER_PORT || '1433')
const SQLSERVER_USER = process.env.NEXT_PUBLIC_SQLSERVER_USER || null
const SQLSERVER_DATABASE = process.env.NEXT_PUBLIC_SQLSERVER_DATABASE || null
const ENABLE_MONITORING = (process.env.NEXT_PUBLIC_ENABLE_MONITORING || '1').toLowerCase() === '1'
const AUTO_INSTALL_BLITZ = (process.env.NEXT_PUBLIC_AUTO_INSTALL_BLITZ || '1').toLowerCase() !== '0'
const CLICKHOUSE_HOST = process.env.NEXT_PUBLIC_CLICKHOUSE_HOST || null
const CLICKHOUSE_PORT = Number(process.env.NEXT_PUBLIC_CLICKHOUSE_PORT || '8123')
const CLICKHOUSE_DATABASE = process.env.NEXT_PUBLIC_CLICKHOUSE_DATABASE || null

export default function Home() {
  const {
    sessionId,
    messages,
    isLoading,
    isConnecting,
    isStreaming,
    error,
    hideInternalBlocks,
    blitzInstallState,
    sessionHistory,
    isHistoryLoading,
    historyError,
    setHideInternalBlocks,
    sendMessage,
    startNewChat,
    openSession,
    installBlitzScripts,
    declineBlitzInstall,
    stopStreaming,
    refreshSessionHistory,
    retryConnection,
    clearError,
  } = useChatSession({
    apiBase: API_BASE,
    repoPath: REPO_PATH,
    sqlserverHost: SQLSERVER_HOST,
    sqlserverPort: SQLSERVER_PORT,
    sqlserverUser: SQLSERVER_USER,
    sqlserverDatabase: SQLSERVER_DATABASE,
    enableMonitoring: ENABLE_MONITORING,
    autoInstallBlitz: AUTO_INSTALL_BLITZ,
    clickhouseHost: CLICKHOUSE_HOST,
    clickhousePort: CLICKHOUSE_PORT,
    clickhouseDatabase: CLICKHOUSE_DATABASE,
  })

  const [isMobileHistoryOpen, setIsMobileHistoryOpen] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)

  const activeSession = useMemo(
    () => sessionHistory.find((session) => session.session_id === sessionId) || null,
    [sessionHistory, sessionId],
  )
  const activeTitle = activeSession?.title || 'New Analysis'
  const isHomeView = messages.length === 0 && !isConnecting && Boolean(sessionId)

  const scrollToLatest = (behavior: ScrollBehavior = 'smooth') => {
    messagesEndRef.current?.scrollIntoView({ behavior, block: 'end' })
  }

  const handleOpenSession = async (targetSessionId: string) => {
    setIsMobileHistoryOpen(false)
    await openSession(targetSessionId)
  }

  const handleStartNewChat = useCallback(async () => {
    setIsMobileHistoryOpen(false)
    await startNewChat()
  }, [startNewChat])

  useEffect(() => {
    const handleSidebarNewChat = () => {
      void handleStartNewChat()
    }

    window.addEventListener('sim:new-chat', handleSidebarNewChat)
    return () => {
      window.removeEventListener('sim:new-chat', handleSidebarNewChat)
    }
  }, [handleStartNewChat])

  const handleSendMessage = (content: string, files?: File[]) => {
    void sendMessage(content, files)
    requestAnimationFrame(() => scrollToLatest('smooth'))
    setTimeout(() => scrollToLatest('auto'), 120)
  }

  return (
    <div className="relative flex h-[100dvh] overflow-hidden">
      <HistoryPane
        sessions={sessionHistory}
        activeSessionId={sessionId}
        isLoading={isHistoryLoading}
        error={historyError}
        onSelectSession={handleOpenSession}
        className="fixed left-0 top-[170px] z-[60] hidden h-[calc(100dvh-170px)] w-60 md:flex"
      />

      {isMobileHistoryOpen && (
        <>
          <div
            className="fixed inset-0 z-40 bg-black/45 lg:hidden"
            onClick={() => setIsMobileHistoryOpen(false)}
          />
          <div className="fixed inset-y-0 left-0 z-50 w-[88vw] max-w-sm border-r border-[var(--border)] bg-[var(--surface)] lg:hidden">
            <HistoryPane
              sessions={sessionHistory}
              activeSessionId={sessionId}
              isLoading={isHistoryLoading}
              error={historyError}
              onSelectSession={handleOpenSession}
              onClose={() => setIsMobileHistoryOpen(false)}
              className="h-full"
            />
          </div>
        </>
      )}

      <section className="flex min-h-0 min-w-0 flex-1 flex-col">
        <ChatHeader
          activeTitle={activeTitle}
          isConnecting={isConnecting}
          isLoading={isLoading}
          hasSession={Boolean(sessionId)}
          hideInternalBlocks={hideInternalBlocks}
          codeAnalysisEnabled={Boolean(REPO_PATH)}
          showControlActions={!isHomeView}
          onToggleInternals={() => setHideInternalBlocks(!hideInternalBlocks)}
          onStartNewChat={handleStartNewChat}
          onToggleMobileHistory={() => setIsMobileHistoryOpen((prev) => !prev)}
        />

        {error && (
          <div className="mx-4 mt-3 rounded-lg border border-[var(--danger)]/30 bg-[var(--danger)]/10 px-4 py-3 text-sm text-[var(--danger)] sm:mx-6">
            <div className="flex items-center justify-between gap-3">
              <span>{error}</span>
              <div className="flex items-center gap-3 text-xs">
                <button
                  onClick={retryConnection}
                  className="underline underline-offset-2 hover:no-underline"
                >
                  Retry
                </button>
                <button onClick={clearError} className="hover:opacity-70" aria-label="Dismiss error">
                  <X className="h-4 w-4" />
                </button>
              </div>
            </div>
          </div>
        )}

        {isHomeView ? (
          <main className="flex flex-1 items-center justify-center px-4 py-8">
            <div className="w-full max-w-4xl">
              <div className="mx-auto mb-8 max-w-3xl text-center">
                <div className="mx-auto mb-4 flex h-14 w-14 items-center justify-center rounded-2xl border border-[var(--border)] bg-[var(--surface)]">
                  <Database className="h-7 w-7 text-[var(--accent)]" />
                </div>
                <h2 className="font-heading text-3xl font-semibold">Welcome to SQL Server Assistant</h2>
                <p className="mx-auto mt-2 max-w-xl text-[var(--muted-foreground)]">
                  Describe the production symptom, and the assistant will investigate waits, workload shifts,
                  blocking patterns, and recent metric anomalies.
                </p>
              </div>

              <div className="mx-auto w-full max-w-3xl">
                <MessageInput
                  onSend={handleSendMessage}
                  disabled={!sessionId || isLoading}
                  placeholder={
                    !sessionId
                      ? 'Connecting to backend...'
                      : isLoading
                        ? 'Analyzing...'
                        : 'Describe your database issue...'
                  }
                />
              </div>

              <div className="mt-6 flex flex-wrap justify-center gap-2 text-sm">
                <button
                  onClick={() => handleSendMessage('My database is slow, can you help?')}
                  className="rounded-full border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5 transition-colors hover:bg-[var(--muted)]"
                >
                  My database is slow
                </button>
                <button
                  onClick={() => handleSendMessage("I'm seeing query timeouts")}
                  className="rounded-full border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5 transition-colors hover:bg-[var(--muted)]"
                >
                  I&apos;m seeing query timeouts
                </button>
                <button
                  onClick={() => handleSendMessage('Can you run a health check?')}
                  className="rounded-full border border-[var(--border)] bg-[var(--surface)] px-3 py-1.5 transition-colors hover:bg-[var(--muted)]"
                >
                  Run a health check
                </button>
              </div>
            </div>
          </main>
        ) : (
          <>
            <main
              className="min-h-0 flex-1 overflow-y-auto"
              onClick={() => setIsMobileHistoryOpen(false)}
            >
              <div className="mx-auto max-w-5xl px-4 py-6 sm:px-6">
                {messages.map((message) => (
                  <div key={message.id} id={`message-${message.id}`}>
                    <ChatMessage
                      message={message}
                      hideInternalBlocks={hideInternalBlocks}
                      sessionId={sessionId}
                      blitzInstallState={blitzInstallState}
                      onInstallBlitz={installBlitzScripts}
                      onDeclineBlitz={declineBlitzInstall}
                    />
                  </div>
                ))}

                <div ref={messagesEndRef} />
              </div>
            </main>

            <footer className="shrink-0 px-4 py-4 sm:px-6">
              <div className="mx-auto max-w-5xl">
                {isStreaming && (
                  <div className="mb-2 flex justify-end">
                    <button
                      type="button"
                      onClick={stopStreaming}
                      className="inline-flex items-center gap-1.5 rounded-md bg-[var(--muted)] px-3.5 py-1.5 text-sm font-medium text-[var(--foreground)] transition-colors hover:bg-[var(--border)]"
                    >
                      <Square className="h-4 w-4 fill-current" />
                      Stop
                    </button>
                  </div>
                )}
                <MessageInput
                  onSend={handleSendMessage}
                  disabled={!sessionId || isLoading}
                  placeholder={
                    !sessionId
                      ? 'Connecting to backend...'
                      : isLoading
                        ? 'Analyzing...'
                        : 'Describe your database issue...'
                  }
                />
              </div>
            </footer>
          </>
        )}
      </section>
    </div>
  )
}
