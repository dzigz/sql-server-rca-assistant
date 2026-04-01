'use client'

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type {
  BackendMessage,
  BlitzInstallState,
  Message,
  MessageBlock,
  MessageFile,
  SessionSummary,
  ToolCall,
} from '@/components/chat/types'
import { buildApiUrl } from '@/lib/api'

const SESSION_STORAGE_KEY = 'sim_session_id'

interface UseChatSessionOptions {
  apiBase: string
  repoPath?: string | null
  sqlserverHost?: string | null
  sqlserverPort?: number
  sqlserverUser?: string | null
  sqlserverDatabase?: string | null
  enableMonitoring?: boolean
  autoInstallBlitz?: boolean
  clickhouseHost?: string | null
  clickhousePort?: number
  clickhouseDatabase?: string | null
}

const makeId = (prefix: string) => {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return `${prefix}-${crypto.randomUUID()}`
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`
}

function convertBackendMessage(msg: BackendMessage): Message {
  const message: Message = {
    id: makeId('restored'),
    role: msg.role as 'user' | 'assistant' | 'system',
    content: msg.content,
    blocks: [],
    isStreaming: false,
  }

  if (msg.blocks && msg.blocks.length > 0) {
    message.blocks = msg.blocks.map((block, blockIdx) => {
      const frontendBlock: MessageBlock = {
        type: block.type,
        id: block.id || makeId(`block-${blockIdx}`),
        isComplete: true,
      }

      if (block.type === 'thinking' || block.type === 'text') {
        frontendBlock.content = block.content || ''
      } else if (block.type === 'tool') {
        frontendBlock.toolCall = {
          id: block.id || makeId(`tool-${blockIdx}`),
          name: block.name || 'unknown',
          arguments: block.arguments || {},
          result: block.result,
          completed: true,
        }
      }

      return frontendBlock
    })
  }

  return message
}

function getErrorMessage(err: unknown, fallback: string): string {
  if (err instanceof Error && err.message) return err.message
  return fallback
}

export function useChatSession({
  apiBase,
  repoPath,
  sqlserverHost,
  sqlserverPort = 1433,
  sqlserverUser,
  sqlserverDatabase,
  enableMonitoring = true,
  autoInstallBlitz = true,
  clickhouseHost,
  clickhousePort = 8123,
  clickhouseDatabase,
}: UseChatSessionOptions) {
  const [sessionId, setSessionId] = useState<string | null>(null)
  const [messages, setMessages] = useState<Message[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [isConnecting, setIsConnecting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [hideInternalBlocks, setHideInternalBlocks] = useState(false)
  const [blitzInstallState, setBlitzInstallState] = useState<BlitzInstallState>({ status: 'idle' })
  const [sessionHistory, setSessionHistory] = useState<SessionSummary[]>([])
  const [isHistoryLoading, setIsHistoryLoading] = useState(false)
  const [historyError, setHistoryError] = useState<string | null>(null)
  const activeAbortControllerRef = useRef<AbortController | null>(null)

  const isStreaming = useMemo(
    () => messages.some((message) => message.isStreaming),
    [messages],
  )

  const refreshSessionHistory = useCallback(async () => {
    setIsHistoryLoading(true)
    setHistoryError(null)
    try {
      const response = await fetch(buildApiUrl('/api/session/summaries', apiBase))
      if (!response.ok) {
        throw new Error('Failed to load session history')
      }
      const data = await response.json()
      const sessions = Array.isArray(data.sessions)
        ? data.sessions.filter(
            (session: SessionSummary) =>
              typeof session.message_count === 'number' && session.message_count > 0,
          )
        : []
      setSessionHistory(sessions)
    } catch (err) {
      setHistoryError(getErrorMessage(err, 'Failed to load session history.'))
    } finally {
      setIsHistoryLoading(false)
    }
  }, [apiBase])

  const createSession = useCallback(async () => {
    setIsConnecting(true)
    setError(null)
    try {
      const createSessionUrl = buildApiUrl('/api/session/create', apiBase)
      const response = await fetch(createSessionUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          repo_path: repoPath || undefined,
          sqlserver_host: sqlserverHost || undefined,
          sqlserver_port: sqlserverPort,
          sqlserver_user: sqlserverUser || undefined,
          sqlserver_database: sqlserverDatabase || undefined,
          enable_monitoring: enableMonitoring,
          auto_install_blitz: autoInstallBlitz,
          clickhouse_host: clickhouseHost || undefined,
          clickhouse_port: clickhousePort,
          clickhouse_database: clickhouseDatabase || undefined,
        }),
      })

      if (!response.ok) {
        let detail = ''
        try {
          const payload = await response.json()
          if (payload && typeof payload.detail === 'string') {
            detail = payload.detail
          } else {
            detail = JSON.stringify(payload)
          }
        } catch {
          try {
            detail = await response.text()
          } catch {
            detail = ''
          }
        }
        const statusLabel = `${response.status} ${response.statusText}`.trim()
        throw new Error(
          detail
            ? `Failed to create session (${statusLabel}) via ${createSessionUrl}: ${detail}`
            : `Failed to create session (${statusLabel}) via ${createSessionUrl}`,
        )
      }

      const data = await response.json()
      setSessionId(data.session_id)
      setBlitzInstallState({ status: 'idle' })
      localStorage.setItem(SESSION_STORAGE_KEY, data.session_id)
      await refreshSessionHistory()
    } catch (err) {
      const message = getErrorMessage(err, 'Failed to connect to backend. Make sure the server is running.')
      if (message.includes('Failed to fetch') || message.includes('NetworkError')) {
        setError('Failed to connect to backend. Make sure the server is running.')
      } else {
        setError(message)
      }
      console.error('Session creation failed:', err)
    } finally {
      setIsConnecting(false)
    }
  }, [
    apiBase,
    clickhouseDatabase,
    clickhouseHost,
    clickhousePort,
    autoInstallBlitz,
    enableMonitoring,
    refreshSessionHistory,
    repoPath,
    sqlserverDatabase,
    sqlserverHost,
    sqlserverPort,
    sqlserverUser,
  ])

  const restoreSession = useCallback(
    async (targetSessionId: string) => {
      setIsConnecting(true)
      setError(null)
      try {
        const historyResponse = await fetch(buildApiUrl(`/api/session/${targetSessionId}/history`, apiBase))
        if (!historyResponse.ok) {
          localStorage.removeItem(SESSION_STORAGE_KEY)
          await createSession()
          return
        }

        const historyData = await historyResponse.json()
        const backendMessages: BackendMessage[] = historyData.messages || []
        const restoredMessages = backendMessages.map((msg) => convertBackendMessage(msg))

        setSessionId(targetSessionId)
        setMessages(restoredMessages)
        setBlitzInstallState({ status: 'idle' })
        localStorage.setItem(SESSION_STORAGE_KEY, targetSessionId)
        await refreshSessionHistory()
      } catch (err) {
        console.error('Session restoration failed:', err)
        localStorage.removeItem(SESSION_STORAGE_KEY)
        await createSession()
      } finally {
        setIsConnecting(false)
      }
    },
    [apiBase, createSession, refreshSessionHistory],
  )

  const openSession = useCallback(
    async (targetSessionId: string) => {
      if (targetSessionId === sessionId) return
      setMessages([])
      setSessionId(null)
      await restoreSession(targetSessionId)
    },
    [restoreSession, sessionId],
  )

  const startNewChat = useCallback(async () => {
    setMessages([])
    setSessionId(null)
    setBlitzInstallState({ status: 'idle' })
    localStorage.removeItem(SESSION_STORAGE_KEY)
    await createSession()
  }, [createSession])

  const installBlitzScripts = useCallback(async () => {
    if (!sessionId) {
      setBlitzInstallState({
        status: 'error',
        message: 'No active session. Start a new chat first.',
      })
      return
    }

    setBlitzInstallState({ status: 'installing' })
    try {
      const response = await fetch(buildApiUrl(`/api/session/${sessionId}/blitz/install`, apiBase), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ confirm: true }),
      })

      const payload = await response.json().catch(() => ({} as Record<string, unknown>))
      if (!response.ok) {
        const detail = typeof payload.detail === 'string' ? payload.detail : 'Failed to install Blitz scripts.'
        throw new Error(detail)
      }

      const message = typeof payload.message === 'string'
        ? payload.message
        : 'First Responder Kit scripts installed successfully.'

      setBlitzInstallState({
        status: 'installed',
        message,
      })
    } catch (err) {
      setBlitzInstallState({
        status: 'error',
        message: getErrorMessage(err, 'Failed to install Blitz scripts.'),
      })
    }
  }, [apiBase, sessionId])

  const declineBlitzInstall = useCallback(async () => {
    if (!sessionId) {
      setBlitzInstallState({
        status: 'error',
        message: 'No active session. Start a new chat first.',
      })
      return
    }

    try {
      const response = await fetch(buildApiUrl(`/api/session/${sessionId}/blitz/decline`, apiBase), {
        method: 'POST',
      })

      const payload = await response.json().catch(() => ({} as Record<string, unknown>))
      if (!response.ok) {
        const detail = typeof payload.detail === 'string' ? payload.detail : 'Failed to decline installation.'
        throw new Error(detail)
      }

      const message = typeof payload.message === 'string'
        ? payload.message
        : 'Installation declined for this session.'
      setBlitzInstallState({
        status: 'declined',
        message,
      })
    } catch (err) {
      setBlitzInstallState({
        status: 'error',
        message: getErrorMessage(err, 'Failed to decline installation.'),
      })
    }
  }, [apiBase, sessionId])

  const sendMessage = useCallback(
    async (content: string, files?: File[]) => {
      if (!sessionId || isLoading) return

      setIsLoading(true)
      setError(null)

      let messageFiles: MessageFile[] | undefined
      if (files && files.length > 0) {
        messageFiles = await Promise.all(
          files.map(async (file): Promise<MessageFile> => {
            const isImage = file.type.startsWith('image/')
            if (isImage) {
              const base64 = await new Promise<string>((resolve) => {
                const reader = new FileReader()
                reader.onload = () => {
                  const result = reader.result as string
                  resolve(result.split(',')[1])
                }
                reader.readAsDataURL(file)
              })
              return {
                name: file.name,
                type: 'image',
                mediaType: file.type,
                data: base64,
              }
            }
            const text = await file.text()
            return {
              name: file.name,
              type: 'csv',
              mediaType: 'text/csv',
              data: text,
            }
          }),
        )
      }

      const userMessage: Message = {
        id: makeId('user'),
        role: 'user',
        content,
        blocks: [],
        files: messageFiles,
      }
      setMessages((prev) => [...prev, userMessage])

      const assistantMessage: Message = {
        id: makeId('assistant'),
        role: 'assistant',
        content: '',
        blocks: [],
        isStreaming: true,
      }
      setMessages((prev) => [...prev, assistantMessage])
      const abortController = new AbortController()
      activeAbortControllerRef.current = abortController

      try {
        const formData = new FormData()
        formData.append('session_id', sessionId)
        formData.append('message', content)
        if (files) {
          files.forEach((file) => formData.append('files', file))
        }

        const response = await fetch(buildApiUrl('/api/chat/stream', apiBase), {
          method: 'POST',
          body: formData,
          signal: abortController.signal,
        })

        if (!response.ok) {
          throw new Error('Failed to send message')
        }

        const reader = response.body?.getReader()
        if (!reader) {
          throw new Error('No response body')
        }

        const decoder = new TextDecoder()
        let currentBlocks: MessageBlock[] = []
        let currentContent = ''
        let pendingUpdate = false
        let rafId: number | null = null

        const scheduleUpdate = (updates: Partial<Message> = {}) => {
          if (pendingUpdate) return
          pendingUpdate = true
          rafId = requestAnimationFrame(() => {
            setMessages((prev) =>
              prev.map((m) =>
                m.id === assistantMessage.id
                  ? { ...m, blocks: [...currentBlocks], ...updates }
                  : m,
              ),
            )
            pendingUpdate = false
          })
        }

        const updateMessageImmediate = (updates: Partial<Message> = {}) => {
          if (rafId) cancelAnimationFrame(rafId)
          pendingUpdate = false
          setMessages((prev) =>
            prev.map((m) =>
              m.id === assistantMessage.id
                ? { ...m, blocks: [...currentBlocks], ...updates }
                : m,
            ),
          )
        }

        const updateLastBlock = (type: 'thinking' | 'text', delta: string) => {
          const lastIdx = currentBlocks.length - 1
          if (lastIdx >= 0 && currentBlocks[lastIdx].type === type) {
            currentBlocks[lastIdx] = {
              ...currentBlocks[lastIdx],
              content: (currentBlocks[lastIdx].content || '') + delta,
            }
          }
        }

        const processEvent = (eventData: string) => {
          try {
            const data = JSON.parse(eventData)

            switch (data.type) {
              case 'thinking_start':
                currentBlocks = currentBlocks.map((b) =>
                  b.type === 'tool' && !b.isComplete
                    ? {
                        ...b,
                        isComplete: true,
                        toolCall: b.toolCall
                          ? { ...b.toolCall, completed: true }
                          : undefined,
                      }
                    : b,
                )
                currentBlocks = [
                  ...currentBlocks,
                  {
                    type: 'thinking',
                    id: makeId('thinking'),
                    content: '',
                    isComplete: false,
                  },
                ]
                updateMessageImmediate()
                break

              case 'thinking_delta':
                updateLastBlock('thinking', data.content)
                scheduleUpdate()
                break

              case 'thinking_end':
                if (currentBlocks.length > 0) {
                  const lastIdx = currentBlocks.length - 1
                  if (currentBlocks[lastIdx].type === 'thinking') {
                    currentBlocks[lastIdx] = {
                      ...currentBlocks[lastIdx],
                      isComplete: true,
                    }
                    updateMessageImmediate()
                  }
                }
                break

              case 'text_start':
                currentBlocks = currentBlocks.map((b) =>
                  b.type === 'tool' && !b.isComplete
                    ? {
                        ...b,
                        isComplete: true,
                        toolCall: b.toolCall
                          ? { ...b.toolCall, completed: true }
                          : undefined,
                      }
                    : b,
                )
                currentBlocks = [
                  ...currentBlocks,
                  {
                    type: 'text',
                    id: makeId('text'),
                    content: '',
                    isComplete: false,
                  },
                ]
                currentContent = ''
                updateMessageImmediate()
                break

              case 'text_delta':
                currentContent += data.content
                updateLastBlock('text', data.content)
                scheduleUpdate({ content: currentContent })
                break

              case 'text_end':
                if (currentBlocks.length > 0) {
                  const lastIdx = currentBlocks.length - 1
                  if (currentBlocks[lastIdx].type === 'text') {
                    currentBlocks[lastIdx] = { ...currentBlocks[lastIdx], isComplete: true }
                    updateMessageImmediate()
                  }
                }
                break

              case 'tool_use_end':
                if (data.tool_call) {
                  currentBlocks = currentBlocks.map((b) =>
                    b.type === 'tool' && b.toolCall
                      ? {
                          ...b,
                          isComplete: true,
                          toolCall: { ...b.toolCall, completed: true },
                        }
                      : b,
                  )

                  const toolCall: ToolCall = {
                    id: data.tool_call.id || makeId('tool'),
                    name: data.tool_call.name || 'unknown',
                    arguments: data.tool_call.arguments || {},
                    completed: false,
                  }

                  currentBlocks = [
                    ...currentBlocks,
                    {
                      type: 'tool',
                      id: `tool-${toolCall.id}`,
                      toolCall,
                      isComplete: false,
                    },
                  ]
                  updateMessageImmediate()
                }
                break

              case 'tool_result':
                if (data.tool_call_id && data.tool_result) {
                  currentBlocks = currentBlocks.map((b) => {
                    if (b.type === 'tool' && b.toolCall && b.toolCall.id === data.tool_call_id) {
                      return {
                        ...b,
                        isComplete: true,
                        toolCall: {
                          id: b.toolCall.id,
                          name: b.toolCall.name,
                          arguments: b.toolCall.arguments,
                          result: data.tool_result,
                          completed: true,
                        },
                      }
                    }
                    return b
                  })
                  updateMessageImmediate()
                }
                break

              case 'done':
                if (rafId) cancelAnimationFrame(rafId)
                currentBlocks = currentBlocks.map((b) => ({
                  ...b,
                  isComplete: true,
                  toolCall: b.toolCall
                    ? {
                        id: b.toolCall.id,
                        name: b.toolCall.name,
                        arguments: b.toolCall.arguments,
                        result: b.toolCall.result,
                        completed: true,
                      }
                    : undefined,
                }))
                updateMessageImmediate({
                  content: data.content || currentContent,
                  isStreaming: false,
                })
                setIsLoading(false)
                activeAbortControllerRef.current = null
                void refreshSessionHistory()
                return true

              case 'error':
                setError(data.content)
                setIsLoading(false)
                activeAbortControllerRef.current = null
                void refreshSessionHistory()
                return true

              default:
                break
            }
          } catch (e) {
            console.error('Failed to parse SSE event:', e)
          }
          return false
        }

        let buffer = ''
        let isDone = false

        while (!isDone) {
          const { done, value } = await reader.read()
          if (done) break

          buffer += decoder.decode(value, { stream: true })
          const lines = buffer.split('\n')
          buffer = lines.pop() || ''

          for (const line of lines) {
            if (line.startsWith('data: ')) {
              const eventData = line.slice(6)
              isDone = processEvent(eventData)
              if (isDone) break
            }
          }
        }

        if (buffer.startsWith('data: ')) {
          processEvent(buffer.slice(6))
        }

        if (rafId) cancelAnimationFrame(rafId)
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantMessage.id
              ? { ...m, isStreaming: false }
              : m,
          ),
        )
        setIsLoading(false)
        activeAbortControllerRef.current = null
        void refreshSessionHistory()
      } catch (err) {
        activeAbortControllerRef.current = null
        if (err instanceof DOMException && err.name === 'AbortError') {
          setIsLoading(false)
          setMessages((prev) =>
            prev.map((m) =>
              m.id === assistantMessage.id
                ? { ...m, isStreaming: false }
                : m,
            ),
          )
          void refreshSessionHistory()
          return
        }
        setError('Failed to send message. Please try again.')
        setIsLoading(false)
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantMessage.id
              ? { ...m, isStreaming: false }
              : m,
          ),
        )
        void refreshSessionHistory()
      }
    },
    [apiBase, isLoading, refreshSessionHistory, sessionId],
  )

  const stopStreaming = useCallback(() => {
    if (!activeAbortControllerRef.current) return
    activeAbortControllerRef.current.abort()
    activeAbortControllerRef.current = null
  }, [])

  const retryConnection = useCallback(async () => {
    setError(null)
    localStorage.removeItem(SESSION_STORAGE_KEY)
    await createSession()
  }, [createSession])

  const clearError = useCallback(() => setError(null), [])

  useEffect(() => {
    void refreshSessionHistory()
    // Always begin with a fresh chat session on page load.
    localStorage.removeItem(SESSION_STORAGE_KEY)
    void createSession()
  }, [createSession, refreshSessionHistory])

  return {
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
  }
}
