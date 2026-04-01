export interface ToolCall {
  id: string
  name: string
  arguments: Record<string, unknown>
  result?: string
  completed?: boolean
}

export interface MessageBlock {
  type: 'thinking' | 'tool' | 'text'
  id: string
  content?: string
  toolCall?: ToolCall
  isComplete?: boolean
}

export interface MessageFile {
  name: string
  type: 'csv' | 'image'
  mediaType: string
  data: string
}

export interface Message {
  id: string
  role: 'user' | 'assistant' | 'system'
  content: string
  blocks: MessageBlock[]
  isStreaming?: boolean
  files?: MessageFile[]
}

export interface BackendMessage {
  role: string
  content: string
  timestamp: string
  blocks?: Array<{
    type: 'thinking' | 'tool' | 'text'
    content?: string
    id?: string
    name?: string
    arguments?: Record<string, unknown>
    result?: string
  }>
}

export type BlitzInstallStatus = 'idle' | 'installing' | 'installed' | 'declined' | 'error'

export interface BlitzInstallState {
  status: BlitzInstallStatus
  message?: string
}

export interface SessionSummary {
  session_id: string
  created_at: string
  message_count: number
  title: string
  last_message_at: string
}
