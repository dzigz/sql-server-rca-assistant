'use client'

import { useState, useRef, useEffect, KeyboardEvent } from 'react'
import { Send, Plus, FileText, Image, X } from 'lucide-react'

export interface AttachedFile {
  file: File
  preview?: string  // Object URL for image preview
}

interface MessageInputProps {
  onSend: (message: string, files?: File[]) => void
  disabled?: boolean
  placeholder?: string
}

export function MessageInput({ onSend, disabled, placeholder }: MessageInputProps) {
  const [message, setMessage] = useState('')
  const [attachedFiles, setAttachedFiles] = useState<AttachedFile[]>([])
  const [showMenu, setShowMenu] = useState(false)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const csvInputRef = useRef<HTMLInputElement>(null)
  const imageInputRef = useRef<HTMLInputElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)

  // Auto-resize textarea (max 50vh to prevent taking over screen)
  useEffect(() => {
    const textarea = textareaRef.current
    if (textarea) {
      textarea.style.height = 'auto'
      const maxHeight = window.innerHeight * 0.5
      textarea.style.height = `${Math.min(textarea.scrollHeight, maxHeight)}px`
    }
  }, [message])

  // Close menu when clicking outside
  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setShowMenu(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  // Cleanup object URLs on unmount
  useEffect(() => {
    return () => {
      attachedFiles.forEach(f => {
        if (f.preview) URL.revokeObjectURL(f.preview)
      })
    }
  }, [])

  function handleSend() {
    const trimmed = message.trim()
    if ((trimmed || attachedFiles.length > 0) && !disabled) {
      onSend(trimmed, attachedFiles.length > 0 ? attachedFiles.map(f => f.file) : undefined)
      setMessage('')
      // Cleanup previews
      attachedFiles.forEach(f => {
        if (f.preview) URL.revokeObjectURL(f.preview)
      })
      setAttachedFiles([])
    }
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  function handleFileSelect(e: React.ChangeEvent<HTMLInputElement>) {
    const files = e.target.files
    if (!files) return

    const newFiles: AttachedFile[] = []
    for (let i = 0; i < files.length; i++) {
      const file = files[i]
      const attached: AttachedFile = { file }

      // Create preview URL for images
      if (file.type.startsWith('image/')) {
        attached.preview = URL.createObjectURL(file)
      }

      newFiles.push(attached)
    }

    setAttachedFiles(prev => [...prev, ...newFiles])
    setShowMenu(false)

    // Reset input
    e.target.value = ''
  }

  function removeFile(index: number) {
    setAttachedFiles(prev => {
      const file = prev[index]
      if (file.preview) URL.revokeObjectURL(file.preview)
      return prev.filter((_, i) => i !== index)
    })
  }

  function handleMenuKeyDown(e: KeyboardEvent<HTMLDivElement>) {
    if (e.key === 'Escape') {
      setShowMenu(false)
    }
  }

  const canSend = (message.trim() || attachedFiles.length > 0) && !disabled

  return (
    <div className="space-y-3">
      {/* Attached files preview */}
      {attachedFiles.length > 0 && (
        <div className="flex flex-wrap gap-2 px-1">
          {attachedFiles.map((attached, idx) => (
            <div
              key={idx}
              className="flex items-center gap-2 rounded-lg bg-[var(--muted)] px-2 py-1.5 text-sm"
            >
              {attached.preview ? (
                <img
                  src={attached.preview}
                  alt={attached.file.name}
                  className="w-10 h-10 object-cover rounded"
                />
              ) : (
                <FileText className="w-4 h-4 text-[var(--muted-foreground)]" />
              )}
              <span className="truncate max-w-[120px]">{attached.file.name}</span>
              <button
                type="button"
                onClick={() => removeFile(idx)}
                className="rounded p-0.5 transition-colors hover:bg-[var(--border)]"
              >
                <X className="w-3.5 h-3.5 text-[var(--muted-foreground)]" />
              </button>
            </div>
          ))}
        </div>
      )}

      <div
        className={`
          relative rounded-2xl border border-[var(--border)] bg-[var(--surface)] transition
          ${disabled ? 'opacity-70' : 'focus-within:border-[var(--accent)] focus-within:ring-2 focus-within:ring-[var(--accent)]/20'}
        `}
      >
        {/* File upload button with dropdown */}
        <div
          className="absolute left-2 top-1/2 -translate-y-1/2"
          ref={menuRef}
          onKeyDown={handleMenuKeyDown}
        >
          <div className="relative">
            <button
              type="button"
              onClick={() => setShowMenu(!showMenu)}
              disabled={disabled}
              aria-label="Attach files"
              aria-haspopup="menu"
              aria-expanded={showMenu}
              className={`
                flex h-10 w-10 items-center justify-center rounded-xl
                text-[var(--muted-foreground)] transition-colors
                hover:bg-[var(--muted)] hover:text-[var(--foreground)]
                disabled:opacity-50 disabled:cursor-not-allowed
              `}
            >
              <Plus className="h-5 w-5" />
            </button>

            {/* Dropdown menu */}
            {showMenu && (
              <div
                className="absolute bottom-full left-0 z-10 mb-2 w-52 overflow-hidden rounded-lg border border-[var(--border)] bg-[var(--background)] shadow-lg"
                role="menu"
                aria-label="File upload options"
              >
                <button
                  onClick={() => csvInputRef.current?.click()}
                  className="w-full flex items-center gap-2 px-3 py-2 text-sm transition-colors hover:bg-[var(--muted)]"
                  role="menuitem"
                >
                  <FileText className="w-4 h-4" />
                  Upload CSV File
                </button>
                <button
                  onClick={() => imageInputRef.current?.click()}
                  className="w-full flex items-center gap-2 px-3 py-2 text-sm transition-colors hover:bg-[var(--muted)]"
                  role="menuitem"
                >
                  <Image className="w-4 h-4" />
                  Upload Chart Image
                </button>
              </div>
            )}
          </div>
        </div>

        {/* Hidden file inputs */}
        <input
          ref={csvInputRef}
          type="file"
          accept=".csv"
          multiple
          onChange={handleFileSelect}
          className="hidden"
        />
        <input
          ref={imageInputRef}
          type="file"
          accept="image/*"
          multiple
          onChange={handleFileSelect}
          className="hidden"
        />

        {/* Message input */}
        <textarea
          ref={textareaRef}
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder || 'Type your message...'}
          disabled={disabled}
          aria-label="Chat message input"
          rows={1}
          className={`
            min-h-12 w-full resize-none border-0 bg-transparent pl-14 pr-14 pt-[14px] pb-[12px]
            text-sm leading-5 placeholder:text-[var(--muted-foreground)]
            focus:outline-none
            disabled:opacity-50 disabled:cursor-not-allowed
            overflow-hidden
          `}
        />

        {/* Send button */}
        <div className="absolute right-2 top-1/2 -translate-y-1/2">
          <button
            type="button"
            onClick={handleSend}
            disabled={!canSend}
            aria-label="Send message"
            className={`
              flex h-10 w-10 items-center justify-center rounded-xl
              bg-[var(--accent)] text-[var(--accent-foreground)] transition-colors
              hover:opacity-90
              disabled:opacity-50 disabled:cursor-not-allowed
            `}
          >
            <Send className="h-5 w-5" />
          </button>
        </div>
      </div>
    </div>
  )
}
