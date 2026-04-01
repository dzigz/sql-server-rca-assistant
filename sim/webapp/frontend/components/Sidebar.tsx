'use client'

import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import { Database, BarChart3, MessageSquare } from 'lucide-react'

export function Sidebar() {
  const pathname = usePathname()
  const router = useRouter()
  const isChatActive = pathname === '/'
  const isDashboardActive = pathname === '/dashboard'

  const handleAssistantClick = () => {
    if (pathname === '/') {
      window.dispatchEvent(new CustomEvent('sim:new-chat'))
      return
    }
    router.push('/')
  }

  return (
    <>
      <aside className="fixed inset-y-0 left-0 z-50 hidden w-60 bg-[var(--surface)] md:flex md:flex-col">
        <div className="px-4 pb-4 pt-5">
          <button
            type="button"
            onClick={handleAssistantClick}
            className="inline-flex items-center gap-2 rounded-lg px-1 py-1 text-left transition-colors hover:bg-[var(--muted)]"
            aria-label="Start new chat"
          >
            <Database className="h-5 w-5 text-[var(--accent)]" />
            <span className="font-heading text-base font-semibold">SQL Server Assistant</span>
          </button>
        </div>

        <nav className="flex flex-1 flex-col gap-1 px-3 pb-4" aria-label="Primary Navigation">
          <Link
            href="/"
            onClick={(event) => {
              if (isChatActive) {
                event.preventDefault()
                window.dispatchEvent(new CustomEvent('sim:new-chat'))
              }
            }}
            className={`flex items-center gap-2 rounded-lg px-3 py-2 text-sm transition-colors ${
              isChatActive
                ? 'bg-[var(--accent)]/12 text-[var(--foreground)]'
                : 'text-[var(--muted-foreground)] hover:bg-[var(--muted)] hover:text-[var(--foreground)]'
            }`}
          >
            <MessageSquare className="h-4 w-4" />
            Chat
          </Link>

          <Link
            href="/dashboard"
            className={`flex items-center gap-2 rounded-lg px-3 py-2 text-sm transition-colors ${
              isDashboardActive
                ? 'bg-[var(--accent)]/12 text-[var(--foreground)]'
                : 'text-[var(--muted-foreground)] hover:bg-[var(--muted)] hover:text-[var(--foreground)]'
            }`}
          >
            <BarChart3 className="h-4 w-4" />
            Dashboard
          </Link>
        </nav>
      </aside>

      <nav
        className="fixed inset-x-0 bottom-0 z-50 grid grid-cols-2 border-t border-[var(--border)] bg-[var(--surface)] md:hidden"
        aria-label="Mobile Navigation"
      >
        <Link
          href="/"
          className={`flex flex-col items-center justify-center gap-1 py-2 text-xs ${
            isChatActive ? 'text-[var(--accent)]' : 'text-[var(--muted-foreground)]'
          }`}
        >
          <MessageSquare className="h-4 w-4" />
          Chat
        </Link>
        <Link
          href="/dashboard"
          className={`flex flex-col items-center justify-center gap-1 py-2 text-xs ${
            isDashboardActive ? 'text-[var(--accent)]' : 'text-[var(--muted-foreground)]'
          }`}
        >
          <BarChart3 className="h-4 w-4" />
          Dashboard
        </Link>
      </nav>
    </>
  )
}
