import type { Metadata } from 'next'
import './globals.css'
import { Sidebar } from '@/components/Sidebar'

export const metadata: Metadata = {
  title: 'SQL Server Assistant',
  description: 'AI-powered database performance analysis',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-[var(--background)]">
        <Sidebar />
        <main className="min-h-screen pb-14 md:ml-60 md:pb-0">
          {children}
        </main>
      </body>
    </html>
  )
}
