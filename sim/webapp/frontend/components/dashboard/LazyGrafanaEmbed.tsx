'use client'

import { useEffect, useRef, useState } from 'react'
import { Loader2 } from 'lucide-react'
import { GrafanaEmbed, type GrafanaEmbedProps } from './GrafanaEmbed'

interface LazyGrafanaEmbedProps extends GrafanaEmbedProps {
  placeholderHeight?: number
}

export function LazyGrafanaEmbed({
  placeholderHeight,
  height = 300,
  ...props
}: LazyGrafanaEmbedProps) {
  const [isVisible, setIsVisible] = useState(false)
  const placeholderRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const node = placeholderRef.current
    if (!node) return

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          setIsVisible(true)
          observer.disconnect()
        }
      },
      {
        rootMargin: '240px 0px',
        threshold: 0.01,
      },
    )

    observer.observe(node)
    return () => observer.disconnect()
  }, [])

  if (!isVisible) {
    return (
      <div
        ref={placeholderRef}
        className="flex items-center justify-center rounded-lg border border-[var(--border)] bg-[var(--surface)]"
        style={{ height: placeholderHeight || height }}
      >
        <div className="flex items-center gap-2 text-xs text-[var(--muted-foreground)]">
          <Loader2 className="h-4 w-4 animate-spin" />
          Loading chart...
        </div>
      </div>
    )
  }

  return <GrafanaEmbed {...props} height={height} />
}
