/**
 * Global iframe registry to preserve iframes across React component remounts.
 *
 * Problem: ReactMarkdown recreates components on every render, causing iframes
 * to reload constantly during streaming.
 *
 * Solution: Keep iframes in a registry outside React's tree. Components request
 * iframes by key, and the registry reuses existing ones instead of creating new ones.
 */

interface IframeEntry {
  iframe: HTMLIFrameElement
  url: string
  refCount: number
  lastUsed: number
  hideTimeout?: ReturnType<typeof setTimeout>  // Delayed hide to avoid move-on-remount
}

class IframeRegistry {
  private iframes: Map<string, IframeEntry> = new Map()
  private container: HTMLDivElement | null = null

  /**
   * Get or create the hidden container for storing unused iframes
   */
  private getContainer(): HTMLDivElement {
    if (!this.container) {
      this.container = document.createElement('div')
      this.container.id = 'iframe-registry-container'
      this.container.style.cssText = 'position: absolute; left: -9999px; top: -9999px; visibility: hidden;'
      document.body.appendChild(this.container)
    }
    return this.container
  }

  /**
   * Generate a unique key for an iframe based on its properties
   */
  getKey(dashboardUid: string, panelId?: number): string {
    return `grafana-${dashboardUid}-${panelId ?? 'full'}`
  }

  /**
   * Get or create an iframe for the given parameters.
   * If an iframe with the same key exists, reuse it.
   */
  acquire(
    key: string,
    url: string,
    onLoad?: () => void,
    onError?: () => void
  ): HTMLIFrameElement {
    const existing = this.iframes.get(key)

    if (existing) {
      // Cancel any pending hide operation
      if (existing.hideTimeout) {
        clearTimeout(existing.hideTimeout)
        existing.hideTimeout = undefined
      }

      existing.refCount++
      existing.lastUsed = Date.now()

      // If URL changed, update it
      if (existing.url !== url) {
        existing.iframe.src = url
        existing.url = url
      }

      return existing.iframe
    }

    // Create new iframe
    const iframe = document.createElement('iframe')
    iframe.src = url
    iframe.width = '100%'
    iframe.height = '100%'
    iframe.frameBorder = '0'
    iframe.style.border = 'none'

    if (onLoad) {
      iframe.addEventListener('load', onLoad, { once: true })
    }
    if (onError) {
      iframe.addEventListener('error', onError, { once: true })
    }

    // Store in registry
    this.iframes.set(key, {
      iframe,
      url,
      refCount: 1,
      lastUsed: Date.now(),
    })

    // Initially add to hidden container
    this.getContainer().appendChild(iframe)

    return iframe
  }

  /**
   * Release an iframe. Just decrement refCount, don't move it.
   */
  release(key: string): void {
    const entry = this.iframes.get(key)
    if (!entry) return

    entry.refCount = Math.max(0, entry.refCount - 1)
  }

  /**
   * Clean up old unused iframes (older than maxAge ms)
   */
  cleanup(maxAge: number = 5 * 60 * 1000): void {
    const now = Date.now()
    Array.from(this.iframes.entries()).forEach(([key, entry]) => {
      if (entry.refCount === 0 && now - entry.lastUsed > maxAge) {
        entry.iframe.remove()
        this.iframes.delete(key)
      }
    })
  }

  /**
   * Get stats for debugging
   */
  getStats(): { total: number; inUse: number } {
    let inUse = 0
    Array.from(this.iframes.values()).forEach(entry => {
      if (entry.refCount > 0) inUse++
    })
    return { total: this.iframes.size, inUse }
  }
}

// Singleton instance
export const iframeRegistry = new IframeRegistry()

// Cleanup old iframes periodically
if (typeof window !== 'undefined') {
  setInterval(() => iframeRegistry.cleanup(), 60 * 1000)
}
