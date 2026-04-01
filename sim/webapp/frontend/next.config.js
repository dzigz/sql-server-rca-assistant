const DEFAULT_BACKEND_ORIGIN = 'http://127.0.0.1:8000'

function normalizeBackendOrigin(value) {
  if (!value || !value.trim()) return DEFAULT_BACKEND_ORIGIN

  let normalized = value.trim().replace(/\/+$/, '')
  if (normalized.endsWith('/api')) {
    normalized = normalized.slice(0, -'/api'.length)
  }

  return normalized || DEFAULT_BACKEND_ORIGIN
}

const backendOrigin = normalizeBackendOrigin(process.env.NEXT_PUBLIC_API_URL)

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  async rewrites() {
    return {
      beforeFiles: [
        {
          source: '/api/:path*',
          destination: `${backendOrigin}/api/:path*`,
        },
      ],
    }
  },
}

module.exports = nextConfig
