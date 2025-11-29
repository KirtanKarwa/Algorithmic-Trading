import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'

const PROTECTED_PATHS = [
  '/dashboard',
  '/backtesting',
  '/paper-trading',
  '/logs',
  '/optimizer',
  '/settings',
]

export function middleware(req: NextRequest) {
  const { pathname } = req.nextUrl

  // Allow non-protected paths
  const isProtected = PROTECTED_PATHS.some((p) => pathname.startsWith(p))
  if (!isProtected) return NextResponse.next()

  // Read token from cookie (set on login)
  const token = req.cookies.get('token')?.value
  if (!token) {
    const url = req.nextUrl.clone()
    url.pathname = '/login'
    url.searchParams.set('next', pathname) // optional: preserve where they tried to go
    return NextResponse.redirect(url)
  }

  return NextResponse.next()
}

export const config = {
  matcher: [
    '/dashboard/:path*',
    '/backtesting/:path*',
    '/paper-trading/:path*',
    '/logs/:path*',
    '/optimizer/:path*',
    '/settings/:path*',
  ],
}