// Frontend/lib/logout.ts
import { clearToken } from '@/lib/auth'
import { clearSessionToken } from '@/lib/session'

export function logoutAndRedirect() {
  clearToken()         // localStorage
  clearSessionToken()  // cookie
  if (typeof window !== 'undefined') {
    window.location.replace('/login')
  }
}