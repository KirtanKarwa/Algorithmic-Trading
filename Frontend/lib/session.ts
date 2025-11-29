// Frontend/lib/session.ts
export function setSessionToken(token: string) {
    // 7 days expiry; adjust as you like
    document.cookie = `token=${token}; path=/; max-age=${7 * 24 * 60 * 60}; SameSite=Lax`
  }
  
  export function clearSessionToken() {
    document.cookie = 'token=; path=/; max-age=0; SameSite=Lax'
  }