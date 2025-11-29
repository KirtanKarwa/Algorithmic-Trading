import axios from "axios"
import { getToken } from "./auth"

// Use Next.js API routes as proxy to backend
const API_BASE_URL = typeof window !== "undefined" ? window.location.origin : "http://localhost:3000"

export const api = axios.create({ baseURL: API_BASE_URL })

api.interceptors.request.use((config) => {
  const token = getToken()
  if (token) config.headers.Authorization = `Bearer ${token}`
  return config
})

api.interceptors.response.use(
  (res) => res,
  (err) => {
    const message = err?.response?.data?.error || err?.response?.data?.message || err?.message || "Request failed"
    return Promise.reject(new Error(message))
  },
)

export async function fetcher(url: string) {
  const res = await api.get(url)
  return res.data
}

// Auth endpoints - use Next.js API routes
export async function loginApi(payload: { email: string; password: string }) {
  const res = await api.post("/api/login", payload)
  return res.data as { token: string }
}

export async function registerApi(payload: { email: string; password: string; name?: string }) {
  const res = await api.post("/api/signup", payload)
  return res.data
}

// Trading/data endpoints - use Next.js API routes
export async function getEquity(strategy?: string) {
  const params = strategy ? `?strategy=${strategy}` : ""
  const res = await api.get(`/api/equity${params}`)
  return res.data
}

export async function getPnl(strategy?: string) {
  const params = strategy ? `?strategy=${strategy}` : ""
  const res = await api.get(`/api/pnl${params}`)
  return res.data
}

export async function getTrades(strategy?: string) {
  const params = strategy ? `?strategy=${strategy}` : ""
  const res = await api.get(`/api/trades${params}`)
  return res.data
}

export async function getPositions() {
  const res = await api.get("/api/positions")
  return res.data
}

export async function runBacktest(payload: any) {
  const res = await api.post("/api/backtest", payload)
  return res.data
}

export async function controlPaperTrading(payload: any) {
  const res = await api.post("/api/paper", payload)
  return res.data
}

// Paper endpoints
export async function getPaperBalance() {
  const res = await api.get("/api/paper/balance")
  return res.data as { balance: number; symbol: string }
}
export async function depositPaper(amount: number) {
  const res = await api.post("/api/paper/deposit", { amount })
  return res.data
}
export async function withdrawPaper(amount: number) {
  const res = await api.post("/api/paper/withdraw", { amount })
  return res.data
}
export async function setPaperSymbol(symbol: string) {
  const res = await api.post("/api/paper/symbol", { symbol })
  return res.data
}
export async function getPaperResults(strategy?: string) {
  const params = strategy ? `?strategy=${encodeURIComponent(strategy)}` : ""
  const res = await api.get(`/api/paper/results${params}`)
  return res.data
}
