import { NextResponse } from "next/server"

const BACKEND_URL = process.env.BACKEND_URL || "http://localhost:5000"

export async function GET(req: Request) {
  try {
    const url = new URL(req.url)
    const strategy = url.searchParams.get("strategy") || "RSI_EMA"
    const authHeader = req.headers.get("authorization")
    const res = await fetch(`${BACKEND_URL}/api/paper/results?strategy=${encodeURIComponent(strategy)}`, {
      headers: { ...(authHeader ? { Authorization: authHeader } : {}) },
      cache: "no-store",
    })
    const data = await res.json()
    return NextResponse.json(data, { status: res.status, headers: { "Cache-Control": "no-store" } })
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 })
  }
}
