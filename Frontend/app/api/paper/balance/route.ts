import { NextResponse } from "next/server"

const BACKEND_URL = process.env.BACKEND_URL || "http://localhost:5000"

export async function GET(req: Request) {
  try {
    const authHeader = req.headers.get("authorization")
    const res = await fetch(`${BACKEND_URL}/api/paper/balance`, { headers: { ...(authHeader ? { Authorization: authHeader } : {}) } })
    const data = await res.json()
    return NextResponse.json(data)
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 })
  }
}
