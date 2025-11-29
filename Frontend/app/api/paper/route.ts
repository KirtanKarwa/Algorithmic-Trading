import { NextResponse } from "next/server"

const BACKEND_URL = process.env.BACKEND_URL || "http://localhost:5000"

export async function POST(req: Request) {
  try {
    const payload = await req.json()
    const authHeader = req.headers.get("authorization")
    const res = await fetch(`${BACKEND_URL}/api/papertrading`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(authHeader && { Authorization: authHeader }),
      },
      body: JSON.stringify(payload),
    })
    const data = await res.json()
    if (!res.ok) {
      return NextResponse.json({ error: data.message || "Failed to control paper trading" }, { status: res.status })
    }
    return NextResponse.json(data)
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 })
  }
}


