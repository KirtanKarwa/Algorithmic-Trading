import { NextResponse } from "next/server"

const BACKEND_URL = process.env.BACKEND_URL || "http://localhost:5000"

async function parseBackendResponse(res: Response) {
  const contentType = res.headers.get("content-type") || ""
  if (contentType.includes("application/json")) {
    try {
      const json = await res.json()
      return { json }
    } catch {
      const text = await res.text()
      return { text }
    }
  }
  const text = await res.text()
  return { text }
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization")
    let body: any = null
    try {
      body = await req.json()
    } catch {}
    const res = await fetch(`${BACKEND_URL}/api/optimizer`, {
      method: "POST",
      headers: {
        ...(authHeader ? { Authorization: authHeader } : {}),
        "Content-Type": "application/json",
      },
      body: body ? JSON.stringify(body) : undefined,
    })
    const { json, text } = await parseBackendResponse(res)
    if (!res.ok) {
      return NextResponse.json({ error: (json as any)?.message || text || "Optimizer failed" }, { status: res.status })
    }
    return NextResponse.json(json ?? { ok: true })
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 })
  }
}

export async function GET(req: Request) {
  try {
    const authHeader = req.headers.get("authorization")
    const res = await fetch(`${BACKEND_URL}/api/optimizer`, {
      headers: {
        ...(authHeader ? { Authorization: authHeader } : {}),
      },
      cache: "no-store",
    })
    const { json, text } = await parseBackendResponse(res)
    if (!res.ok) {
      return NextResponse.json({ error: (json as any)?.message || text || "Failed to load optimizer results" }, { status: res.status })
    }
    return NextResponse.json(json ?? { rows: [] }, { headers: { "Cache-Control": "no-store" } })
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 })
  }
}
