import { NextResponse } from "next/server"

const BACKEND_URL = process.env.BACKEND_URL || "http://localhost:5000"

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url)
    const strategy = searchParams.get("strategy") || "RSI_EMA"
    const authHeader = req.headers.get("authorization")
    
    const response = await fetch(`${BACKEND_URL}/api/trades?strategy=${strategy}`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        ...(authHeader && { "Authorization": authHeader }),
      },
    })
    
    const data = await response.json()
    
    if (!response.ok) {
      return NextResponse.json({ error: data.message || "Failed to fetch trades data" }, { status: response.status })
    }
    
    return NextResponse.json(data)
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 })
  }
}
