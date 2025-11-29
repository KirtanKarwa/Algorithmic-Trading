import { NextResponse } from "next/server"

const BACKEND_URL = process.env.BACKEND_URL || "http://localhost:5000"

export async function POST(req: Request) {
  try {
    const body = await req.json()
    const authHeader = req.headers.get("authorization")
    
    const response = await fetch(`${BACKEND_URL}/api/backtest`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(authHeader && { "Authorization": authHeader }),
      },
      body: JSON.stringify(body),
    })
    
    const data = await response.json()
    
    if (!response.ok) {
      return NextResponse.json({ error: data.message || "Backtest failed" }, { status: response.status })
    }
    
    return NextResponse.json(data, { headers: { "Cache-Control": "no-store" } })
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 })
  }
}

export async function GET(req: Request) {
  try {
    const url = new URL(req.url)
    const strategy = url.searchParams.get("strategy") || "RSI_EMA"
    const authHeader = req.headers.get("authorization")

    const headers: Record<string, string> = {}
    if (authHeader) headers["Authorization"] = authHeader

    const results = await Promise.allSettled([
      fetch(`${BACKEND_URL}/api/equity?strategy=${encodeURIComponent(strategy)}`, { headers }),
      fetch(`${BACKEND_URL}/api/pnl?strategy=${encodeURIComponent(strategy)}`, { headers }),
      fetch(`${BACKEND_URL}/api/trades?strategy=${encodeURIComponent(strategy)}`, { headers }),
    ])

    const [eqRes, pnlRes, trRes] = results

    const equity = eqRes.status === 'fulfilled' ? await eqRes.value.json() : { points: [] }
    const pnl = pnlRes.status === 'fulfilled' ? await pnlRes.value.json() : {}
    const trades = trRes.status === 'fulfilled' ? await trRes.value.json() : { rows: [] }

    const points: Array<{ t: string; v: number }> = Array.isArray(equity?.points) ? equity.points : []
    let totalReturn = "-"
    let maxDD = "-"
    let winRate = "-"
    let sharpe = "-"

    if (points.length >= 2) {
      const start = points[0].v
      const end = points[points.length - 1].v
      if (start && isFinite(start) && isFinite(end)) {
        const tr = ((end - start) / start) * 100
        totalReturn = `${tr.toFixed(2)}%`
      }

      let peak = points[0].v
      let mdd = 0
      for (let i = 1; i < points.length; i++) {
        const v = points[i].v
        if (v > peak) peak = v
        const dd = (v / peak) - 1
        if (dd < mdd) mdd = dd
      }
      maxDD = `${(mdd * 100).toFixed(2)}%`

      const returns: number[] = []
      for (let i = 1; i < points.length; i++) {
        const prev = points[i - 1].v
        const curr = points[i].v
        if (prev && isFinite(prev) && isFinite(curr)) {
          const r = (curr - prev) / prev
          if (isFinite(r)) returns.push(r)
        }
      }
      if (returns.length > 1) {
        const mean = returns.reduce((a, b) => a + b, 0) / returns.length
        const variance = returns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / (returns.length - 1)
        const std = Math.sqrt(variance)
        if (std > 0) {
          const annualized = (mean / std) * Math.sqrt(252)
          sharpe = annualized.toFixed(2)
        }
      }
    }

    const tradeRows: any[] = Array.isArray(trades?.rows) ? trades.rows : []
    if (tradeRows.length > 0) {
      let wins = 0
      let losses = 0
      for (const row of tradeRows) {
        const pnlVal = typeof row.pnl === "number" ? row.pnl : parseFloat(String(row.pnl ?? ""))
        if (!isNaN(pnlVal)) {
          if (pnlVal > 0) wins++
          else losses++
        }
      }
      if (wins + losses > 0) {
        winRate = `${((wins / (wins + losses)) * 100).toFixed(2)}%`
      }
    }

    if (typeof pnl?.change24h === "string" && pnl.change24h.endsWith("%")) {
      totalReturn = pnl.change24h
    }

    const stats = { totalReturn, maxDD, winRate, sharpe }

    return NextResponse.json({ equity: { points }, trades: { rows: tradeRows }, stats }, { headers: { "Cache-Control": "no-store" } })
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 })
  }
}
