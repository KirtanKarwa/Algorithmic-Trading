"use client"

import useSWR from "swr"
import { useState, useEffect } from "react"
import { useToast } from "@/components/ui/use-toast"
import { Button } from "@/components/ui/button"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import { LoadingSpinner, LoadingOverlay, LoadingCard } from "@/components/ui/loading-spinner"
import { api, fetcher } from "@/lib/fetcher"

function parsePercent(val: string | number | undefined): number | null {
  if (typeof val === "number") return val
  if (typeof val === "string") {
    const n = parseFloat(val.replace("%", ""))
    return isNaN(n) ? null : n
  }
  return null
}

function PercentCell({ value, invert = false }: { value: string; invert?: boolean }) {
  const n = parsePercent(value)
  if (n === null) return <span className="tabular-nums">{value}</span>
  const signed = invert ? -n : n
  const cls = signed >= 0 ? "text-green-600" : "text-red-600"
  const absStr = Math.abs(n).toFixed(1) + "%"
  return <span className={`tabular-nums ${cls}`}>{absStr}</span>
}

function AbsRedPercent({ value }: { value: string }) {
  const n = parsePercent(value)
  if (n === null) return <span className="tabular-nums text-red-600">{value}</span>
  return <span className="tabular-nums text-red-600">{Math.abs(n).toFixed(1)}%</span>
}

function RankBadge({ rank }: { rank: number }) {
  let cls = "border px-2 py-0.5 text-xs font-semibold rounded-md"
  let label = String(rank)
  if (rank === 1) {
    cls += " bg-yellow-500/20 border-yellow-500/30 text-yellow-400"
  } else if (rank === 2) {
    cls += " bg-slate-400/20 border-slate-400/30 text-slate-300"
  } else if (rank === 3) {
    cls += " bg-amber-800/20 border-amber-800/30 text-amber-500"
  } else {
    cls += " bg-accent/30 border-accent text-foreground/80"
  }
  return <span className={cls}>{label}</span>
}

function toDisplayDate(iso: string): string {
  if (!iso || !/\d{4}-\d{2}-\d{2}/.test(iso)) return iso || ""
  const [y, m, d] = iso.split("-")
  return `${d}/${m}/${y}`
}

function toIsoDate(display: string): string | null {
  const m = display.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/)
  if (!m) return null
  const d = m[1].padStart(2, "0")
  const mo = m[2].padStart(2, "0")
  const y = m[3]
  return `${y}-${mo}-${d}`
}

export default function OptimizerPage() {
  const { toast } = useToast()
  const { data, mutate, isLoading } = useSWR("/api/optimizer", fetcher, { revalidateOnFocus: false, revalidateIfStale: false, dedupingInterval: 10000 })

  const [isRunning, setIsRunning] = useState(false)
  const [localRows, setLocalRows] = useState<any[] | null>(null)
  const [interval, setInterval] = useState("1h")
  const [range, setRange] = useState({ from: "2025-01-01", to: "2025-02-01" })
  const [fromText, setFromText] = useState("01/01/2025")
  const [toText, setToText] = useState("01/02/2025")

  useEffect(() => {
    setFromText(toDisplayDate(range.from))
    setToText(toDisplayDate(range.to))
  }, [range.from, range.to])

  async function run() {
    try {
      setIsRunning(true)
      // Clear previous results immediately for better UX
      setLocalRows([])
      const payload: any = { interval }
      const fromIso = toIsoDate(fromText)
      const toIso = toIsoDate(toText)
      if (fromIso) payload.start = fromIso
      if (toIso) payload.end = toIso
      await api.post("/api/optimizer", payload)
      // Explicitly fetch fresh results to avoid any SWR cache race
      const fresh = await api.get("/api/optimizer")
      setLocalRows((fresh.data as any)?.rows ?? [])
      // also update SWR cache
      await mutate(fresh.data, false)
      toast({ title: "Optimization complete", description: "Top strategies updated." })
    } catch (error) {
      toast({ title: "Optimization failed", description: "Please try again." })
    } finally {
      setIsRunning(false)
    }
  }

  const rows = (localRows ?? data?.rows ?? []) as Array<any>

  return (
    <div className="grid gap-4">
      <div className="flex items-center justify-between rounded-xl border bg-gradient-to-r from-primary/5 via-background to-background px-4 py-3">
        <div>
          <h2 className="text-xl font-semibold">Strategy Optimizer</h2>
          <div className="mt-1 text-xs text-muted-foreground">Interval: <span className="font-semibold">{interval}</span> · Range: <span className="font-semibold">{fromText}</span> → <span className="font-semibold">{toText}</span></div>
        </div>
        <div className="flex items-end gap-3">
          <div className="grid grid-cols-12 gap-3 items-end">
            <div className="col-span-4">
              <label className="block text-xs font-semibold text-muted-foreground mb-1">Interval</label>
              <select className="h-10 w-40 border rounded-md px-2 bg-background shadow-sm" value={interval} onChange={(e) => setInterval(e.target.value)}>
                <option value="1m">1m</option>
                <option value="5m">5m</option>
                <option value="15m">15m</option>
                <option value="1h">1h</option>
                <option value="4h">4h</option>
                <option value="1d">1d</option>
              </select>
            </div>
            <div className="col-span-4">
              <label className="block text-xs font-semibold text-muted-foreground mb-1">From (DD/MM/YYYY)</label>
              <input className="h-10 w-40 border rounded-md px-2 bg-background text-center shadow-sm" value={fromText} onChange={(e) => setFromText(e.target.value)} onBlur={() => { const iso = toIsoDate(fromText); if (iso) setRange({ ...range, from: iso }) }} />
            </div>
            <div className="col-span-4">
              <label className="block text-xs font-semibold text-muted-foreground mb-1">To (DD/MM/YYYY)</label>
              <input className="h-10 w-40 border rounded-md px-2 bg-background text-center shadow-sm" value={toText} onChange={(e) => setToText(e.target.value)} onBlur={() => { const iso = toIsoDate(toText); if (iso) setRange({ ...range, to: iso }) }} />
            </div>
          </div>
          <Button onClick={run} disabled={isRunning} className="h-10 px-5 font-semibold shadow-sm">
          {isRunning ? (
            <>
              <LoadingSpinner size="sm" className="mr-2" />
              Optimizing...
            </>
          ) : (
            "Run Optimization"
          )}
          </Button>
        </div>
      </div>

      <Card className="shadow-lg">
        <CardHeader className="pb-2">
          <CardTitle>Results</CardTitle>
        </CardHeader>
        <CardContent className="overflow-auto">
          <LoadingOverlay isLoading={isLoading && !rows.length} message="Loading optimization results...">
            <div className="rounded-xl border bg-card">
            <table className="w-full text-sm">
              <thead className="sticky top-0 z-10 bg-card/80 backdrop-blur text-muted-foreground">
                <tr className="[&>th]:py-2 [&>th]:px-3">
                  <th className="text-left">Strategy</th>
                  <th className="text-left">Total Return</th>
                  <th className="text-left">Max DD</th>
                  <th className="text-left">Win Rate</th>
                  <th className="text-left">Sharpe</th>
                  <th className="text-left">Rank</th>
                </tr>
              </thead>
              <tbody className="[&>tr:nth-child(odd)]:bg-accent/5">
                {rows.map((r, idx) => (
                  <tr key={`${r.strategy}-${idx}`} className="border-t hover:bg-accent/10 transition-colors">
                    <td className="py-2 px-3">
                      <span className="inline-flex items-center rounded-md border px-2 py-1 text-xs font-semibold">
                        {r.strategy}
                      </span>
                    </td>
                    <td className="py-2 px-3"><PercentCell value={r.totalReturn} /></td>
                    <td className="py-2 px-3"><AbsRedPercent value={r.maxDD} /></td>
                    <td className="py-2 px-3 tabular-nums"><PercentCell value={r.winRate} /></td>
                    <td className="py-2 px-3 tabular-nums">{r.sharpe}</td>
                    <td className="py-2 px-3"><RankBadge rank={Number(r.rank)} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
            </div>
          </LoadingOverlay>
        </CardContent>
      </Card>

      
    </div>
  )
}
