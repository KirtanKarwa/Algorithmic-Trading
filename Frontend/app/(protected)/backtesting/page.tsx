"use client"

import { useState, useMemo, useEffect } from "react"
import useSWR from "swr"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Label } from "@/components/ui/label"
import { Input } from "@/components/ui/input"
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select"
import { Button } from "@/components/ui/button"
import { useToast } from "@/components/ui/use-toast"
import { EquityChart } from "@/components/charts/equity-chart"
import { TradesTable } from "@/components/tables/trades-table"
import { LoadingSpinner, LoadingOverlay, LoadingCard } from "@/components/ui/loading-spinner"
import { fetcher, runBacktest, getEquity, getTrades, getPnl } from "@/lib/fetcher"
import { FileText } from "lucide-react"

function computeClientStats(points: Array<{ t: string; v: number }> = [], tradesRows: any[] = []) {
  let totalReturn = "+0.00%"
  let maxDD = "-"
  let winRate = "-"
  let sharpe = "-"

  if (points.length >= 2) {
    const start = points[0].v
    const end = points[points.length - 1].v
    if (isFinite(start) && isFinite(end) && start !== 0) {
      totalReturn = `${(((end - start) / start) * 100).toFixed(2)}%`
    }

    let peak = points[0].v
    let mdd = 0
    for (let i = 1; i < points.length; i++) {
      const v = points[i].v
      if (v > peak) peak = v
      const dd = v / peak - 1
      if (dd < mdd) mdd = dd
    }
    maxDD = `${(mdd * 100).toFixed(2)}%`

    const returns: number[] = []
    for (let i = 1; i < points.length; i++) {
      const prev = points[i - 1].v
      const curr = points[i].v
      if (isFinite(prev) && isFinite(curr) && prev !== 0) returns.push((curr - prev) / prev)
    }
    if (returns.length > 1) {
      const mean = returns.reduce((a, b) => a + b, 0) / returns.length
      const variance = returns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / (returns.length - 1)
      const std = Math.sqrt(variance)
      if (std > 0) sharpe = ((mean / std) * Math.sqrt(252)).toFixed(2)
    }
  }

  if (Array.isArray(tradesRows) && tradesRows.length > 0) {
    let wins = 0
    let losses = 0
    for (const r of tradesRows) {
      const pnlNum = typeof r.pnl === "number" ? r.pnl : parseFloat(String(r.pnl ?? ""))
      if (!isNaN(pnlNum)) pnlNum > 0 ? wins++ : losses++
    }
    if (wins + losses > 0) winRate = `${((wins / (wins + losses)) * 100).toFixed(2)}%`
  }

  return { totalReturn, maxDD, winRate, sharpe }
}

function formatCurrencyFull(n: number | string | null | undefined) {
  const v = typeof n === "number" ? n : parseFloat(String(n ?? ""))
  if (!isFinite(v)) return "-"
  try { return `$${Math.round(v).toLocaleString()}` } catch { return `$${Math.round(v)}` }
}

function toDisplayDate(iso: string): string {
  // Expect YYYY-MM-DD -> DD/MM/YYYY
  if (!iso || !/\d{4}-\d{2}-\d{2}/.test(iso)) return iso || ""
  const [y, m, d] = iso.split("-")
  return `${d}/${m}/${y}`
}

function toIsoDate(display: string): string | null {
  // Accept DD/MM/YYYY and normalize to YYYY-MM-DD; fallback null if invalid
  const m = display.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/)
  if (!m) return null
  const d = m[1].padStart(2, "0")
  const mo = m[2].padStart(2, "0")
  const y = m[3]
  return `${y}-${mo}-${d}`
}

export default function BacktestingPage() {
  const { toast } = useToast()
  const [symbol, setSymbol] = useState("BTCUSDT")
  const [strategy, setStrategy] = useState("RSI_EMA")
  const [interval, setInterval] = useState("1h")
  const [range, setRange] = useState({ from: "2025-01-01", to: "2025-02-01" })
  const [fromText, setFromText] = useState<string>(toDisplayDate("2025-01-01"))
  const [toText, setToText] = useState<string>(toDisplayDate("2025-02-01"))
  const [localResult, setLocalResult] = useState<any>(null)
  const [isRunning, setIsRunning] = useState(false)
  const swrKey = `/api/backtest?strategy=${encodeURIComponent(strategy)}`
  const { data, mutate, isLoading } = useSWR(swrKey, fetcher, { revalidateOnFocus: false, revalidateIfStale: false, dedupingInterval: 10000 })

  useEffect(() => {
    setFromText(toDisplayDate(range.from))
    setToText(toDisplayDate(range.to))
  }, [range.from, range.to])

  async function run() {
    try {
      setIsRunning(true)
      const result = await runBacktest({ symbol, strategy, interval, range })
      setLocalResult(result)
      await mutate(result, false)
      try {
        const [equity, pnl, trades] = await Promise.all([
          getEquity(strategy),
          getPnl(strategy),
          getTrades(strategy),
        ])
        const stats = result?.stats ?? {
          totalReturn: typeof pnl?.change24h === "string" ? pnl.change24h : "+0.00%",
          maxDD: "-",
          winRate: "-",
          sharpe: "-",
        }
        const composed = { equity, trades, stats }
        setLocalResult(composed)
        await mutate(composed, false)
      } catch {}
      await mutate()
      toast({ title: "Backtest complete", description: "Results are ready." })
    } catch (e: any) {
      toast({ title: "Backtest failed", description: e.message, variant: "destructive" as any })
    } finally {
      setIsRunning(false)
    }
  }

  const effective = localResult || data
  const points = effective?.equity?.points || []
  const clientStats = useMemo(() => computeClientStats(points, effective?.trades?.rows || []), [points, effective?.trades])
  const finalBalanceFallback = points.length ? formatCurrencyFull(points[points.length - 1].v) : "-"
  const stats = {
    finalBalance: effective?.stats?.finalBalance ?? finalBalanceFallback,
    totalReturn: effective?.stats?.totalReturn ?? clientStats.totalReturn,
    maxDD: effective?.stats?.maxDD ?? clientStats.maxDD,
    winRate: effective?.stats?.winRate ?? clientStats.winRate,
    sharpe: effective?.stats?.sharpe ?? clientStats.sharpe,
  }

  return (
    <div className="grid gap-6">
      <Card className="border shadow-lg">
        <CardHeader className="pb-4">
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="text-2xl font-bold">Backtest Control Panel</CardTitle>
              <p className="text-sm text-muted-foreground mt-1">Configure and execute strategy backtests</p>
            </div>
            <Button 
              variant="outline" 
              onClick={() => {
                const logsSection = document.getElementById('trade-logs-section')
                logsSection?.scrollIntoView({ behavior: 'smooth' })
              }}
              className="flex items-center gap-2"
            >
              <FileText className="h-4 w-4" />
              View Logs
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-12 gap-3 items-end">
            {/* Trading Pair */}
            <div className="col-span-12 md:col-span-2 space-y-1">
              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Symbol</Label>
              <Input
                className="h-11 bg-background border-2"
                placeholder="BTCUSDT"
                value={symbol}
                onChange={(e) => setSymbol(e.target.value)}
              />
            </div>

            {/* Strategy */}
            <div className="col-span-12 md:col-span-2 space-y-1">
              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Strategy</Label>
              <Select value={strategy} onValueChange={(v) => { setStrategy(v); setLocalResult(null) }}>
                <SelectTrigger className="h-11 bg-background border-2">
                  <SelectValue placeholder="Select strategy" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="RSI_EMA">RSI EMA</SelectItem>
                  <SelectItem value="MACD">MACD</SelectItem>
                  <SelectItem value="BOLLINGER_RSI">Bollinger RSI</SelectItem>
                  <SelectItem value="SMA_CROSS">SMA Cross</SelectItem>
                  <SelectItem value="VOLUME_BREAKOUT">Volume Breakout</SelectItem>
                  <SelectItem value="BREAKOUT_VOLUME">Breakout Volume</SelectItem>
                  <SelectItem value="PSAR_MACD">PSAR + MACD</SelectItem>
                  <SelectItem value="FIBONACCI_REVERSAL">Fibonacci Reversal</SelectItem>
                  <SelectItem value="TRIX">TRIX</SelectItem>
                  <SelectItem value="HEIKIN_ASHI_EMA">Heikin Ashi + EMA</SelectItem>
                  <SelectItem value="SUPERTREND_RSI">Supertrend + RSI</SelectItem>
                  <SelectItem value="ADX_EMA">ADX + EMA</SelectItem>
                  <SelectItem value="ICHIMOKU">Ichimoku</SelectItem>
                  <SelectItem value="EMA200_PRICE_ACTION">EMA200 Price Action</SelectItem>
                  <SelectItem value="KELTNER_BREAKOUT">Keltner Breakout</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* Timeframe */}
            <div className="col-span-12 md:col-span-2 space-y-1">
              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Interval</Label>
              <Select value={interval} onValueChange={setInterval}>
                <SelectTrigger className="h-11 bg-background border-2">
                  <SelectValue placeholder="Select interval" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="1m">1m</SelectItem>
                  <SelectItem value="5m">5m</SelectItem>
                  <SelectItem value="15m">15m</SelectItem>
                  <SelectItem value="1h">1h</SelectItem>
                  <SelectItem value="4h">4h</SelectItem>
                  <SelectItem value="1d">1d</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* Date Range */}
            <div className="col-span-12 md:col-span-2 space-y-1">
              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">From Date</Label>
              <Input
                className="h-11 border-2 text-center"
                placeholder="DD/MM/YYYY"
                value={fromText}
                onChange={(e) => setFromText(e.target.value)}
                onBlur={() => {
                  const iso = toIsoDate(fromText)
                  if (iso) setRange({ ...range, from: iso })
                }}
              />
            </div>

            <div className="col-span-12 md:col-span-2 space-y-1">
              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">To Date</Label>
              <Input
                className="h-11 border-2 text-center"
                placeholder="DD/MM/YYYY"
                value={toText}
                onChange={(e) => setToText(e.target.value)}
                onBlur={() => {
                  const iso = toIsoDate(toText)
                  if (iso) setRange({ ...range, to: iso })
                }}
              />
            </div>

            {/* Run Button */}
            <div className="col-span-12 md:col-span-2 space-y-1">
              <Label className="text-xs font-bold uppercase tracking-wider text-transparent">Run</Label>
              <Button 
                onClick={run} 
                disabled={isRunning} 
                className="h-11 w-full font-semibold"
              >
                {isRunning ? (
                  <>
                    <LoadingSpinner size="sm" className="mr-2" />
                    Running...
                  </>
                ) : (
                  "Run Backtest"
                )}
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Results Section */}
      <Card>
        <CardHeader>
          <CardTitle className="text-xl">Backtest Results</CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Equity Chart */}
          <div className="space-y-3">
            <h3 className="text-lg font-semibold">Equity Curve</h3>
            <LoadingOverlay isLoading={isLoading && !points.length} message="Loading equity data...">
              {points.length > 0 ? (
                <EquityChart data={points.slice(-1000)} height={420} />
              ) : (
                <div className="h-[420px] rounded-md border flex items-center justify-center text-sm text-muted-foreground">
                  No equity data yet. Run a backtest to see the chart.
                </div>
              )}
            </LoadingOverlay>
          </div>

          {/* Performance Summary */}
          <div className="space-y-3">
            <h3 className="text-lg font-semibold">Performance Summary</h3>
            <div className="rounded-xl border bg-accent/5 p-4 md:p-6">
              <div className="mb-4 flex items-center justify-between">
                <div className="text-sm font-semibold tracking-wide text-muted-foreground">Backtest Summary</div>
                <div className="text-xs text-muted-foreground">Latest run</div>
              </div>
              <div className="grid gap-3 md:grid-cols-5">
                <SummaryItem label="Final Balance" value={stats.finalBalance ?? "-"} />
                <SummaryItem label="Total Return" value={stats.totalReturn} />
                <SummaryItem label="Max Drawdown" value={stats.maxDD} />
                <SummaryItem label="Win Rate" value={stats.winRate} />
                <SummaryItem label="Sharpe Ratio" value={stats.sharpe} />
              </div>
            </div>
          </div>

          {/* Trade Logs */}
          <div id="trade-logs-section" className="space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold">Trade Logs</h3>
              <div className="flex items-center gap-3">
                <div className="text-sm text-muted-foreground">
                  {effective?.trades?.rows?.length || 0} trades executed
                </div>
                <Button 
                  variant="outline" 
                  size="sm"
                  onClick={() => {
                    const trades = effective?.trades?.rows || []
                    if (trades.length === 0) {
                      toast({ title: "No trades to export", variant: "destructive" })
                      return
                    }
                    
                    const headers = ["Time", "Symbol", "Side", "Entry", "Exit", "P/L", "Strategy"]
                    const csv = [
                      headers.join(","),
                      ...trades.map((trade: any) => [
                        trade.time || "",
                        trade.symbol || "",
                        trade.side || "",
                        trade.entry || "",
                        trade.exit || "",
                        trade.pnl || "",
                        trade.strategy || ""
                      ].join(",")),
                    ].join("\n")
                    
                    const blob = new Blob([csv], { type: "text/csv" })
                    const url = URL.createObjectURL(blob)
                    const a = document.createElement("a")
                    a.href = url
                    a.download = `backtest-trades-${strategy}-${new Date().toISOString().split('T')[0]}.csv`
                    a.click()
                    URL.revokeObjectURL(url)
                    toast({ title: "CSV exported successfully" })
                  }}
                  className="flex items-center gap-2"
                >
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                  </svg>
                  Export CSV
                </Button>
              </div>
            </div>
            <LoadingOverlay isLoading={isLoading && !effective?.trades?.rows?.length} message="Loading trade data...">
              {effective?.trades?.rows?.length > 0 ? (
                <div className="overflow-auto rounded-xl border bg-card">
                  <table className="w-full text-sm">
                    <thead className="sticky top-0 z-10 bg-card text-muted-foreground">
                      <tr className="[&>th]:text-left [&>th]:py-3 [&>th]:px-4">
                        <th>Time</th>
                        <th>Symbol</th>
                        <th>Side</th>
                        <th className="text-right">Entry</th>
                        <th className="text-right">Exit</th>
                        <th className="text-right">P/L</th>
                        <th>Strategy</th>
                      </tr>
                    </thead>
                    <tbody>
                      {effective.trades.rows.map((trade: any, idx: number) => (
                        <tr key={`${trade.time}-${idx}`} className="border-t hover:bg-accent/10">
                          <td className="py-3 px-4 whitespace-nowrap">
                            {new Date(trade.time).toLocaleString()}
                          </td>
                          <td className="py-3 px-4 font-medium">{trade.symbol || symbol}</td>
                          <td className="py-3 px-4">
                            <span className={`inline-flex items-center rounded-md px-2 py-1 text-xs font-semibold ${
                              trade.side === 'Long' ? 'bg-emerald-500/15 text-emerald-400' : 'bg-red-500/15 text-red-400'
                            }`}>
                              {trade.side}
                            </span>
                          </td>
                          <td className="py-3 px-4 text-right tabular-nums">
                            ${typeof trade.entry === 'number' ? trade.entry.toFixed(2) : trade.entry}
                          </td>
                          <td className="py-3 px-4 text-right tabular-nums">
                            ${typeof trade.exit === 'number' ? trade.exit.toFixed(2) : trade.exit}
                          </td>
                          <td className={`py-3 px-4 text-right tabular-nums ${
                            Number(trade.pnl) >= 0 ? 'text-green-400' : 'text-red-400'
                          }`}>
                            {typeof trade.pnl === 'number' ? (trade.pnl * 100).toFixed(2) + '%' : trade.pnl}
                          </td>
                          <td className="py-3 px-4">
                            <span className="inline-flex items-center rounded-md border px-2 py-1 text-sm font-semibold text-primary border-primary/30 bg-primary/10">
                              {trade.strategy || strategy}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="h-32 rounded-md border flex items-center justify-center text-sm text-muted-foreground">
                  No trades executed yet. Run a backtest to see trade logs.
                </div>
              )}
            </LoadingOverlay>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function SummaryItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border-2 bg-background p-4 shadow-sm">
      <div className="text-xs font-bold text-muted-foreground uppercase tracking-wide mb-2">{label}</div>
      <div className="text-2xl font-bold text-foreground">{value}</div>
    </div>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-border p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-lg font-semibold">{value}</div>
    </div>
  )
}
