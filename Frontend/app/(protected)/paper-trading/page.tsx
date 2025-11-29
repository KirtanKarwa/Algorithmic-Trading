"use client"

import { useEffect, useState } from "react"
import useSWR from "swr"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Label } from "@/components/ui/label"
import { PositionsTable } from "@/components/tables/positions-table"
import { TradesTable } from "@/components/tables/trades-table"
import { EquityChart } from "@/components/charts/equity-chart"
import { LoadingSpinner, LoadingOverlay, LoadingCard } from "@/components/ui/loading-spinner"
import { useToast } from "@/components/ui/use-toast"
import { fetcher, controlPaperTrading, depositPaper, withdrawPaper, setPaperSymbol, getPaperResults, getPaperBalance } from "@/lib/fetcher"
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select"
import { Input } from "@/components/ui/input"
import { FileText } from "lucide-react"

export default function PaperTradingPage() {
  const { toast } = useToast()
  const router = useRouter()
  const [live, setLive] = useState(false)
  const [amount, setAmount] = useState<string>("")
  const [strategy, setStrategy] = useState("RSI_EMA") // Default strategy for paper trading
  const [timeframe, setTimeframe] = useState("15m") // Default timeframe

  const { data: equity, isLoading: equityLoading } = useSWR(`/api/equity?strategy=${encodeURIComponent(strategy)}`, fetcher, { revalidateOnFocus: false, revalidateIfStale: false, dedupingInterval: 10000 })
  const { data: positions, isLoading: positionsLoading } = useSWR("/api/positions", fetcher, { revalidateOnFocus: false, revalidateIfStale: false, dedupingInterval: 10000 })
  const { data: paperTrades, isLoading: tradesLoading } = useSWR(
    ["/api/paper/results", strategy, live ? "live" : "idle"],
    async () => await getPaperResults(strategy),
    { refreshInterval: live ? 2000 : 0, revalidateOnFocus: false }
  )
  const { data: paper, mutate: mutatePaper, isLoading: paperLoading } = useSWR("/api/paper/balance", fetcher, { revalidateOnFocus: false, revalidateIfStale: false, dedupingInterval: 10000 })

  const balance: number = paper?.balance ?? 0
  const symbol: string = paper?.symbol ?? "BTCUSDT"
  const liveFromServer: boolean = !!paper?.live

  // Live equity maintained client-side while paper trading is running
  const [liveEquity, setLiveEquity] = useState<Array<{ t: string; v: number }>>([])
  const [timerId, setTimerId] = useState<any>(null)

  function timeframeToMs(tf: string): number {
    switch (tf) {
      case "1m": return 60_000
      case "5m": return 5 * 60_000
      case "15m": return 15 * 60_000
      case "1h": return 60 * 60_000
      case "4h": return 4 * 60 * 60_000
      case "1d": return 24 * 60 * 60_000
      default: return 60_000
    }
  }

  useEffect(() => {
    if (liveFromServer !== live) setLive(liveFromServer)
  }, [liveFromServer])

  // Manage equity sampling timer when live
  useEffect(() => {
    if (live) {
      // start timer according to timeframe
      const interval = timeframeToMs(timeframe)
      const poll = async () => {
        try {
          const res = await getPaperBalance()
          const now = new Date().toISOString()
          setLiveEquity(prev => [...prev, { t: now, v: Number(res.balance || 0) }].slice(-2000))
        } catch {}
      }
      // immediate sample and schedule
      poll()
      const id = setInterval(poll, interval)
      setTimerId(id)
      return () => { clearInterval(id) }
    } else {
      if (timerId) clearInterval(timerId)
      setTimerId(null)
    }
  }, [live, timeframe])

  const parsedAmount = parseFloat(amount)
  const validAmount = !isNaN(parsedAmount) && parsedAmount > 0
  const canWithdraw = validAmount && parsedAmount <= balance

  async function handleDeposit() {
    if (!validAmount) return toast({ title: "Invalid amount", variant: "destructive" as any })
    try {
      const res = await depositPaper(parsedAmount)
      setAmount("")
      // Optimistic update then revalidate
      await mutatePaper({ balance: res.balance, symbol }, false)
      await mutatePaper()
      toast({ title: "Deposited", description: "Balance updated" })
    } catch (e: any) {
      toast({ title: "Deposit failed", description: e.message, variant: "destructive" as any })
    }
  }

  async function handleWithdraw() {
    if (!canWithdraw) return toast({ title: "Invalid withdraw amount", variant: "destructive" as any })
    try {
      const res = await withdrawPaper(parsedAmount)
      setAmount("")
      // Optimistic update then revalidate
      await mutatePaper({ balance: res.balance, symbol }, false)
      await mutatePaper()
      toast({ title: "Withdrawn", description: "Balance updated" })
    } catch (e: any) {
      toast({ title: "Withdraw failed", description: e.message, variant: "destructive" as any })
    }
  }

  async function handleSymbolChange(v: string) {
    if (live) {
      return toast({ title: "Cannot change symbol while live", variant: "destructive" as any })
    }
    try {
      await setPaperSymbol(v)
      // Optimistic update then revalidate
      await mutatePaper({ balance, symbol: v }, false)
      await mutatePaper()
      toast({ title: "Symbol set", description: v })
    } catch (e: any) {
      toast({ title: "Failed to set symbol", description: e.message, variant: "destructive" as any })
    }
  }

  async function toggle() {
    const next = !live
    try {
      const payload = next 
        ? { action: "start", symbol, strategy, interval: timeframe }
        : { action: "stop" }
      await controlPaperTrading(payload)
      setLive(next)
      await mutatePaper()
      // Clear live equity on start, reset when stopping
      if (next) setLiveEquity([])
      toast({ title: next ? "Paper trading started" : "Paper trading stopped" })
      if (next) {
        // scroll to logs
        setTimeout(() => {
          const el = document.getElementById("paper-trade-logs-section")
          if (el) el.scrollIntoView({ behavior: "smooth", block: "start" })
        }, 50)
      }
    } catch (e: any) {
      toast({ title: "Paper trading error", description: e.message, variant: "destructive" as any })
    }
  }

  return (
    <div className="grid gap-6">
      {/* Control Panel */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle className="text-xl">🎯 Paper Trading Control</CardTitle>
            <div className="flex items-center gap-3">
              <div className={`size-3 rounded-full ${live ? "bg-green-400 animate-pulse" : "bg-red-400"}`} aria-hidden />
              <span className="text-sm font-medium">{live ? "Live Trading" : "Stopped"}</span>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-5">
          {/* Top Row: Balance and Start/Stop */}
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
            {/* Balance Card - Extended */}
            <div className="lg:col-span-9 rounded-xl border-2 bg-gradient-to-br from-primary/10 via-accent/5 to-primary/5 p-6 shadow-sm">
              <div className="flex items-center justify-between mb-4">
                <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Account Balance</Label>
                <div className={`size-2.5 rounded-full animate-pulse ${live ? "bg-green-500" : "bg-gray-400"}`} />
              </div>
              
              <div className="grid grid-cols-5 gap-4 items-center">
                {/* Main Balance */}
                <div className="col-span-2">
                  <div className="text-3xl font-bold tracking-tight mb-1">${balance.toFixed(2)}</div>
                  <div className="flex items-center gap-2 text-xs text-muted-foreground">
                    <span>Initial: $1,000.00</span>
                  </div>
                </div>

                {/* Stats - now horizontal row */}
                <div className="rounded-lg bg-background/50 p-3 border border-border/50">
                  <div className="text-xs text-muted-foreground mb-1">24h Change</div>
                  <div className="text-base font-bold text-green-500">+2.45%</div>
                </div>
                <div className="rounded-lg bg-background/50 p-3 border border-border/50">
                  <div className="text-xs text-muted-foreground mb-1">Total Return</div>
                  <div className="text-base font-bold text-primary">${(balance - 1000).toFixed(2)}</div>
                </div>
                <div className="rounded-lg bg-background/50 p-3 border border-border/50">
                  <div className="text-xs text-muted-foreground mb-1">Return %</div>
                  <div className={`text-base font-bold ${balance >= 1000 ? 'text-green-500' : 'text-red-500'}`}>
                    {(((balance - 1000) / 1000) * 100).toFixed(2)}%
                  </div>
                </div>
              </div>

              <div className="mt-4 flex items-center gap-2">
                <Button 
                  variant="outline" 
                  size="sm"
                  onClick={() => {
                    const el = document.getElementById("paper-trade-logs-section")
                    if (el) el.scrollIntoView({ behavior: "smooth", block: "start" })
                  }}
                  className="flex items-center gap-1.5 h-8 text-xs border-primary/30 hover:bg-primary/10"
                >
                  <FileText className="h-3.5 w-3.5" />
                  View Logs
                </Button>
              </div>
            </div>

            {/* Start/Stop Button */}
            <div className="lg:col-span-3 flex items-center justify-end">
              <Button 
                onClick={toggle} 
                size="lg"
                className={`h-14 px-8 w-full lg:w-auto font-bold text-lg shadow-xl hover:shadow-2xl transition-all duration-300 ${live ? "bg-red-500 hover:bg-red-600 hover:scale-105" : "bg-primary hover:bg-primary/90 hover:scale-105"}`}
              >
                {live ? "Stop Trading" : "Start Trading"}
              </Button>
            </div>
          </div>

          {/* Configuration Section */}
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-4">
            {/* Trading Pair */}
            <div className="space-y-2">
              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">📊 Trading Pair</Label>
              <Select value={symbol} onValueChange={handleSymbolChange} disabled={live}>
                <SelectTrigger className="h-11 bg-background border-2 focus:border-primary shadow-sm" disabled={live}>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {['BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','ADAUSDT','XRPUSDT','USDEUR'].map((s) => (
                    <SelectItem key={s} value={s}>{s}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Strategy */}
            <div className="space-y-2">
              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">🧠 Strategy</Label>
              <Select value={strategy} onValueChange={(v) => { if (!live) setStrategy(v); else toast({ title: "Cannot change strategy while live", variant: "destructive" as any }) }} disabled={live}>
                <SelectTrigger className="h-11 bg-background border-2 focus:border-primary shadow-sm" disabled={live}>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="RSI_EMA">📈 RSI EMA</SelectItem>
                  <SelectItem value="MACD">📊 MACD</SelectItem>
                  <SelectItem value="BOLLINGER_RSI">🎯 Bollinger RSI</SelectItem>
                  <SelectItem value="SMA_CROSS">⚡ SMA Cross</SelectItem>
                  <SelectItem value="VOLUME_BREAKOUT">📊 Volume Breakout</SelectItem>
                  <SelectItem value="BREAKOUT_VOLUME">📊 Breakout Volume</SelectItem>
                  <SelectItem value="PSAR_MACD">🧭 PSAR + MACD</SelectItem>
                  <SelectItem value="FIBONACCI_REVERSAL">🔄 Fibonacci Reversal</SelectItem>
                  <SelectItem value="TRIX">📉 TRIX</SelectItem>
                  <SelectItem value="HEIKIN_ASHI_EMA">🕯️ Heikin Ashi + EMA</SelectItem>
                  <SelectItem value="SUPERTREND_RSI">🛤️ Supertrend + RSI</SelectItem>
                  <SelectItem value="ADX_EMA">📈 ADX + EMA</SelectItem>
                  <SelectItem value="ICHIMOKU">☁️ Ichimoku</SelectItem>
                  <SelectItem value="EMA200_PRICE_ACTION">📏 EMA200 Price Action</SelectItem>
                  <SelectItem value="KELTNER_BREAKOUT">📎 Keltner Breakout</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* Timeframe */}
            <div className="space-y-2">
              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">⏰ Timeframe</Label>
              <Select value={timeframe} onValueChange={(v) => { if (!live) setTimeframe(v); else toast({ title: "Cannot change timeframe while live", variant: "destructive" as any }) }} disabled={live}>
                <SelectTrigger className="h-11 bg-background border-2 focus:border-primary shadow-sm" disabled={live}>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="1m">⚡ 1 Minute</SelectItem>
                  <SelectItem value="5m">🕐 5 Minutes</SelectItem>
                  <SelectItem value="15m">⏰ 15 Minutes</SelectItem>
                  <SelectItem value="1h">🕐 1 Hour</SelectItem>
                  <SelectItem value="4h">🕓 4 Hours</SelectItem>
                  <SelectItem value="1d">📅 1 Day</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* Deposit */}
            <div className="space-y-2">
              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">💵 Deposit</Label>
              <div className="flex gap-2">
                <Input 
                  value={amount} 
                  onChange={(e) => setAmount(e.target.value)} 
                  placeholder="0.00" 
                  type="number"
                  className="h-11 border-2 focus:border-primary shadow-sm flex-1" 
                />
                <Button 
                  onClick={handleDeposit} 
                  disabled={!validAmount || live}
                  className="h-11 px-5 bg-primary hover:bg-primary/90 font-semibold shadow-sm hover:shadow-md transition-all"
                >
                  + Add
                </Button>
              </div>
            </div>

            {/* Withdraw */}
            <div className="space-y-2">
              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">💰 Withdraw</Label>
              <Button 
                onClick={handleWithdraw} 
                disabled={!canWithdraw || live}
                className="h-11 w-full bg-black hover:bg-black/90 text-white disabled:opacity-50 shadow-sm font-semibold"
              >
                Withdraw
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Real-time Equity */}
      <Card>
        <CardHeader>
          <CardTitle className="text-xl">📈 Real-time Equity</CardTitle>
        </CardHeader>
        <CardContent>
          <LoadingOverlay isLoading={equityLoading} message="Loading equity data...">
            <EquityChart data={(live ? liveEquity : (equity?.points || [])).slice(-1000)} height={320} />
          </LoadingOverlay>
        </CardContent>
      </Card>

      {/* Removed duplicate logs-like section to keep a single unified logs area */}
      {/* Trade Logs */}
      <Card>
        <CardHeader>
          <CardTitle className="text-xl">📋 Paper Trading Logs</CardTitle>
        </CardHeader>
        <CardContent>
          <div id="paper-trade-logs-section" className="space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold">Recent Trades</h3>
              <div className="text-sm text-muted-foreground">
                {Array.isArray(paperTrades?.rows) ? paperTrades.rows.length : 0} trades
              </div>
            </div>
            <TradesTable trades={paperTrades?.rows || []} defaultSymbol={symbol} defaultStrategy={strategy} />
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
