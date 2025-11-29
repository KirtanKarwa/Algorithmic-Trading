"use client"

import { useMemo } from "react"
import useSWR from "swr"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { fetcher } from "@/lib/fetcher"
import { EquityChart } from "@/components/charts/equity-chart"
import { TradesTable } from "@/components/tables/trades-table"
import { Activity, BarChart3, DollarSign, Zap } from "lucide-react"
import { motion } from "framer-motion"
import { Skeleton } from "@/components/ui/skeleton"

export default function DashboardPage() {
  const { data: equity, isLoading: loadingEquity, error: errorEquity } = useSWR("/api/equity", fetcher)
  const { data: pnl, isLoading: loadingPnl } = useSWR("/api/pnl", fetcher)
  const { data: trades, isLoading: loadingTrades } = useSWR("/api/trades", (url) => fetcher(url + "?limit=8"))

  const summary = useMemo(() => {
    const balance = pnl?.balance ?? "$—"
    const change = pnl?.change24h ?? "—"
    const openPositions = pnl?.openPositions ?? "—"
    const tradeCount = pnl?.tradeCount ?? "—"
    return [
      { title: "Total Balance", value: String(balance), icon: DollarSign },
      { title: "24h Change", value: String(change), icon: BarChart3 },
      { title: "Open Positions", value: String(openPositions), icon: Activity },
      { title: "Trades Executed", value: String(tradeCount), icon: Zap },
    ]
  }, [pnl])

  return (
    <div className="grid gap-4">
      <motion.div
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.25 }}
        className="grid gap-4 grid-cols-1 md:grid-cols-4"
      >
        {summary.map((s) => {
          const Icon = s.icon
          return (
            <Card key={s.title}>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">{s.title}</CardTitle>
                <Icon className="size-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{s.value}</div>
              </CardContent>
            </Card>
          )
        })}
      </motion.div>

      <div className="grid gap-4 grid-cols-1 xl:grid-cols-3">
        <Card className="xl:col-span-2">
          <CardHeader>
            <CardTitle>Equity Curve</CardTitle>
          </CardHeader>
          <CardContent>
            {loadingEquity ? <Skeleton className="h-[260px] w-full" /> : <EquityChart data={equity?.points || []} />}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Active Strategies</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-3">
            {["RSI EMA", "MACD", "Bollinger RSI"].map((name, i) => (
              <div key={name} className="flex items-center justify-between rounded-md border border-border p-3">
                <div>
                  <div className="font-medium">{name}</div>
                  <div className="text-xs text-muted-foreground">Status: {i % 2 === 0 ? "Running" : "Idle"}</div>
                </div>
                <div className={`text-sm font-semibold ${i % 2 === 0 ? "text-green-400" : "text-yellow-400"}`}>
                  {i % 2 === 0 ? "+8.3%" : "+1.2%"}
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Recent Trades</CardTitle>
        </CardHeader>
        <CardContent>
          {loadingTrades ? <Skeleton className="h-40 w-full" /> : <TradesTable trades={trades?.rows || []} />}
        </CardContent>
      </Card>
    </div>
  )
}
