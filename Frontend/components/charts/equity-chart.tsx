"use client"

import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Area, AreaChart, ReferenceLine } from "recharts"
import { useTheme } from "next-themes"
import { useMemo, memo } from "react"

type Point = { t: string; v: number }

type Props = { data: Point[]; height?: number }

function formatCurrencyShort(value: number) {
  const abs = Math.abs(value)
  if (abs >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(2)}B`
  if (abs >= 1_000_000) return `$${(value / 1_000_000).toFixed(2)}M`
  try {
    return `$${Math.round(value).toLocaleString()}`
  } catch {
    return `$${Math.round(value)}`
  }
}

function formatDateCompact(v: string) {
  const d = new Date(v)
  if (!isNaN(d.getTime())) return d.toLocaleDateString(undefined, { month: "short", day: "2-digit" })
  return v?.length > 10 ? v.slice(5, 10) : v
}

function niceStep(span: number, desiredTicks = 6) {
  if (span <= 0 || !isFinite(span)) return 1
  const raw = span / desiredTicks
  const pow10 = Math.pow(10, Math.floor(Math.log10(raw)))
  const candidates = [1, 2, 5].map((m) => m * pow10)
  let best = candidates[0]
  let bestDiff = Math.abs(raw - best)
  for (const c of candidates) {
    const diff = Math.abs(raw - c)
    if (diff < bestDiff) {
      best = c
      bestDiff = diff
    }
  }
  return best
}

export const EquityChart = memo(function EquityChart({ data, height = 480 }: Props) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === "dark"

  const strokeColor = isDark ? "#ffffff" : "hsl(var(--primary))"
  const fillColor = isDark ? "rgba(255,255,255,0.35)" : "hsl(var(--primary))"
  const axisColor = isDark ? "#ffffff" : "hsl(var(--foreground))"
  const gridColor = isDark ? "rgba(255,255,255,0.15)" : "hsl(var(--border))"

  const hasData = Array.isArray(data) && data.length > 0

  // Convert timestamps to numeric axis for left-anchored time scale
  const chartData = useMemo(() => {
    if (!hasData) return [] as Array<{ x: number; v: number; t: string }>
    return data
      .map(p => {
        const ms = new Date(p.t).getTime()
        return { x: Number.isFinite(ms) ? ms : 0, v: p.v, t: p.t }
      })
      .filter(p => Number.isFinite(p.x) && Number.isFinite(p.v))
  }, [hasData, data])

  const { baseline, domain, ticks } = useMemo(() => {
    if (!hasData) return { baseline: 1000, domain: [900, 1100] as [number, number], ticks: [900, 950, 1000, 1050, 1100] as number[] }
    const baselineVal = Number.isFinite(data[0].v) ? data[0].v : 1000
    let minV = baselineVal
    let maxV = baselineVal
    for (const p of data) {
      if (Number.isFinite(p.v)) {
        if (p.v < minV) minV = p.v
        if (p.v > maxV) maxV = p.v
      }
    }
    const up = Math.max(0, maxV - baselineVal)
    const down = Math.max(0, baselineVal - minV)
    const halfSpan = Math.max(up, down, baselineVal * 0.02)
    const step = niceStep(halfSpan * 2, 6)
    const minBound = Math.floor((baselineVal - halfSpan) / step) * step
    const maxBound = Math.ceil((baselineVal + halfSpan) / step) * step
    const t: number[] = []
    for (let v = minBound; v <= maxBound + 1e-9; v += step) t.push(v)
    return { baseline: baselineVal, domain: [minBound, maxBound] as [number, number], ticks: t }
  }, [hasData, data])

  return (
    <div className="rounded-md border bg-card" style={{ height }}>
      {hasData ? (
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={chartData} margin={{ top: 10, right: 12, left: 6, bottom: 18 }}>
            <CartesianGrid stroke={gridColor} strokeOpacity={isDark ? 1 : 0.15} />
            <XAxis
              dataKey="x"
              type="number"
              scale="time"
              domain={["dataMin", "auto"]}
              padding={{ left: 0, right: 0 }}
              stroke={axisColor}
              tickLine={false}
              axisLine={false}
              minTickGap={28}
              tickFormatter={(v: number) => formatDateCompact(new Date(v).toISOString())}
            />
            <YAxis
              stroke={axisColor}
              tickLine={false}
              axisLine={false}
              domain={domain}
              ticks={ticks}
              width={64}
              tickFormatter={(v: number) => formatCurrencyShort(v)}
            />
            <Tooltip
              contentStyle={{
                background: "hsl(var(--popover))",
                color: "hsl(var(--popover-foreground))",
                border: "1px solid hsl(var(--border))",
              }}
              labelFormatter={(l) => `Time: ${new Date(Number(l)).toLocaleString()}`}
              formatter={(v: number) => [formatCurrencyShort(v), "Equity"]}
            />
            <ReferenceLine y={baseline} stroke={axisColor} strokeDasharray="4 4" ifOverflow="extendDomain" label={{ value: formatCurrencyShort(baseline), position: "right", fill: axisColor }} />
            <Area
              type="monotone"
              dataKey="v"
              stroke={strokeColor}
              strokeWidth={3}
              fill={fillColor}
              fillOpacity={isDark ? 0.35 : 0.35}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="v"
              stroke={strokeColor}
              strokeWidth={3}
              dot={false}
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      ) : (
        <div className="h-full rounded-md flex items-center justify-center text-sm text-muted-foreground">
          No equity points to plot.
        </div>
      )}
    </div>
  )
})
