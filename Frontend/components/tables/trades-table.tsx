import { memo } from "react"

function toNumber(val: any): number | null {
  const n = typeof val === 'number' ? val : parseFloat(String(val ?? ''))
  return isNaN(n) ? null : n
}

function formatNumber(n: any) {
  const v = toNumber(n)
  if (v === null) return ''
  try { return v.toLocaleString(undefined, { maximumFractionDigits: 2 }) } catch { return String(v) }
}

function formatStrategyName(name: string) {
  if (!name) return ''
  // Replace underscores and camel case with spaces, capitalize words
  const spaced = name.replace(/_/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2')
  return spaced
    .split(' ')
    .filter(Boolean)
    .map(w => w[0] ? w[0].toUpperCase() + w.slice(1).toLowerCase() : w)
    .join(' ')
}

function normalizeTrades(
  raw: any[],
  defaults: { symbol?: string; strategy?: string } = {},
): Array<{ time: string; symbol: string; side: string; entry: string; exit: string; pnl: string; strategy: string }> {
  const rows = [...raw].sort((a, b) => new Date(a.time ?? 0).getTime() - new Date(b.time ?? 0).getTime())
  const result: Array<{ time: string; symbol: string; side: string; entry: string; exit: string; pnl: string; strategy: string }> = []

  let openLong: any | null = null
  let openShort: any | null = null

  for (const r of rows) {
    const type = (r.type ?? r.side ?? '').toString().toUpperCase()
    const price = toNumber(r.price ?? r.entry)
    const time = r.time ?? r.timestamp ?? ''
    const symbol = (r.symbol ?? r.pair ?? r.ticker ?? defaults.symbol ?? '').toString()
    const strategy = (r.strategy ?? r.strategyName ?? defaults.strategy ?? '').toString()

    if (type.includes('LONG_ENTRY')) {
      openLong = { time, symbol, entry: price, strategy }
    } else if (type.includes('LONG_EXIT')) {
      const exitPrice = toNumber(r.price ?? r.exit)
      const entry = openLong?.entry ?? toNumber(r.entry)
      const entryTime = openLong?.time ?? time
      const sym = openLong?.symbol ?? symbol
      const strat = openLong?.strategy ?? strategy
      const pnlNum = toNumber(r.pnl)
      const pnl = pnlNum !== null ? pnlNum : (entry && exitPrice ? (exitPrice - entry) / entry : null)
      result.push({
        time: String(entryTime),
        symbol: String(sym ?? ''),
        side: 'Long',
        entry: entry !== null ? String(entry) : String(price ?? ''),
        exit: exitPrice !== null ? String(exitPrice) : String(r.exit ?? ''),
        pnl: pnl !== null ? pnl.toFixed(2) : '',
        strategy: String(strat ?? ''),
      })
      openLong = null
    } else if (type.includes('SHORT_ENTRY')) {
      openShort = { time, symbol, entry: price, strategy }
    } else if (type.includes('SHORT_EXIT')) {
      const exitPrice = toNumber(r.price ?? r.exit)
      const entry = openShort?.entry ?? toNumber(r.entry)
      const entryTime = openShort?.time ?? time
      const sym = openShort?.symbol ?? symbol
      const strat = openShort?.strategy ?? strategy
      const pnlNum = toNumber(r.pnl)
      const pnl = pnlNum !== null ? pnlNum : (entry && exitPrice ? (entry - exitPrice) / entry : null)
      result.push({
        time: String(entryTime),
        symbol: String(sym ?? ''),
        side: 'Short',
        entry: entry !== null ? String(entry) : String(price ?? ''),
        exit: exitPrice !== null ? String(exitPrice) : String(r.exit ?? ''),
        pnl: pnl !== null ? pnl.toFixed(2) : '',
        strategy: String(strat ?? ''),
      })
      openShort = null
    }
  }

  if (openLong) {
    result.push({ time: String(openLong.time), symbol: String(openLong.symbol ?? defaults.symbol ?? ''), side: 'Long', entry: String(openLong.entry ?? ''), exit: '', pnl: '', strategy: String(openLong.strategy ?? defaults.strategy ?? '') })
  }
  if (openShort) {
    result.push({ time: String(openShort.time), symbol: String(openShort.symbol ?? defaults.symbol ?? ''), side: 'Short', entry: String(openShort.entry ?? ''), exit: '', pnl: '', strategy: String(openShort.strategy ?? defaults.strategy ?? '') })
  }

  return result.sort((a, b) => new Date(b.time).getTime() - new Date(a.time).getTime())
}

export const TradesTable = memo(function TradesTable({ trades, defaultSymbol, defaultStrategy }: { trades: any[]; defaultSymbol?: string; defaultStrategy?: string }) {
  const rows = Array.isArray(trades) ? trades : []
  const normalized = normalizeTrades(rows, { symbol: defaultSymbol, strategy: defaultStrategy })
  return (
    <div className="overflow-auto rounded-xl border bg-card">
      <table className="w-full text-sm">
        <thead className="sticky top-0 z-10 bg-card text-muted-foreground">
          <tr className="[&>th]:text-left [&>th]:py-2 [&>th]:px-3">
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
          {normalized.map((r, idx) => (
            <tr key={`${r.time}-${idx}`} className="border-t hover:bg-accent/10">
              <td className="py-2 px-3 whitespace-nowrap">{r.time}</td>
              <td className="py-2 px-3 font-medium">{r.symbol || '-'}</td>
              <td className="py-2 px-3">
                <span className={`inline-flex items-center rounded-md px-2 py-0.5 text-xs font-semibold ${r.side === 'Long' ? 'bg-emerald-500/15 text-emerald-400' : 'bg-red-500/15 text-red-400'}`}>{r.side}</span>
              </td>
              <td className="py-2 px-3 text-right tabular-nums">{formatNumber(r.entry)}</td>
              <td className="py-2 px-3 text-right tabular-nums">{formatNumber(r.exit)}</td>
              <td className={`py-2 px-3 text-right tabular-nums ${Number(r.pnl) >= 0 ? 'text-green-400' : 'text-red-400'}`}>{r.pnl}</td>
              <td className="py-2 px-3">
                <span className="inline-flex items-center rounded-md border px-2 py-1 text-sm font-semibold text-primary border-primary/30 bg-primary/10">{formatStrategyName(r.strategy) || '-'}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
})
