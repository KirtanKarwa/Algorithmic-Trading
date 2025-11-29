export function PositionsTable({ positions }: { positions: any[] }) {
  return (
    <div className="overflow-auto">
      <table className="w-full text-sm">
        <thead className="text-muted-foreground">
          <tr className="[&>th]:text-left [&>th]:py-2">
            <th>Symbol</th>
            <th>Side</th>
            <th>Entry</th>
            <th>Current</th>
            <th>P/L</th>
            <th>Strategy</th>
            <th>Duration</th>
          </tr>
        </thead>
        <tbody className="[&>tr>*]:py-2">
          {positions.map((p) => (
            <tr key={p.id}>
              <td>{p.symbol}</td>
              <td>{p.side}</td>
              <td>{p.entry}</td>
              <td>{p.current}</td>
              <td className={Number(p.pnl) >= 0 ? "text-green-400" : "text-red-400"}>{p.pnl}</td>
              <td>{p.strategy}</td>
              <td>{p.duration}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
