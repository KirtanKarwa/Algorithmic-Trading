export default function HelpPage() {
  return (
    <div className="prose prose-invert max-w-none">
      <h1 className="text-balance">Help / About</h1>
      <p>
        Algorithm Trading Bot is a modern, modular, and responsive trading dashboard with AI-powered strategies. Test your strategies risk-free with paper trading or analyze historical performance with backtesting.
      </p>
      <ul>
        <li>Use Login/Signup to access the dashboard (frontend-only JWT stored in localStorage).</li>
        <li>Explore Dashboard, Live Paper Trading, Backtesting, Optimizer, Trade Logs, and Settings.</li>
        <li>Replace mock API routes in app/api/* with your backend services when ready.</li>
      </ul>
    </div>
  )
}
