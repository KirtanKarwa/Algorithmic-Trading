"use client"

import { useState } from "react"
import axios from "axios"
import { useTheme } from "next-themes"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Label } from "@/components/ui/label"
import { Input } from "@/components/ui/input"
import { Switch } from "@/components/ui/switch"
import { Button } from "@/components/ui/button"
import { useToast } from "@/components/ui/use-toast"

export default function SettingsPage() {
  const { toast } = useToast()
  const { theme, setTheme } = useTheme()
  const [binanceKey, setBinanceKey] = useState("")
  const [binanceSecret, setBinanceSecret] = useState("")
  const [telegram, setTelegram] = useState(true)
  const [tradeSize, setTradeSize] = useState("100")
  const [strategies, setStrategies] = useState({
    momentum: true,
    meanReversion: true,
    breakout: false,
  })

  async function save() {
    await axios.post("/api/settings", {
      binanceKey,
      binanceSecret,
      telegram,
      tradeSize,
      strategies,
      theme,
    })
    toast({ title: "Settings saved", description: "Configuration updated." })
  }

  return (
    <div className="grid gap-4">
      <Card>
        <CardHeader>
          <CardTitle>Settings</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-6 md:grid-cols-2">
          <div className="grid gap-2">
            <Label>Binance API Key</Label>
            <Input value={binanceKey} onChange={(e) => setBinanceKey(e.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label>Binance Secret</Label>
            <Input type="password" value={binanceSecret} onChange={(e) => setBinanceSecret(e.target.value)} />
          </div>

          {/* Appearance / Theme */}
          <div className="flex items-center justify-between md:col-span-2 rounded-md border p-3">
            <div>
              <div className="font-medium">Dark Mode</div>
              <div className="text-sm text-muted-foreground">Toggle between light and dark themes.</div>
            </div>
            <Switch
              checked={theme === "dark"}
              onCheckedChange={(checked) => setTheme(checked ? "dark" : "light")}
            />
          </div>

          <div className="flex items-center justify-between md:col-span-2 rounded-md border p-3">
            <div>
              <div className="font-medium">Telegram Alerts</div>
              <div className="text-sm text-muted-foreground">Receive trade and status notifications.</div>
            </div>
            <Switch checked={telegram} onCheckedChange={setTelegram} />
          </div>

          <div className="grid gap-2 md:col-span-2">
            <Label>Default Trade Size (USD)</Label>
            <Input type="number" value={tradeSize} onChange={(e) => setTradeSize(e.target.value)} />
          </div>

          <div className="grid gap-3 md:col-span-2">
            <div className="font-medium">Enable Strategies</div>
            {(["momentum", "meanReversion", "breakout"] as const).map((k) => (
              <div key={k} className="flex items-center justify-between rounded-md border p-3">
                <div className="capitalize">{k.replace(/([A-Z])/g, " $1")}</div>
                <Switch
                  checked={(strategies as any)[k]}
                  onCheckedChange={(v) => setStrategies((prev) => ({ ...prev, [k]: v }))}
                />
              </div>
            ))}
          </div>

          <div className="md:col-span-2">
            <Button onClick={save} className="w-full">
              Save
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
