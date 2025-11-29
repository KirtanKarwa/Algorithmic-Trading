"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Label } from "@/components/ui/label"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { useToast } from "@/components/ui/use-toast"
import { registerApi, loginApi } from "@/lib/fetcher"
import { setToken } from "@/lib/auth"
import { TrendingUp, Zap, TrendingDown } from "lucide-react"

export default function RegisterPage() {
  const router = useRouter()
  const { toast } = useToast()
  const [name, setName] = useState("")
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [loading, setLoading] = useState(false)

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    try {
      await registerApi({ name, email, password })
      const { token } = await loginApi({ email, password })
      setToken(token)
      router.replace("/dashboard")
    } catch (err: any) {
      toast({ title: "Registration failed", description: err.message, variant: "destructive" as any })
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-[100svh] grid lg:grid-cols-2">
      {/* Left Side - Branding */}
      <div className="hidden lg:flex flex-col justify-center p-8 lg:p-12 bg-gradient-to-br from-primary/10 via-background to-accent/5">
        <div className="max-w-lg mx-auto space-y-8">
          <div className="flex items-center gap-3 mb-4">
            <div className="p-3 rounded-xl bg-primary/10">
              <TrendingUp className="w-8 h-8 text-primary" />
            </div>
            <div>
              <h1 className="text-4xl font-bold bg-gradient-to-r from-primary to-primary/60 bg-clip-text text-transparent">
                Algorithm Trading Bot
              </h1>
              <p className="text-sm text-muted-foreground mt-1">AI-Powered Trading Platform</p>
            </div>
          </div>
          
          <div className="space-y-4">
            <div className="flex items-start gap-4">
              <div className="p-2 rounded-lg bg-primary/10">
                <Zap className="w-5 h-5 text-primary" />
              </div>
              <div>
                <h3 className="font-semibold mb-1">Multiple Strategies</h3>
                <p className="text-sm text-muted-foreground">
                  Choose from 15+ proven trading strategies including RSI, MACD, Bollinger Bands and more
                </p>
              </div>
            </div>
            
            <div className="flex items-start gap-4">
              <div className="p-2 rounded-lg bg-primary/10">
                <TrendingDown className="w-5 h-5 text-primary" />
              </div>
              <div>
                <h3 className="font-semibold mb-1">Risk Management</h3>
                <p className="text-sm text-muted-foreground">
                  Built-in risk management with stop-loss, position sizing, and drawdown controls
                </p>
              </div>
            </div>
          </div>

          <div className="pt-6 space-y-2">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
              <span>Real-time Market Data</span>
            </div>
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
              <span>Portfolio Analytics</span>
            </div>
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
              <span>Performance Tracking</span>
            </div>
          </div>
        </div>
      </div>

      {/* Right Side - Registration Form */}
      <div className="flex items-center justify-center p-4 lg:p-8">
        <div className="w-full max-w-md">
          {/* Mobile Title */}
          <div className="lg:hidden mb-8 text-center">
            <h1 className="text-3xl font-bold bg-gradient-to-r from-primary to-primary/60 bg-clip-text text-transparent mb-2">
              Algorithm Trading Bot
            </h1>
            <p className="text-sm text-muted-foreground">AI-Powered Trading Platform</p>
          </div>

          <Card className="border-2">
            <CardHeader className="space-y-2">
              <CardTitle className="text-2xl">Create Your Account</CardTitle>
              <CardDescription>Start trading with AI-powered strategies today</CardDescription>
            </CardHeader>
            <CardContent>
              <form onSubmit={onSubmit} className="grid gap-4">
                <div className="grid gap-2">
                  <Label htmlFor="name">Full Name</Label>
                  <Input 
                    id="name" 
                    placeholder="John Doe"
                    value={name} 
                    onChange={(e) => setName(e.target.value)} 
                    required 
                    disabled={loading}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor="email">Email</Label>
                  <Input 
                    id="email" 
                    type="email" 
                    placeholder="your.email@example.com"
                    value={email} 
                    onChange={(e) => setEmail(e.target.value)} 
                    required 
                    disabled={loading}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor="password">Password</Label>
                  <Input 
                    id="password" 
                    type="password" 
                    placeholder="Create a strong password"
                    value={password} 
                    onChange={(e) => setPassword(e.target.value)} 
                    required 
                    disabled={loading}
                  />
                </div>
                <Button type="submit" disabled={loading} className="w-full">
                  {loading ? "Creating Account..." : "Create Account"}
                </Button>
                <div className="text-center text-sm text-muted-foreground">
                  Already have an account?{" "}
                  <button
                    type="button"
                    className="text-primary hover:underline font-medium"
                    onClick={() => router.push("/login")}
                  >
                    Sign in
                  </button>
                </div>
              </form>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}


