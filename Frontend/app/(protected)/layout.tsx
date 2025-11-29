"use client"

import type React from "react"

import { useEffect, useMemo, useState } from "react"
import { useRouter, usePathname } from "next/navigation"
import Link from "next/link"
import { Toaster } from "@/components/ui/toaster"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import {
  Mouse as House,
  Activity,
  FlaskConical,
  Brain,
  BookOpen,
  Settings,
  HelpCircle,
  ChevronRight,
  LogOut,
  WalletMinimal,
  TrendingUp,
  UserCircle2,
} from "lucide-react"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { getToken } from "@/lib/auth"
import { logoutAndRedirect } from "@/lib/logout"

export default function ProtectedLayout({ children }: { children: React.ReactNode }) {
  const router = useRouter()
  const pathname = usePathname()
  const [collapsed, setCollapsed] = useState(false)
  const [pnl, setPnl] = useState(2.4) // mock pnl

  useEffect(() => {
    // simple client-side guard: if no token, send to login
    const token = getToken()
    if (!token) {
      router.replace("/login")
    }
  }, [router])

  const nav = useMemo(
    () => [
      { href: "/dashboard", label: "Dashboard", icon: House },
      { href: "/paper-trading", label: "Live Paper Trading", icon: Activity },
      { href: "/backtesting", label: "Backtesting", icon: FlaskConical },
      { href: "/optimizer", label: "Strategy Optimizer", icon: Brain },
      { href: "/settings", label: "Settings", icon: Settings },
      { href: "/help", label: "Help / About", icon: HelpCircle },
    ],
    [],
  )

  function logout() {
    logoutAndRedirect()
  }

  return (
    <div className="min-h-[100svh] flex bg-background text-foreground">
      {/* Sidebar */}
      <aside
        className={cn(
          "sticky top-0 h-[100svh] border-r border-border bg-secondary/40 backdrop-blur-sm",
          "transition-[width] duration-200",
          collapsed ? "w-[64px]" : "w-[240px]",
        )}
      >
        <div className="h-14 flex items-center justify-between px-3 border-b border-border">
          <Link href="/dashboard" className="flex items-center gap-2">
            <div className="size-6 rounded-md bg-primary/10 grid place-items-center">
              <Brain className="size-4 text-primary" />
            </div>
            {!collapsed && <span className="font-medium">Algorithm Trading Bot</span>}
          </Link>
          <Button
            variant="ghost"
            size="icon"
            className="ml-auto"
            onClick={() => setCollapsed((v) => !v)}
            aria-label="Toggle sidebar"
          >
            <ChevronRight className={cn("size-4 transition-transform", collapsed ? "rotate-180" : "")} />
          </Button>
        </div>

        <nav className="p-2">
          <ul className="grid gap-1">
            {nav.map((item) => {
              const Icon = item.icon
              const active = pathname === item.href
              return (
                <li key={item.href}>
                  <Link
                    href={item.href}
                    className={cn(
                      "flex items-center gap-3 rounded-md px-3 py-2 text-sm",
                      "hover:bg-accent hover:text-accent-foreground",
                      active ? "bg-accent text-accent-foreground" : "text-muted-foreground",
                    )}
                  >
                    <Icon className="size-4 shrink-0" />
                    {!collapsed && <span className="text-pretty">{item.label}</span>}
                  </Link>
                </li>
              )
            })}
          </ul>
          {/* Sidebar Logout */}
          <div className="mt-3 border-t border-border pt-2">
            <Button
              variant="ghost"
              className={cn(
                "w-full flex items-center gap-3 rounded-md px-3 py-2 text-sm text-muted-foreground",
                "hover:bg-accent hover:text-accent-foreground",
              )}
              onClick={logout}
            >
              <LogOut className="size-4 shrink-0" />
              {!collapsed && <span className="text-pretty">Logout</span>}
            </Button>
          </div>
        </nav>
      </aside>

      {/* Content area */}
      <div className="flex-1 min-w-0">
        {/* Topbar */}
        <header className="sticky top-0 z-10 h-14 border-b border-border bg-background/70 backdrop-blur supports-[backdrop-filter]:bg-background/50">
          <div className="h-full container max-w-none mx-0 px-4 flex items-center justify-between">
            <div className="flex items-center gap-3">
              <WalletMinimal className="size-4 text-muted-foreground" />
              <span className="text-sm text-muted-foreground">Portfolio</span>
              <span className="font-medium">$24,532.18</span>
              <span
                className={cn(
                  "rounded px-2 py-0.5 text-xs font-medium",
                  pnl >= 0 ? "bg-green-500/15 text-green-400" : "bg-red-500/15 text-red-400",
                )}
                aria-live="polite"
              >
                {pnl >= 0 ? "+" : ""}
                {pnl}%
              </span>
            </div>

            <div className="flex items-center gap-2">
              <TrendingUp className="size-4 text-muted-foreground" />
              <span className="sr-only">Profile</span>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="ghost" size="sm" className="gap-2">
                    <UserCircle2 className="size-5" />
                    <span className="hidden sm:inline">Profile</span>
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-48">
                  <DropdownMenuItem onClick={() => location.assign("/settings")}>Settings</DropdownMenuItem>
                  <DropdownMenuItem onClick={logout} className="text-red-500">
                    <LogOut className="size-4 mr-2" /> Logout
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </div>
        </header>

        <main className="p-4">{children}</main>
      </div>

      <Toaster />
    </div>
  )
}
