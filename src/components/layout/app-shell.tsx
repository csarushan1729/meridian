import { useState, type ReactNode } from "react";
import { Link, useRouterState } from "@tanstack/react-router";
import {
  Activity,
  Database,
  Globe,
  GitBranch,
  LayoutGrid,
  Menu,
  Radio,
  Share2,
  FlaskConical,
  TriangleAlert,
} from "lucide-react";
import { NAV, MORE_NAV } from "@/lib/nav";
import { useCluster } from "@/lib/store";
import { fmtClock, fmtPct, fmtRps } from "@/lib/format";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet";
import { PlaceOrderButton } from "@/components/layout/place-order";
import { StatusPill } from "@/components/shared/status-pill";

const ICONS = {
  "/": LayoutGrid,
  "/mesh": Share2,
  "/orders": GitBranch,
  "/traces": Activity,
  "/bus": Radio,
  "/platform": Database,
  "/regions": Globe,
  "/chaos": FlaskConical,
  "/incidents": TriangleAlert,
} as const;

export function AppShell({ children }: { children: ReactNode }) {
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  const snapshot = useCluster((s) => s.snapshot);
  const [moreOpen, setMoreOpen] = useState(false);

  return (
    <div className="flex min-h-dvh flex-col bg-bg text-fg">
      <header className="sticky top-0 z-30 border-b border-border bg-bg/95">
        <div className="flex h-14 items-center gap-3 px-3 md:px-5">
          <Link to="/" className="flex items-center gap-2.5 pr-2">
            <MeridianMark />
            <span className="text-sm font-medium tracking-tight">Meridian</span>
          </Link>
          <span className="hidden font-mono text-xs text-subtle md:inline">
            helix-prod
          </span>
          <div className="ml-auto flex items-center gap-2 md:gap-3">
            <StatusPill
              className="hidden sm:inline-flex"
              live
              tone={snapshot.errorRate > 0.08 ? "crit" : "ok"}
              label={`${fmtRps(snapshot.rps)} rps`}
            />
            <span className="hidden font-mono text-xs tabular-nums text-muted lg:inline">
              p99 {Math.round(snapshot.p99)}ms
            </span>
            <span className="hidden font-mono text-xs tabular-nums text-muted lg:inline">
              slo {fmtPct(snapshot.availability, 2)}
            </span>
            <span className="hidden font-mono text-xs tabular-nums text-subtle xl:inline">
              {fmtClock(snapshot.now)}
            </span>
            <PlaceOrderButton compact />
            <Sheet open={moreOpen} onOpenChange={setMoreOpen}>
              <SheetTrigger asChild>
                <Button variant="ghost" size="icon" className="md:hidden">
                  <Menu className="size-5" />
                  <span className="sr-only">Menu</span>
                </Button>
              </SheetTrigger>
              <SheetContent side="bottom">
                <p className="mb-3 text-sm font-medium">Navigate</p>
                <nav className="grid grid-cols-2 gap-2 pb-8">
                  {NAV.map((item) => (
                    <Link
                      key={item.to}
                      to={item.to}
                      onClick={() => setMoreOpen(false)}
                      className={cn(
                        "flex min-h-11 items-center rounded-md px-3 text-sm",
                        pathname === item.to
                          ? "bg-bg-subtle text-fg"
                          : "text-muted",
                      )}
                    >
                      {item.label}
                    </Link>
                  ))}
                </nav>
              </SheetContent>
            </Sheet>
          </div>
        </div>
      </header>

      <div className="flex min-h-0 flex-1">
        <aside className="hidden w-52 shrink-0 border-r border-border md:block">
          <nav className="sticky top-14 flex max-h-[calc(100dvh-3.5rem)] flex-col gap-0.5 overflow-y-auto p-3">
            {NAV.map((item) => {
              const Icon = ICONS[item.to];
              const active = pathname === item.to;
              return (
                <Link
                  key={item.to}
                  to={item.to}
                  className={cn(
                    "flex min-h-11 items-center gap-2.5 rounded-md px-3 text-sm transition-[background-color,color] duration-150",
                    active
                      ? "bg-bg-subtle text-fg"
                      : "text-muted hover:bg-bg-subtle/60 hover:text-fg",
                  )}
                >
                  <Icon className="size-4 shrink-0" />
                  <span>{item.label}</span>
                </Link>
              );
            })}
          </nav>
        </aside>

        <main className="min-w-0 flex-1 px-3 pt-4 pb-24 md:px-6 md:pt-6 md:pb-16">
          {children}
        </main>
      </div>

      {snapshot.ticker.length > 0 ? (
        <div className="pointer-events-none fixed inset-x-0 bottom-14 overflow-hidden border-t border-border bg-bg/90 md:bottom-0">
          <div className="ticker-track flex w-max gap-8 py-2 pl-4 font-mono text-xs text-muted">
            {[...snapshot.ticker, ...snapshot.ticker].map((line, i) => (
              <span key={`${line}-${i}`} className="whitespace-nowrap">
                {line}
              </span>
            ))}
          </div>
        </div>
      ) : null}

      <nav className="fixed inset-x-0 bottom-0 z-20 grid grid-cols-5 border-t border-border bg-bg md:hidden">
        {NAV.slice(0, 4).map((item) => {
          const Icon = ICONS[item.to];
          const active = pathname === item.to;
          return (
            <Link
              key={item.to}
              to={item.to}
              className={cn(
                "flex min-h-14 flex-col items-center justify-center gap-1 text-xs",
                active ? "text-fg" : "text-muted",
              )}
            >
              <Icon className="size-4" />
              {item.label}
            </Link>
          );
        })}
        <button
          type="button"
          onClick={() => setMoreOpen(true)}
          className={cn(
            "flex min-h-14 flex-col items-center justify-center gap-1 text-xs",
            MORE_NAV.some((i) => i.to === pathname) ? "text-fg" : "text-muted",
          )}
        >
          <Menu className="size-4" />
          More
        </button>
      </nav>
    </div>
  );
}

export function MeridianMark() {
  return (
    <svg
      viewBox="0 0 24 24"
      className="size-5 text-accent"
      aria-hidden="true"
    >
      <circle
        cx="12"
        cy="12"
        r="9"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.25"
        opacity="0.55"
      />
      <path
        d="M12 3c3.2 3.4 3.2 14.6 0 18C8.8 17.6 8.8 6.4 12 3Z"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.25"
      />
      <path
        d="M3 12h18"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.25"
        opacity="0.7"
      />
    </svg>
  );
}
