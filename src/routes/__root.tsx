import {
  createRootRoute,
  HeadContent,
  Outlet,
  Scripts,
  useLocation,
} from "@tanstack/react-router";
import { AuthProvider } from "@/lib/auth/provider";
import { RedirectToSignIn } from "@/lib/auth/gates";
import { useCurrentUserState } from "@/lib/auth/use-current-user";
import { PreviewHostBridge } from "@/components/preview-host-bridge";
import { AppShell } from "@/components/layout/app-shell";
import { ClusterBridge } from "@/components/layout/cluster-bridge";
import { getClusterSnapshot } from "@/lib/cluster/api";
import { bootSnapshot } from "@/lib/cluster/engine";
import type { Snapshot } from "@/lib/cluster/types";
import appCss from "../styles.css?url";

const APP_NAME = "Meridian";

export const Route = createRootRoute({
  loader: async () => {
    try {
      if (import.meta.env.SSR) {
        // Signed-out visitors get no dashboard data in the page HTML.
        const { requireUserId } = await import("@/lib/auth/verify.server");
        try {
          await requireUserId();
        } catch {
          return null;
        }
        const { getSnapshot } = await import("@/lib/cluster/snapshot.server");
        return await getSnapshot();
      }
      return await getClusterSnapshot();
    } catch (err) {
      if (err instanceof Error && err.message === "Unauthorized") return null;
      console.error("[helix] snapshot loader failed", err);
      return bootSnapshot();
    }
  },
  staleTime: 10_000,
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { title: APP_NAME },
      {
        name: "description",
        content:
          "Meridian control plane — Kafka, Redis, Postgres, and eight commerce services under one live cluster.",
      },
      { name: "theme-color", content: "#0a0b0d" },
    ],
    links: [
      { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" },
      { rel: "stylesheet", href: appCss },
      { rel: "manifest", href: "/__grok/manifest.webmanifest" },
      { rel: "apple-touch-icon", href: "/__grok/icon-180.png" },
      { rel: "preconnect", href: "https://fonts.googleapis.com" },
      { rel: "preconnect", href: "https://fonts.gstatic.com", crossOrigin: "anonymous" },
      {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap",
      },
    ],
  }),
  component: RootComponent,
});

function RootComponent() {
  const initial = Route.useLoaderData();
  return (
    <html lang="en" className="antialiased" suppressHydrationWarning>
      <head>
        <HeadContent />
      </head>
      <body>
        <PreviewHostBridge />
        <AuthProvider>
          <AuthGate initial={initial} />
        </AuthProvider>
        <Scripts />
      </body>
    </html>
  );
}

/** /login is open to everyone. Every other page needs a signed-in user. */
function AuthGate({ initial }: { initial: Snapshot | null }) {
  const { user, isPending } = useCurrentUserState();
  const pathname = useLocation({ select: (l) => l.pathname });

  if (pathname === "/login") return <Outlet />;
  if (isPending) {
    return <div className="grid min-h-dvh place-items-center text-sm text-muted">Loading…</div>;
  }
  if (!user) return <RedirectToSignIn />;
  return (
    <ClusterBridge initial={initial}>
      <AppShell>
        <Outlet />
      </AppShell>
    </ClusterBridge>
  );
}
