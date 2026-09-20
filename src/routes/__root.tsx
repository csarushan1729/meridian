import { createRootRoute, HeadContent, Outlet, Scripts } from "@tanstack/react-router";
import { AuthProvider } from "@/lib/auth/provider";
import { PreviewHostBridge } from "@/components/preview-host-bridge";
import { AppShell } from "@/components/layout/app-shell";
import { ClusterBridge } from "@/components/layout/cluster-bridge";
import { getClusterSnapshot } from "@/lib/cluster/api";
import { bootSnapshot } from "@/lib/cluster/engine";
import appCss from "../styles.css?url";

const APP_NAME = "Meridian";

export const Route = createRootRoute({
  loader: async () => {
    try {
      if (import.meta.env.SSR) {
        const { getRuntime } = await import("@/lib/cluster/runtime.server");
        return (await getRuntime()).snapshot();
      }
      return await getClusterSnapshot();
    } catch (err) {
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
          <ClusterBridge initial={initial}>
            <AppShell>
              <Outlet />
            </AppShell>
          </ClusterBridge>
        </AuthProvider>
        <Scripts />
      </body>
    </html>
  );
}
