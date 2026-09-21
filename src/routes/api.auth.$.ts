import { createFileRoute } from "@tanstack/react-router";
import { auth } from "@/lib/auth/server";

// All sign-up / sign-in / session requests (/api/auth/*) are handled by Better Auth.
// Users, passwords (hashed) and sessions are stored in Postgres (DATABASE_URL).
export const Route = createFileRoute("/api/auth/$")({
  server: {
    handlers: {
      GET: ({ request }) => auth.handler(request),
      POST: ({ request }) => auth.handler(request),
    },
  },
});
