import { useState } from "react";
import { authEnabled, signOut } from "@/lib/auth/client";
import { useCurrentUser } from "@/lib/auth/use-current-user";
import { Button } from "@/components/ui/button";

/** Who is signed in + sign out. Hidden when sign-in is switched off (local dev user). */
export function AccountMenu() {
  const user = useCurrentUser();
  const [busy, setBusy] = useState(false);
  if (!authEnabled || !user) return null;
  const label = user.displayName ?? user.primaryEmail ?? "Account";

  return (
    <div className="flex items-center gap-2">
      <span
        className="grid size-8 place-items-center rounded-full bg-bg-subtle text-xs font-medium text-fg"
        title={user.primaryEmail ?? label}
      >
        {label.charAt(0).toUpperCase()}
      </span>
      <span className="hidden max-w-32 truncate text-xs text-muted xl:inline">
        {label}
      </span>
      <Button
        variant="ghost"
        size="sm"
        disabled={busy}
        onClick={() => {
          setBusy(true);
          void signOut("/login").catch(() => setBusy(false));
        }}
      >
        {busy ? "Signing out…" : "Sign out"}
      </Button>
    </div>
  );
}
