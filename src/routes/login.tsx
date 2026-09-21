import { useEffect, useState, type FormEvent } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { authClient } from "@/lib/auth/client";
import { RedirectToSignIn } from "@/lib/auth/gates";
import { useCurrentUserState } from "@/lib/auth/use-current-user";
import { Button } from "@/components/ui/button";
import { Panel } from "@/components/shared/panel";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/login")({
  head: () => ({ meta: [{ title: "Sign in · Meridian" }] }),
  component: LoginPage,
});

const inputClass =
  "h-11 w-full rounded-sm bg-bg-subtle px-3 text-base text-fg shadow-[var(--shadow-border)] outline-none placeholder:text-subtle focus-visible:ring-2 focus-visible:ring-accent/50 md:text-sm";

type Mode = "signin" | "signup" | "verify";
const RESEND_SECONDS = 30;

/** Turn a Better Auth error into a short sentence a person can act on. */
function friendlyError(error: {
  code?: string;
  message?: string;
  status?: number;
}): string {
  switch (error.code) {
    case "INVALID_OTP":
      return "That code is not correct. Please check it and try again.";
    case "OTP_EXPIRED":
      return "That code has expired. Tap “Send a new code”.";
    case "TOO_MANY_ATTEMPTS":
      return "Too many wrong tries. Tap “Send a new code”.";
  }
  if (error.status === 429)
    return "Too many requests. Please wait a minute and try again.";
  return error.message || "Something went wrong. Please try again.";
}

function LoginPage() {
  const { user } = useCurrentUserState();
  const [mode, setMode] = useState<Mode>("signin");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [code, setCode] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [cooldown, setCooldown] = useState(0);

  // Count down the "send a new code" button.
  useEffect(() => {
    if (cooldown <= 0) return;
    const t = setTimeout(() => setCooldown((c) => c - 1), 1000);
    return () => clearTimeout(t);
  }, [cooldown]);

  // Already signed in (or sign-in is off in local dev): go to the dashboard.
  if (user) return <RedirectToSignIn to="/" />;

  const goToVerify = (message: string) => {
    setCode("");
    setError(null);
    setInfo(message);
    setCooldown(RESEND_SECONDS);
    setMode("verify");
  };

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    setInfo(null);
    setBusy(true);
    try {
      if (mode === "verify") {
        await verifyCode();
        return;
      }
      const cleanEmail = email.trim();
      if (mode === "signup") {
        const res = await authClient.signUp.email({
          name: name.trim() || cleanEmail.split("@")[0] || "User",
          email: cleanEmail,
          password,
        });
        if (res.error) {
          setError(friendlyError(res.error));
        } else if (res.data?.token) {
          window.location.assign("/"); // verification is not required on this server
          return;
        } else {
          goToVerify(`We sent a 6-digit code to ${cleanEmail}.`);
        }
      } else {
        const res = await authClient.signIn.email({
          email: cleanEmail,
          password,
        });
        if (res.error?.code === "EMAIL_NOT_VERIFIED") {
          goToVerify(
            `Your email is not verified yet. We sent a new code to ${cleanEmail}.`,
          );
        } else if (res.error) {
          setError(friendlyError(res.error));
        } else {
          window.location.assign("/");
          return;
        }
      }
    } catch {
      setError("Could not reach the server. Please try again.");
    }
    setBusy(false);
  }

  async function verifyCode() {
    const cleanEmail = email.trim();
    const res = await authClient.emailOtp.verifyEmail({
      email: cleanEmail,
      otp: code,
    });
    if (res.error) {
      setError(friendlyError(res.error));
      setBusy(false);
      return;
    }
    // The server signs you in after a correct code. If it did not, sign in with the password.
    if (!res.data?.token && password) {
      const s = await authClient.signIn.email({ email: cleanEmail, password });
      if (s.error) {
        setError(friendlyError(s.error));
        setBusy(false);
        return;
      }
    }
    window.location.assign("/"); // full page load, so the server renders the dashboard for you
  }

  async function resendCode() {
    setError(null);
    setInfo(null);
    setBusy(true);
    try {
      const res = await authClient.emailOtp.sendVerificationOtp({
        email: email.trim(),
        type: "email-verification",
      });
      if (res.error) setError(friendlyError(res.error));
      else {
        setInfo("A new code is on its way. The old one no longer works.");
        setCooldown(RESEND_SECONDS);
        setCode("");
      }
    } catch {
      setError("Could not reach the server. Please try again.");
    }
    setBusy(false);
  }

  const signup = mode === "signup";
  const verify = mode === "verify";

  return (
    <main className="grid min-h-dvh place-items-center bg-bg p-4">
      <div className="w-full max-w-sm">
        <div className="mb-6 text-center">
          <p className="text-xs tracking-wide text-subtle uppercase">
            Helix production
          </p>
          <h1 className="mt-1 text-2xl font-medium tracking-tight">Meridian</h1>
          <p className="mt-2 text-sm text-muted">
            {verify
              ? "Check your email for the code."
              : signup
                ? "Create an account to open the control plane."
                : "Sign in to open the control plane."}
          </p>
        </div>

        <Panel>
          <form onSubmit={onSubmit} className="space-y-4">
            {verify ? (
              <label className="block space-y-1.5">
                <span className="text-xs text-muted">6-digit code</span>
                <input
                  className={cn(
                    inputClass,
                    "text-center font-mono text-xl tracking-[0.4em] md:text-xl",
                  )}
                  type="text"
                  inputMode="numeric"
                  pattern="[0-9]{6}"
                  maxLength={6}
                  required
                  autoFocus
                  autoComplete="one-time-code"
                  placeholder="······"
                  value={code}
                  onChange={(e) =>
                    setCode(e.target.value.replace(/\D/g, "").slice(0, 6))
                  }
                />
              </label>
            ) : (
              <>
                {signup && (
                  <label className="block space-y-1.5">
                    <span className="text-xs text-muted">Name</span>
                    <input
                      className={inputClass}
                      type="text"
                      autoComplete="name"
                      placeholder="Your name"
                      value={name}
                      onChange={(e) => setName(e.target.value)}
                    />
                  </label>
                )}
                <label className="block space-y-1.5">
                  <span className="text-xs text-muted">Email</span>
                  <input
                    className={inputClass}
                    type="email"
                    required
                    autoComplete="email"
                    placeholder="you@example.com"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                  />
                </label>
                <label className="block space-y-1.5">
                  <span className="text-xs text-muted">Password</span>
                  <input
                    className={inputClass}
                    type="password"
                    required
                    minLength={8}
                    autoComplete={signup ? "new-password" : "current-password"}
                    placeholder={
                      signup ? "At least 8 characters" : "Your password"
                    }
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                  />
                </label>
              </>
            )}

            {info && (
              <p
                role="status"
                className="rounded-sm bg-bg-subtle px-3 py-2 text-sm text-muted"
              >
                {info}
              </p>
            )}
            {error && (
              <p
                role="alert"
                className="rounded-sm bg-crit/15 px-3 py-2 text-sm text-crit"
              >
                {error}
              </p>
            )}

            <Button
              type="submit"
              className="w-full"
              disabled={busy || (verify && code.length !== 6)}
            >
              {busy
                ? "Please wait…"
                : verify
                  ? "Verify and continue"
                  : signup
                    ? "Create account"
                    : "Sign in"}
            </Button>

            {verify && (
              <Button
                type="button"
                variant="ghost"
                className="w-full"
                disabled={busy || cooldown > 0}
                onClick={() => void resendCode()}
              >
                {cooldown > 0
                  ? `Send a new code in ${cooldown}s`
                  : "Send a new code"}
              </Button>
            )}
          </form>
        </Panel>

        <p className="mt-4 text-center text-sm text-muted">
          {verify ? (
            <button
              type="button"
              onClick={() => {
                setMode("signup");
                setError(null);
                setInfo(null);
              }}
              className="cursor-pointer text-fg underline-offset-4 hover:underline"
            >
              Use a different email
            </button>
          ) : (
            <>
              {signup ? "Already have an account?" : "New here?"}{" "}
              <button
                type="button"
                onClick={() => {
                  setMode(signup ? "signin" : "signup");
                  setError(null);
                  setInfo(null);
                }}
                className="cursor-pointer text-fg underline-offset-4 hover:underline"
              >
                {signup ? "Sign in" : "Create an account"}
              </button>
            </>
          )}
        </p>
        <p className="mt-6 text-center text-xs text-subtle">
          Demo project. We only use your email to send the code. Please do not
          reuse a real password.
        </p>
      </div>
    </main>
  );
}
