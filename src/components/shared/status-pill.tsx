import { cn } from "@/lib/utils";

const TONE = {
  ok: "bg-ok",
  warn: "bg-warn",
  crit: "bg-crit",
  info: "bg-info",
  muted: "bg-subtle",
} as const;

export function StatusPill({
  tone,
  label,
  live = false,
  className,
}: {
  tone: keyof typeof TONE;
  label: string;
  live?: boolean;
  className?: string;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-2 font-mono text-xs text-muted",
        className,
      )}
    >
      <span
        className={cn("size-1.5 rounded-full", TONE[tone], live && "live-dot")}
      />
      {label}
    </span>
  );
}

export function healthTone(status: "healthy" | "degraded" | "down") {
  if (status === "healthy") return "ok" as const;
  if (status === "degraded") return "warn" as const;
  return "crit" as const;
}
