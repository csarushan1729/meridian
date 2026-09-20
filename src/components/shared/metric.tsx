import { cn } from "@/lib/utils";

export function Metric({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "ok" | "warn" | "crit" | "info";
}) {
  return (
    <div className="min-w-0">
      <div className="text-xs tracking-wide text-subtle uppercase">{label}</div>
      <div
        className={cn(
          "mt-1 font-mono text-xl leading-tight tabular-nums tracking-tight",
          tone === "ok" && "text-ok",
          tone === "warn" && "text-warn",
          tone === "crit" && "text-crit",
          tone === "info" && "text-info",
          !tone && "text-fg",
        )}
      >
        {value}
      </div>
      {hint ? <div className="mt-1 text-xs text-muted">{hint}</div> : null}
    </div>
  );
}
