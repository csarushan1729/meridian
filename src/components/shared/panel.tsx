import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export function Panel({
  className,
  children,
}: {
  className?: string;
  children: ReactNode;
}) {
  return (
    <section
      className={cn(
        "rounded-xl bg-bg-elevated p-4 shadow-[var(--shadow-border)]",
        className,
      )}
    >
      {children}
    </section>
  );
}

export function PanelHeader({
  title,
  aside,
}: {
  title: string;
  aside?: ReactNode;
}) {
  return (
    <div className="mb-4 flex items-end justify-between gap-3">
      <h2 className="text-sm font-medium tracking-tight text-fg">{title}</h2>
      {aside}
    </div>
  );
}
