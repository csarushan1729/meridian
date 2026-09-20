import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center gap-1.5 rounded-sm px-2 py-0.5 font-mono text-xs tracking-wide",
  {
    variants: {
      tone: {
        neutral: "bg-bg-subtle text-muted",
        ok: "bg-ok/12 text-ok",
        warn: "bg-warn/12 text-warn",
        crit: "bg-crit/12 text-crit",
        info: "bg-info/12 text-info",
        accent: "bg-accent/12 text-accent",
      },
    },
    defaultVariants: { tone: "neutral" },
  },
);

export function Badge({
  className,
  tone,
  ...props
}: React.ComponentProps<"span"> & VariantProps<typeof badgeVariants>) {
  return <span className={cn(badgeVariants({ tone }), className)} {...props} />;
}
