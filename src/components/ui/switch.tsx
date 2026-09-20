import * as SwitchPrimitive from "@radix-ui/react-switch";
import { cn } from "@/lib/utils";

export function Switch({
  className,
  ...props
}: React.ComponentProps<typeof SwitchPrimitive.Root>) {
  return (
    <SwitchPrimitive.Root
      className={cn(
        "peer inline-flex h-6 w-11 shrink-0 items-center rounded-full bg-bg-subtle shadow-[var(--shadow-border)] outline-none transition-[background-color] duration-150 focus-visible:ring-2 focus-visible:ring-accent/50 data-[state=checked]:bg-accent",
        className,
      )}
      {...props}
    >
      <SwitchPrimitive.Thumb className="pointer-events-none block size-5 translate-x-0.5 rounded-full bg-fg transition-transform duration-150 ease-out data-[state=checked]:translate-x-[22px] data-[state=checked]:bg-accent-fg" />
    </SwitchPrimitive.Root>
  );
}
