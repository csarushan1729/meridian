import { useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import { SKUS, REGIONS } from "@/lib/cluster/catalog";
import type { RegionId } from "@/lib/cluster/types";
import { placeOrder } from "@/lib/store";
import { fmtMoney } from "@/lib/format";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";

export function PlaceOrderButton({ compact = false }: { compact?: boolean }) {
  const [open, setOpen] = useState(false);
  const [skuId, setSkuId] = useState(SKUS[0]!.id);
  const [region, setRegion] = useState<RegionId>("us-east-1");
  const navigate = useNavigate();
  const sku = SKUS.find((s) => s.id === skuId) ?? SKUS[0]!;

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size={compact ? "sm" : "default"}>Place order</Button>
      </DialogTrigger>
      <DialogContent>
        <DialogTitle>Dispatch an order</DialogTitle>
        <DialogDescription>
          The gateway admits the request, then the orders service runs a saga
          across inventory, payments, fulfillment, and shipping.
        </DialogDescription>
        <form
          className="mt-5 space-y-4"
          onSubmit={(e) => {
            e.preventDefault();
            placeOrder({ skuId, qty: 1, region });
            setOpen(false);
            void navigate({ to: "/orders" });
          }}
        >
          <label className="block">
            <span className="text-xs tracking-wide text-subtle uppercase">SKU</span>
            <select
              value={skuId}
              onChange={(e) => setSkuId(e.target.value)}
              className="mt-1.5 h-11 w-full rounded-sm bg-bg-subtle px-3 text-sm text-fg shadow-[var(--shadow-border)] outline-none focus-visible:ring-2 focus-visible:ring-accent/50"
            >
              {SKUS.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.name} · {fmtMoney(s.price)}
                </option>
              ))}
            </select>
          </label>
          <label className="block">
            <span className="text-xs tracking-wide text-subtle uppercase">Region</span>
            <select
              value={region}
              onChange={(e) => setRegion(e.target.value as RegionId)}
              className="mt-1.5 h-11 w-full rounded-sm bg-bg-subtle px-3 text-sm text-fg shadow-[var(--shadow-border)] outline-none focus-visible:ring-2 focus-visible:ring-accent/50"
            >
              {REGIONS.map((r) => (
                <option key={r.id} value={r.id}>
                  {r.id} · {r.city}
                </option>
              ))}
            </select>
          </label>
          <p className="font-mono text-xs text-muted">
            {sku.name} will charge {fmtMoney(sku.price)} and reserve 1 unit.
          </p>
          <Button type="submit" className="w-full">
            Admit through gateway
          </Button>
        </form>
      </DialogContent>
    </Dialog>
  );
}
