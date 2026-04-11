"""
UChicago Trading Competition 2026 — Case 1: Market Making
=========================================================

Strategy overview
-----------------
A    : earnings sweep (primary edge) + passive MM with OFI adjustment
C    : sweep + passive MM driven by Fed-probability-dependent FV
       BETA / GAMMA fitted online via non-blocking gradient descent
ETF  : NAV arb (create / redeem) triggered on every A or C FV update
Opts : put-call parity + box-spread arb on B options (size 5)
Fed  : conviction-scaled prediction-market trading + end-of-round max-pos

Key architectural decisions
----------------------------
- place_safe / cancel_symbol  : centralised limit checks, one place to patch
- asyncio.create_task         : cross-asset triggers never block the hot path
- loop.run_in_executor        : heavy gradient descent stays off the gRPC thread
- LIMITS dict                 : update ONE dict on competition day
"""

from typing import Optional
from utcxchangelib.xchange_client import XChangeClient, Side
import asyncio
import math

# ══════════════════════════════════════════════════════════════════════
#  KNOWN PARAMETERS  (from competition staff)
# ══════════════════════════════════════════════════════════════════════
PE_A       = 10       # Stock A: constant P/E ratio (confirmed by staff)
PE0_C      = 14.0     # Stock C: baseline P/E when Δy = 0
Y0         = 0.045    # baseline yield
B0_OVER_N  = 40.0     # bond portfolio value per share ($)
DUR        = 7.5      # bond duration  (D in formula)
CONV_C     = 55.0     # bond convexity (C in formula, renamed to avoid shadowing)
LAMBDA_C   = 0.65     # bond weight in C's price
RF_RATE    = 0        # risk-free rate (confirmed 0)
ETF_COST   = 5        # swap cost per ETF unit (cents)

OPTION_STRIKES = [950, 1000, 1050]
BOX_PAIRS      = [(950, 1000), (1000, 1050)]
FED_CONTRACTS  = ["R_HIKE", "R_HOLD", "R_CUT"]

ALL_SYMBOLS = [
    "A", "B",
    "B_C_950", "B_P_950", "B_C_1000", "B_P_1000", "B_C_1050", "B_P_1050",
    "C", "ETF",
    "R_HIKE", "R_HOLD", "R_CUT",
]

# ══════════════════════════════════════════════════════════════════════
#  RISK LIMITS  ← patch this dict on competition day, nowhere else
# ══════════════════════════════════════════════════════════════════════
LIMITS: dict[str, dict[str, int]] = {
    s: {"order": 40, "open": 50, "outvol": 120, "pos": 200}
    for s in ALL_SYMBOLS
}


# ══════════════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ══════════════════════════════════════════════════════════════════════

async def place_safe(
    client: "MyXchangeClient",
    symbol: str,
    qty: int,
    side: Side,
    price: int,
) -> Optional[str]:
    """
    Place an order with full risk-limit enforcement.
    Returns order_id or None if any limit would be breached.
    This is the ONLY place that calls client.place_order for normal orders.
    """
    if qty <= 0:
        return None
    lim = LIMITS[symbol]

    # 1. Per-order size cap
    qty = min(qty, lim["order"])

    # 2. Outstanding volume cap
    outvol = sum(
        info[1]
        for info in client.open_orders.values()
        if info and info[0].symbol == symbol and not info[2]
    )
    qty = min(qty, lim["outvol"] - outvol)
    if qty <= 0:
        return None

    # 3. Open-order count cap
    n_open = sum(
        1
        for info in client.open_orders.values()
        if info and info[0].symbol == symbol and not info[2]
    )
    if n_open >= lim["open"]:
        return None

    # 4. Absolute position cap
    pos = client.positions.get(symbol, 0)
    if side == Side.BUY:
        qty = min(qty, lim["pos"] - pos)
    else:
        qty = min(qty, lim["pos"] + pos)
    if qty <= 0:
        return None

    return await client.place_order(symbol, qty, side, price)


async def cancel_symbol(
    client: "MyXchangeClient",
    symbol: str,
    pending: set,
) -> None:
    """Cancel every open order for *symbol* not already pending cancellation."""
    targets = [
        oid
        for oid, info in client.open_orders.items()
        if info and info[0].symbol == symbol and oid not in pending
    ]
    for oid in targets:
        pending.add(oid)
        await client.cancel_order(oid)


# ══════════════════════════════════════════════════════════════════════
#  STOCK A  — earnings sweep + passive MM + OFI
# ══════════════════════════════════════════════════════════════════════

class StockAStrategy:
    """
    Primary edge: the moment EPS arrives, every resting order at the old
    fair value is mispriced — we cancel our quotes and sweep the full book.

    Secondary: passive market-making around FV with:
      - inventory skew     (shift mid toward reducing position)
      - dynamic spread     (widen as |pos| grows)
      - OFI adjustment     (tilt mid toward order-flow pressure)
    """

    def __init__(self, client: "MyXchangeClient"):
        self.client = client

        # ── Passive MM params ──────────────────────────────
        self.base_spread    = 4
        self.skew_per_unit  = 0.15
        self.quote_size     = 10
        self.max_soft_pos   = 100   # start skewing toward flat above this

        # ── Aggressive sweep params ───────────────────────
        self.edge_threshold      = 2   # cents below FV to consider "stale"
        self.snipe_size          = 20
        self.post_earnings_ticks = 8   # extra-aggressive for this many MM cycles

        # ── Order-flow imbalance ──────────────────────────
        self._ofi_hist:  list[int] = []
        self._ofi_window = 20
        self._ofi_scale  = 0.5      # max OFI shift = 0.5 × base_spread

        # ── Internal state ────────────────────────────────
        self.earnings_countdown    = 0
        self.pending_cancels: set  = set()
        self._last_snipe_t         = 0.0
        self._last_bid             = None   # (price, size) last placed
        self._last_ask             = None

    # ── Fair value ──────────────────────────────────────────────────
    @staticmethod
    def calc_fv(eps: float) -> int:
        """FV in cents: EPS × PE × 100. No annualising (confirmed by staff)."""
        return round(eps * PE_A * 100)

    # ── Order-flow imbalance ─────────────────────────────────────────
    def record_trade(self, price: int, qty: int) -> None:
        """Classify a market trade as buyer- or seller-initiated."""
        bb, ba = self.client.get_best_bid_ask("A")
        if bb is None or ba is None:
            return
        direction = 1 if price >= (bb + ba) / 2 else -1
        self._ofi_hist.append(direction * qty)
        if len(self._ofi_hist) > self._ofi_window:
            self._ofi_hist.pop(0)

    def _ofi_adj(self) -> float:
        if not self._ofi_hist:
            return 0.0
        net   = sum(self._ofi_hist)
        total = sum(abs(x) for x in self._ofi_hist)
        return 0.0 if total == 0 else self._ofi_scale * (net / total) * self.base_spread

    # ── Sweep ────────────────────────────────────────────────────────
    async def _sweep(self) -> None:
        """Pick off every resting order mispriced vs current FV."""
        fv = self.client.fair_values.get("A")
        if fv is None:
            return
        book = self.client.order_books["A"]

        # Stale asks below FV — buy them
        for px, qty in sorted(
            [(px, q) for px, q in book.asks.items() if q > 0 and px < fv - self.edge_threshold],
            key=lambda x: x[0],   # cheapest first
        ):
            oid = await place_safe(self.client, "A", min(qty, self.snipe_size), Side.BUY, px)
            if oid is None:
                break
            print(f"[A] SWEEP BUY  {min(qty, self.snipe_size):>3}@{px}  edge={fv-px:.0f}")

        # Stale bids above FV — sell them
        for px, qty in sorted(
            [(px, q) for px, q in book.bids.items() if q > 0 and px > fv + self.edge_threshold],
            key=lambda x: -x[0],  # most expensive first
        ):
            oid = await place_safe(self.client, "A", min(qty, self.snipe_size), Side.SELL, px)
            if oid is None:
                break
            print(f"[A] SWEEP SELL {min(qty, self.snipe_size):>3}@{px}  edge={px-fv:.0f}")

    # ── Public interface ─────────────────────────────────────────────
    async def on_earnings(self, eps: float) -> None:
        """New EPS arrived: update FV, cancel quotes, sweep book."""
        prev_fv = self.client.fair_values.get("A")
        self.client.fair_values["A"] = self.calc_fv(eps)
        fv    = self.client.fair_values["A"]
        delta = fv - prev_fv if prev_fv else 0
        print(f"[A] *** EARNINGS  EPS={eps:.4f}  FV={fv}  Δ={delta:+.0f}  pos={self.client.positions.get('A', 0)}")

        self.earnings_countdown = self.post_earnings_ticks
        self._last_bid = self._last_ask = None       # force re-quote

        await cancel_symbol(self.client, "A", self.pending_cancels)
        await self._sweep()
        # ETF NAV changed: trigger arb check asynchronously
        asyncio.create_task(self.client.strat_etf.check_arb())

    async def on_book_update(self) -> None:
        """Rate-limited book snipe: faster during post-earnings window."""
        fv = self.client.fair_values.get("A")
        if fv is None:
            return
        now  = asyncio.get_event_loop().time()
        rate = 0.15 if self.earnings_countdown > 0 else 0.40
        if now - self._last_snipe_t < rate:
            return
        self._last_snipe_t = now
        await self._sweep()

    async def update_quotes(self) -> None:
        """Called every trade-loop iteration (~100 ms). Re-quotes if FV or pos changed."""
        fv = self.client.fair_values.get("A")
        if fv is None:
            # Before first earnings: use market mid as FV proxy
            bb, ba = self.client.get_best_bid_ask("A")
            if bb is not None and ba is not None:
                fv = (bb + ba) / 2
                self.client.fair_values["A"] = fv
            else:
                return

        if self.earnings_countdown > 0:
            self.earnings_countdown -= 1

        pos    = self.client.positions.get("A", 0)
        bb, ba = self.client.get_best_bid_ask("A")

        # OFI-tilted mid
        mid = fv + self._ofi_adj()

        # Spread widens with position size
        pos_frac = abs(pos) / max(self.max_soft_pos, 1)
        spread   = self.base_spread + int(pos_frac * 4)
        skew     = self.skew_per_unit * pos     # positive pos → shift quotes down

        bid = round(mid - spread / 2 - skew)
        ask = round(mid + spread / 2 - skew)
        if bid >= ask:
            ask = bid + 1

        # At hard position limits: force unwind at market
        pos_lim = LIMITS["A"]["pos"]
        if pos >= pos_lim and bb is not None:
            ask = bb          # sell at best bid to guarantee fill
        if pos <= -pos_lim and ba is not None:
            bid = ba          # buy at best ask to guarantee fill

        # Don't quote at a price that would be immediately filled at a loss
        if abs(pos) < pos_lim:
            if ba is not None and bid >= ba:
                bid = 0
            if bb is not None and ask <= bb:
                ask = 0

        # Size: lean toward reducing exposure when near soft limit
        bsz = self.quote_size if pos <  self.max_soft_pos else max(2, self.quote_size // 3)
        asz = self.quote_size if pos > -self.max_soft_pos else max(2, self.quote_size // 3)

        # Skip cancel+replace if nothing changed
        if (bid, bsz) == self._last_bid and (ask, asz) == self._last_ask:
            return
        self._last_bid = (bid, bsz)
        self._last_ask = (ask, asz)

        await cancel_symbol(self.client, "A", self.pending_cancels)

        if bid > 0:
            oid = await place_safe(self.client, "A", bsz, Side.BUY, bid)
            if oid:
                self.client.my_quote_ids.add(oid)
        if ask > 0:
            oid = await place_safe(self.client, "A", asz, Side.SELL, ask)
            if oid:
                self.client.my_quote_ids.add(oid)

        print(f"[A] MM  fv={fv:.0f}  ofi={self._ofi_adj():+.1f}  pos={pos}  spread={spread}  "
              f"bid={bid}×{bsz}  ask={ask}×{asz}")

    def on_cancel_confirmed(self, oid: str) -> None:
        self.pending_cancels.discard(oid)

    def on_order_rejected(self, oid: str) -> None:
        self.pending_cancels.discard(oid)


# ══════════════════════════════════════════════════════════════════════
#  STOCK C  — Fed-coupled FV, online parameter fitting, sweep + MM
# ══════════════════════════════════════════════════════════════════════

class StockCStrategy:
    """
    C's fair value depends on two unknown parameters:
      BETA  — converts expected rate change (bps) → yield change (fraction)
      GAMMA — determines P/E sensitivity to yield change

    Before commitment:
      - observe (ERC, C_mid, EPS) tuples; fit BETA/GAMMA asynchronously
      - quote passively around market mid with WIDE spread (uncertainty premium)

    After commitment:
      - FV is computed from the full model every time Fed probs change
      - sweep stale orders when FV moves significantly (CPI, FEDSPEAK, R trades)
      - passive MM around model FV with narrower spread
    """

    # ── Fit hyperparameters ───────────────────────────────────────────
    MIN_OBS        = 10    # minimum observations before first fit
    FIT_EVERY      = 5     # re-fit every N new observations
    STABILITY_WIN  = 3     # require 3 consecutive stable fits before committing
    BETA_INIT      = 0.0001   # prior: 1 bps rate change ≈ 0.01% yield change
    GAMMA_INIT     = 30.0     # prior: moderate P/E sensitivity
    LR_BETA        = 1e-10
    LR_GAMMA       = 1e-6
    N_ITER         = 300

    # ── Quote threshold ───────────────────────────────────────────────
    # Only re-quote C if model FV has moved by at least this many cents
    FV_MOVE_THRESH = 8     # cents

    def __init__(self, client: "MyXchangeClient"):
        self.client = client

        # Fitted parameters (None until committed)
        self.beta:  Optional[float] = None
        self.gamma: Optional[float] = None
        self._committed   = False
        self._fitting     = False
        self._observations: list[tuple] = []   # (erc, c_mid, eps)
        self._obs_since_fit   = 0
        self._recent_fits: list[tuple] = []    # recent (beta, gamma) for stability check

        # ── MM params (wider than A — more model uncertainty) ─────────
        self.base_spread   = 10
        self.skew_per_unit = 0.10
        self.quote_size    = 8
        self.max_soft_pos  = 80
        # Aggressive sweep
        self.edge_threshold = 5
        self.snipe_size     = 15

        # ── Internal state ────────────────────────────────────────────
        self.pending_cancels: set  = set()
        self._last_bid             = None
        self._last_ask             = None
        self._last_quoted_fv       = None
        self._last_snipe_t         = 0.0

    # ── Fair value model ─────────────────────────────────────────────
    def _calc_fv_model(
        self,
        eps:    Optional[float] = None,
        q_hike: Optional[float] = None,
        q_cut:  Optional[float] = None,
    ) -> Optional[int]:
        """
        Returns C fair value in CENTS, or None if BETA/GAMMA not yet committed.

        Formula (from case packet, ×100 to convert dollars → cents):
          ERC    = 25·q_hike − 25·q_cut          (expected rate change, bps)
          Δy     = BETA · ERC
          PE_t   = PE0 · exp(−GAMMA · Δy)
          ΔB/N   = B0/N · (−D·Δy + ½C·Δy²)
          FV_C   = (EPS · PE_t + λ · ΔB/N) × 100
        """
        if self.beta is None or self.gamma is None:
            return None
        eps    = eps    or self.client.eps.get("C", 2.00)
        q_hike = q_hike if q_hike is not None else self.client.fair_values.get("R_HIKE", 0.33)
        q_cut  = q_cut  if q_cut  is not None else self.client.fair_values.get("R_CUT",  0.34)

        erc    = 25.0 * q_hike - 25.0 * q_cut
        delta_y = self.beta * erc
        pe      = PE0_C * math.exp(-self.gamma * delta_y)
        delta_b = B0_OVER_N * (-DUR * delta_y + 0.5 * CONV_C * delta_y ** 2)
        return round((eps * pe + LAMBDA_C * delta_b) * 100)

    def _predict_cents(self, beta: float, gamma: float, erc: float, eps: float) -> float:
        """Used only inside the fitting routine — same formula."""
        delta_y = beta * erc
        pe      = PE0_C * math.exp(-gamma * delta_y)
        delta_b = B0_OVER_N * (-DUR * delta_y + 0.5 * CONV_C * delta_y ** 2)
        return (eps * pe + LAMBDA_C * delta_b) * 100.0

    # ── Parameter estimation (runs in executor thread) ───────────────
    def _fit_sync(self) -> tuple[float, float]:
        """Gradient-descent MSE fit of (BETA, GAMMA) to observed prices."""
        data  = self._observations[-200:]   # use most recent 200 obs
        beta  = self.beta  or self.BETA_INIT
        gamma = self.gamma or self.GAMMA_INIT

        for _ in range(self.N_ITER):
            gb = gg = 0.0
            for erc, c_mid, eps in data:
                pred  = self._predict_cents(beta, gamma, erc, eps)
                err   = pred - c_mid
                dy    = beta * erc
                exp_t = math.exp(-gamma * dy)

                # ∂pred/∂(delta_y) · ∂(delta_y)/∂beta = ∂pred/∂beta
                d_pred_d_dy = (
                    eps * PE0_C * exp_t * (-gamma)
                    + LAMBDA_C * B0_OVER_N * (-DUR + CONV_C * dy)
                ) * 100.0
                d_pred_d_gamma = eps * PE0_C * exp_t * (-dy) * 100.0

                gb += err * d_pred_d_dy   * erc
                gg += err * d_pred_d_gamma

            n     = len(data)
            beta  -= self.LR_BETA  * (2.0 / n) * gb
            gamma -= self.LR_GAMMA * (2.0 / n) * gg
            beta   = max(1e-6, min(0.005, beta))
            gamma  = max(0.1,  min(100.0, gamma))

        return beta, gamma

    async def _fit_async(self) -> None:
        """Run fit in a thread so gRPC stream is never blocked."""
        if self._fitting:
            return
        self._fitting = True
        try:
            loop            = asyncio.get_event_loop()
            beta, gamma     = await loop.run_in_executor(None, self._fit_sync)
            print(f"[C] fit  BETA={beta:.7f}  GAMMA={gamma:.2f}")
            self._recent_fits.append((beta, gamma))
            if len(self._recent_fits) > self.STABILITY_WIN:
                self._recent_fits.pop(0)
            self._try_commit()
        finally:
            self._fitting = False

    def _try_commit(self) -> None:
        if len(self._recent_fits) < self.STABILITY_WIN:
            return
        betas  = [e[0] for e in self._recent_fits]
        gammas = [e[1] for e in self._recent_fits]
        if max(betas) - min(betas) < 5e-5 and max(gammas) - min(gammas) < 2.0:
            self.beta   = sum(betas)  / len(betas)
            self.gamma  = sum(gammas) / len(gammas)
            self._committed = True
            print(f"[C] *** COMMITTED  BETA={self.beta:.7f}  GAMMA={self.gamma:.2f} ***")
            self.recalc_fv()

    # ── FV management ────────────────────────────────────────────────
    def recalc_fv(self) -> bool:
        """Recalculate and store C FV. Returns True if changed by ≥ threshold."""
        new_fv = self._calc_fv_model()
        if new_fv is None:
            return False
        old_fv = self.client.fair_values.get("C")
        self.client.fair_values["C"] = new_fv
        changed = old_fv is None or abs(new_fv - old_fv) >= self.FV_MOVE_THRESH
        if changed:
            print(f"[C] FV updated  {old_fv} → {new_fv}")
        return changed

    # ── Sweep ────────────────────────────────────────────────────────
    async def _sweep(self) -> None:
        fv = self.client.fair_values.get("C")
        if fv is None or not self._committed:
            return
        book = self.client.order_books["C"]

        for px, qty in sorted(
            [(px, q) for px, q in book.asks.items() if q > 0 and px < fv - self.edge_threshold],
            key=lambda x: x[0],
        ):
            oid = await place_safe(self.client, "C", min(qty, self.snipe_size), Side.BUY, px)
            if oid is None:
                break
            print(f"[C] SWEEP BUY  {min(qty,self.snipe_size):>3}@{px}  edge={fv-px:.0f}")

        for px, qty in sorted(
            [(px, q) for px, q in book.bids.items() if q > 0 and px > fv + self.edge_threshold],
            key=lambda x: -x[0],
        ):
            oid = await place_safe(self.client, "C", min(qty, self.snipe_size), Side.SELL, px)
            if oid is None:
                break
            print(f"[C] SWEEP SELL {min(qty,self.snipe_size):>3}@{px}  edge={px-fv:.0f}")

    # ── Public interface ─────────────────────────────────────────────
    async def on_earnings(self, eps: float) -> None:
        self.client.eps["C"] = eps
        changed = self.recalc_fv()
        print(f"[C] EARNINGS  EPS={eps}  FV={self.client.fair_values.get('C')}  committed={self._committed}")
        if changed:
            self._last_bid = self._last_ask = None
            await cancel_symbol(self.client, "C", self.pending_cancels)
            await self._sweep()
        asyncio.create_task(self.client.strat_etf.check_arb())

    async def on_fed_update(self) -> None:
        """
        Called every time an R contract book updates.
        Recalculates C FV; if it has moved materially, sweeps and re-quotes.
        Rate-limited to avoid flooding on every tick.
        """
        if not self._committed:
            return
        changed = self.recalc_fv()
        if not changed:
            return
        now = asyncio.get_event_loop().time()
        if now - self._last_snipe_t > 0.25:
            self._last_snipe_t = now
            await self._sweep()

    async def update_quotes(self) -> None:
        """Called from main trade loop (~100 ms). Re-quotes only on meaningful FV moves."""
        fv = self.client.fair_values.get("C")
        if fv is None:
            # Pre-commitment fallback: quote around market mid with wide spread
            bb, ba = self.client.get_best_bid_ask("C")
            if bb is not None and ba is not None:
                fv = (bb + ba) / 2
            else:
                return

        # Avoid unnecessary cancel+replace churn when FV is stable
        if (self._last_quoted_fv is not None
                and abs(fv - self._last_quoted_fv) < self.FV_MOVE_THRESH / 2):
            return

        pos    = self.client.positions.get("C", 0)
        bb, ba = self.client.get_best_bid_ask("C")

        pos_frac = abs(pos) / max(self.max_soft_pos, 1)
        # Double spread before commitment (extra model uncertainty)
        mult   = 1.0 if self._committed else 2.0
        spread = int((self.base_spread + pos_frac * 6) * mult)
        skew   = self.skew_per_unit * pos

        bid = round(fv - spread / 2 - skew)
        ask = round(fv + spread / 2 - skew)
        if bid >= ask:
            ask = bid + 1

        pos_lim = LIMITS["C"]["pos"]
        if pos >= pos_lim and bb is not None:
            ask = bb
        if pos <= -pos_lim and ba is not None:
            bid = ba
        if abs(pos) < pos_lim:
            if ba is not None and bid >= ba:
                bid = 0
            if bb is not None and ask <= bb:
                ask = 0

        bsz = self.quote_size if pos <  self.max_soft_pos else max(2, self.quote_size // 3)
        asz = self.quote_size if pos > -self.max_soft_pos else max(2, self.quote_size // 3)

        if (bid, bsz) == self._last_bid and (ask, asz) == self._last_ask:
            return
        self._last_bid = (bid, bsz)
        self._last_ask = (ask, asz)
        self._last_quoted_fv = fv

        await cancel_symbol(self.client, "C", self.pending_cancels)
        if bid > 0:
            oid = await place_safe(self.client, "C", bsz, Side.BUY, bid)
            if oid:
                self.client.my_quote_ids.add(oid)
        if ask > 0:
            oid = await place_safe(self.client, "C", asz, Side.SELL, ask)
            if oid:
                self.client.my_quote_ids.add(oid)

        print(f"[C] MM  fv={fv:.0f}  committed={self._committed}  pos={pos}  "
              f"spread={spread}  bid={bid}×{bsz}  ask={ask}×{asz}")

    async def record_observation(self) -> None:
        """Accumulate (ERC, C_mid, EPS) tuples and trigger async fit."""
        eps   = self.client.eps.get("C", 2.00)
        bb, ba = self.client.get_best_bid_ask("C")
        if bb is None or ba is None:
            return
        c_mid = (bb + ba) / 2
        erc   = (25.0 * self.client.fair_values.get("R_HIKE", 0.33)
                - 25.0 * self.client.fair_values.get("R_CUT",  0.34))
        self._observations.append((erc, c_mid, eps))
        self._obs_since_fit += 1

        if (len(self._observations) >= self.MIN_OBS
                and self._obs_since_fit  >= self.FIT_EVERY
                and not self._fitting
                and not self._committed):
            self._obs_since_fit = 0
            asyncio.create_task(self._fit_async())

    def on_cancel_confirmed(self, oid: str) -> None:
        self.pending_cancels.discard(oid)

    def on_order_rejected(self, oid: str) -> None:
        self.pending_cancels.discard(oid)


# ══════════════════════════════════════════════════════════════════════
#  OPTIONS  — PCP + box-spread arb on B options
# ══════════════════════════════════════════════════════════════════════

class OptionsStrategy:
    """
    Two model-free arbitrage strategies (both valid at r=0):

    Put-call parity:  C − P = S − K
      • If C − P > S − K:  buy call, sell put, sell stock
      • If C − P < S − K:  sell call, buy put, buy stock

    Box spread:  always worth K2 − K1 at expiry (= 50 in this case)
      • Buy box  if cost   < K2−K1:  buy C(K1), sell C(K2), sell P(K1), buy P(K2)
      • Sell box if receipt > K2−K1:  sell C(K1), buy C(K2), buy P(K1), sell P(K2)
    """

    def __init__(self, client: "MyXchangeClient"):
        self.client     = client
        self.arb_qty    = 5      # increased from 1 — free money, size up
        self.min_edge   = 3      # cents minimum net profit after all legs
        self._cooldowns: dict[str, float] = {}
        self._cooldown  = 3.0    # seconds before re-entering same arb
        self._last_run  = 0.0

    def _bba(self, sym: str):
        return self.client.get_best_bid_ask(sym)

    def _cooled(self, key: str) -> bool:
        now = asyncio.get_event_loop().time()
        if now - self._cooldowns.get(key, 0) < self._cooldown:
            return False
        self._cooldowns[key] = now
        return True

    async def check_pcp(self, strike: int) -> None:
        """Check put-call parity arb for a given strike."""
        c_bid, c_ask = self._bba(f"B_C_{strike}")
        p_bid, p_ask = self._bba(f"B_P_{strike}")
        s_bid, s_ask = self._bba("B")
        if None in (c_bid, c_ask, p_bid, p_ask, s_bid, s_ask):
            return

        # Buy call side: p_bid + s_bid − c_ask − K > 0
        buy_call_edge = p_bid + s_bid - c_ask - strike
        if buy_call_edge > self.min_edge and self._cooled(f"pcp_bc_{strike}"):
            print(f"[OPT] PCP BUY_CALL K={strike} edge={buy_call_edge}")
            await self.client.place_order(f"B_C_{strike}", self.arb_qty, Side.BUY,  c_ask)
            await self.client.place_order(f"B_P_{strike}", self.arb_qty, Side.SELL, p_bid)
            await self.client.place_order("B",              self.arb_qty, Side.SELL, s_bid)
            return

        # Sell call side: c_bid − p_ask − s_ask + K > 0
        sell_call_edge = c_bid - p_ask - s_ask + strike
        if sell_call_edge > self.min_edge and self._cooled(f"pcp_sc_{strike}"):
            print(f"[OPT] PCP SELL_CALL K={strike} edge={sell_call_edge}")
            await self.client.place_order(f"B_C_{strike}", self.arb_qty, Side.SELL, c_bid)
            await self.client.place_order(f"B_P_{strike}", self.arb_qty, Side.BUY,  p_ask)
            await self.client.place_order("B",              self.arb_qty, Side.BUY,  s_ask)

    async def check_box(self, k1: int, k2: int) -> None:
        """Check box-spread arb. Theoretical value = k2 − k1 (= 50) at r=0."""
        theoretical = k2 - k1
        c1b, c1a = self._bba(f"B_C_{k1}")
        c2b, c2a = self._bba(f"B_C_{k2}")
        p1b, p1a = self._bba(f"B_P_{k1}")
        p2b, p2a = self._bba(f"B_P_{k2}")
        if None in (c1b, c1a, c2b, c2a, p1b, p1a, p2b, p2a):
            return

        buy_edge = theoretical - (c1a - c2b + p2a - p1b)
        if buy_edge > self.min_edge and self._cooled(f"box_buy_{k1}_{k2}"):
            print(f"[OPT] BOX BUY ({k1},{k2}) edge={buy_edge}")
            await self.client.place_order(f"B_C_{k1}", self.arb_qty, Side.BUY,  c1a)
            await self.client.place_order(f"B_C_{k2}", self.arb_qty, Side.SELL, c2b)
            await self.client.place_order(f"B_P_{k1}", self.arb_qty, Side.SELL, p1b)
            await self.client.place_order(f"B_P_{k2}", self.arb_qty, Side.BUY,  p2a)
            return

        sell_edge = (c1b - c2a + p2b - p1a) - theoretical
        if sell_edge > self.min_edge and self._cooled(f"box_sell_{k1}_{k2}"):
            print(f"[OPT] BOX SELL ({k1},{k2}) edge={sell_edge}")
            await self.client.place_order(f"B_C_{k1}", self.arb_qty, Side.SELL, c1b)
            await self.client.place_order(f"B_C_{k2}", self.arb_qty, Side.BUY,  c2a)
            await self.client.place_order(f"B_P_{k1}", self.arb_qty, Side.BUY,  p1a)
            await self.client.place_order(f"B_P_{k2}", self.arb_qty, Side.SELL, p2b)

    async def run(self) -> None:
        """Sweep all arbs. Rate-limited to 1.5 s between full sweeps."""
        now = asyncio.get_event_loop().time()
        if now - self._last_run < 1.5:
            return
        self._last_run = now
        for strike in OPTION_STRIKES:
            await self.check_pcp(strike)
        for k1, k2 in BOX_PAIRS:
            await self.check_box(k1, k2)


# ══════════════════════════════════════════════════════════════════════
#  FED  — prediction-market trading + end-of-round max positioning
# ══════════════════════════════════════════════════════════════════════

class FedStrategy:
    """
    Probability model:
      - seed from market book mids (first observation)
      - update via CPI surprises (signed shift proportional to surprise size)
      - update via FEDSPEAK keyword classification
      - trade R contracts when model diverges from market by > threshold

    End of round:
      - if one outcome has >65% probability, max out position in that contract
        and unwind the other two
    """

    DOVISH_KW = [
        "easing", "cooling", "dovish", "cut", "lower rates",
        "easing inflation", "slowing growth", "policy easing",
        "expectations of policy easing",
    ]
    HAWKISH_KW = [
        "restrictive", "hawkish", "hike", "inflation risks",
        "stay restrictive", "tightening", "higher for longer",
        "restrictive for longer",
    ]
    NEUTRAL_KW = [
        "wide range of views", "stay firm", "weighing", "uncertainties",
        "not in a hurry", "either direction", "complicates",
    ]

    def __init__(self, client: "MyXchangeClient"):
        self.client = client

        # Our probability estimate (updated from news)
        self._probs = {"R_HIKE": 0.33, "R_HOLD": 0.33, "R_CUT": 0.34}
        # Market-implied probs (from book mids)
        self._mkt   = {"R_HIKE": None, "R_HOLD": None, "R_CUT": None}

        self.edge_threshold  = 0.04   # minimum divergence before trading
        self.base_qty        = 5      # base order size
        self.max_qty         = 30     # per-contract position ceiling
        self.cpi_sensitivity = 80     # multiplier on CPI surprise
        self.surprise_thresh = 0.0001 # treat as "neutral" if |surprise| < this
        self.fedspeak_shift  = 0.04   # prob shift per classified headline
        self.toward_hold     = 0.04   # push toward hold on neutral news

        self._book_reads     = 0
        self._last_trade_t   = 0.0
        self._trade_cooldown = 1.0
        self._eor_done       = False  # has end-of-round positioning fired?

    # ── Book update ──────────────────────────────────────────────────
    def update_from_book(self) -> None:
        """Read R contract mids to update market-implied probs and seed our model."""
        updated = 0
        for c in FED_CONTRACTS:
            bb, ba = self.client.get_best_bid_ask(c)
            if bb is not None and ba is not None:
                mid          = (bb + ba) / 2
                self._mkt[c] = mid / 1000.0
                self.client.fair_values[c] = mid / 1000.0
                updated += 1
        if updated == 3:
            self._book_reads += 1
            # On first observation, seed our model from market
            if self._book_reads == 1:
                for c in FED_CONTRACTS:
                    if self._mkt[c] is not None:
                        self._probs[c] = self._mkt[c]
                self._normalize()

    # ── Probability updates ──────────────────────────────────────────
    def _normalize(self) -> None:
        """Clamp to [0.02, 1] and renormalize to sum=1; push into fair_values."""
        total = sum(max(0.02, self._probs[c]) for c in FED_CONTRACTS)
        for c in FED_CONTRACTS:
            self._probs[c] = max(0.02, self._probs[c]) / total
        for c in FED_CONTRACTS:
            self.client.fair_values[c] = self._probs[c]

    def on_cpi_news(self, forecast: float, actual: float) -> None:
        surprise = actual - forecast
        print(f"[FED] CPI surprise={surprise:+.6f}  "
              f"({'hawkish' if surprise > 0 else ('neutral' if abs(surprise) < self.surprise_thresh else 'dovish')})")
        if abs(surprise) < self.surprise_thresh:
            self._probs["R_HOLD"] += self.toward_hold * 2
            self._probs["R_HIKE"] -= self.toward_hold
            self._probs["R_CUT"]  -= self.toward_hold
        elif surprise > 0:   # above-forecast inflation → hawkish
            shift = min(self.cpi_sensitivity * surprise, self._probs["R_CUT"])
            self._probs["R_CUT"]  -= shift
            self._probs["R_HIKE"] += shift
        else:                # below-forecast inflation → dovish
            shift = min(self.cpi_sensitivity * abs(surprise), self._probs["R_HIKE"])
            self._probs["R_HIKE"] -= shift
            self._probs["R_CUT"]  += shift
        self._normalize()
        print(f"[FED] post-CPI  HIKE={self._probs['R_HIKE']:.3f}  "
              f"HOLD={self._probs['R_HOLD']:.3f}  CUT={self._probs['R_CUT']:.3f}")

    def on_fedspeak(self, content: str) -> None:
        text    = content.lower()
        dovish  = any(kw in text for kw in self.DOVISH_KW)
        hawkish = any(kw in text for kw in self.HAWKISH_KW)
        neutral = any(kw in text for kw in self.NEUTRAL_KW)

        if dovish and not hawkish:
            shift = min(self.fedspeak_shift, self._probs["R_HIKE"])
            self._probs["R_HIKE"] -= shift
            self._probs["R_CUT"]  += shift
            print(f"[FED] DOVISH fedspeak → shift={shift:.3f}")
        elif hawkish and not dovish:
            shift = min(self.fedspeak_shift, self._probs["R_CUT"])
            self._probs["R_CUT"]  -= shift
            self._probs["R_HIKE"] += shift
            print(f"[FED] HAWKISH fedspeak → shift={shift:.3f}")
        elif neutral:
            self._probs["R_HOLD"] += self.toward_hold * 2
            self._probs["R_HIKE"] -= self.toward_hold
            self._probs["R_CUT"]  -= self.toward_hold
            print("[FED] NEUTRAL fedspeak")
        else:
            print(f"[FED] UNCLASSIFIED: {content[:80]}")
        self._normalize()

    # ── Trading ──────────────────────────────────────────────────────
    async def trade_contracts(self) -> None:
        """Buy underpriced contracts, sell overpriced ones."""
        if self._book_reads < 3:
            return
        now = asyncio.get_event_loop().time()
        if now - self._last_trade_t < self._trade_cooldown:
            return
        self._last_trade_t = now

        for c in FED_CONTRACTS:
            mkt = self._mkt[c]
            our = self._probs[c]
            if mkt is None:
                continue
            div = our - mkt
            bb, ba = self.client.get_best_bid_ask(c)
            if bb is None or ba is None:
                continue

            # Scale size by conviction (up to 3× base_qty)
            conviction = min(abs(div) / max(self.edge_threshold, 1e-6), 3.0)
            qty = min(int(self.base_qty * conviction), self.max_qty)
            if qty <= 0:
                continue

            if div > self.edge_threshold:
                oid = await place_safe(self.client, c, qty, Side.BUY, ba)
                if oid:
                    print(f"[FED] BUY  {c} qty={qty}  our={our:.3f}  mkt={mkt:.3f}  div={div:+.3f}")
            elif div < -self.edge_threshold:
                oid = await place_safe(self.client, c, qty, Side.SELL, bb)
                if oid:
                    print(f"[FED] SELL {c} qty={qty}  our={our:.3f}  mkt={mkt:.3f}  div={div:+.3f}")

    async def end_of_round_positioning(self) -> None:
        """
        Called ~20 s before round end.
        If one outcome has ≥65% probability: max-long that contract, unwind others.
        This turns into a pure expected-value trade (paying ≤650 to receive 1000).
        """
        if self._eor_done or self._book_reads < 3:
            return
        best = max(FED_CONTRACTS, key=lambda c: self._probs[c])
        if self._probs[best] < 0.65:
            return
        self._eor_done = True
        print(f"[FED] EOR positioning: max {best}  p={self._probs[best]:.3f}")

        # Max long the winner
        bb, ba = self.client.get_best_bid_ask(best)
        if ba is not None:
            pos = self.client.positions.get(best, 0)
            qty = LIMITS[best]["pos"] - pos
            if qty > 0:
                await place_safe(self.client, best, qty, Side.BUY, ba)

        # Unwind losers
        for c in FED_CONTRACTS:
            if c == best:
                continue
            pos = self.client.positions.get(c, 0)
            if pos > 0:
                bb_c, _ = self.client.get_best_bid_ask(c)
                if bb_c is not None:
                    await place_safe(self.client, c, pos, Side.SELL, bb_c)


# ══════════════════════════════════════════════════════════════════════
#  ETF  — NAV arb triggered by A and C FV updates
# ══════════════════════════════════════════════════════════════════════

class ETFStrategy:
    """
    1 ETF = 1A + 1B + 1C.  Swap cost = ETF_COST cents per unit.

    Create arb (ETF overpriced):  buy A+B+C at ask → toETF swap → sell ETF at bid
    Redeem arb (ETF underpriced): buy ETF at ask → fromETF swap → sell A+B+C at bid

    Per the case hint: when ETF/equity diverge, the ETF is more likely mispriced.
    We therefore prefer one-sided ETF shorts (create) over buying the equity legs
    as a directional bet — but we still execute the full arb for safety.
    """

    def __init__(self, client: "MyXchangeClient"):
        self.client          = client
        self.max_qty         = 5
        self.pending_create  = False
        self.pending_redeem  = False
        self.filled_comps    = {"A": 0, "B": 0, "C": 0}
        self.filled_etf      = 0
        self._target_qty     = 0
        self._last_check_t   = 0.0

    def _nav(self) -> Optional[float]:
        fv_a = self.client.fair_values.get("A")
        fv_c = self.client.fair_values.get("C")
        _, _, mid_b = self.client.get_bba_mid("B")
        if mid_b is None:
            return None
        a = fv_a if fv_a is not None else self.client.get_bba_mid("A")[2]
        c = fv_c if fv_c is not None else self.client.get_bba_mid("C")[2]
        if a is None or c is None:
            return None
        return a + mid_b + c

    async def check_arb(self) -> None:
        """Check ETF arb. Called from main loop AND on A/C FV updates."""
        if not self.client._ready:
            return
        now = asyncio.get_event_loop().time()
        if now - self._last_check_t < 0.3:
            return
        self._last_check_t = now

        if self.pending_create or self.pending_redeem:
            return

        nav = self._nav()
        if nav is None:
            return
        etf_bid, etf_ask, etf_mid = self.client.get_bba_mid("ETF")
        if etf_mid is None:
            return

        diff = etf_mid - nav
        print(f"[ETF] etf={etf_mid:.0f}  nav={nav:.0f}  diff={diff:+.0f}")

        if diff > ETF_COST:
            # ETF overpriced: buy components → toETF swap → sell ETF
            pos_a   = self.client.positions.get("A", 0)
            pos_b   = self.client.positions.get("B", 0)
            pos_c   = self.client.positions.get("C", 0)
            pos_etf = self.client.positions.get("ETF", 0)
            qty = max(0, min(
                self.max_qty,
                LIMITS["A"]["pos"]   - pos_a,
                LIMITS["B"]["pos"]   - pos_b,
                LIMITS["C"]["pos"]   - pos_c,
                LIMITS["ETF"]["pos"] - pos_etf,
            ))
            if qty <= 0:
                return
            _, ask_a, _ = self.client.get_bba_mid("A")
            _, ask_b, _ = self.client.get_bba_mid("B")
            _, ask_c, _ = self.client.get_bba_mid("C")
            if None in (ask_a, ask_b, ask_c):
                return
            print(f"[ETF] CREATE {qty}  diff={diff:.0f}")
            self.pending_create = True
            self.filled_comps   = {"A": 0, "B": 0, "C": 0}
            self._target_qty    = qty
            await asyncio.gather(
                place_safe(self.client, "A", qty, Side.BUY, ask_a),
                place_safe(self.client, "B", qty, Side.BUY, ask_b),
                place_safe(self.client, "C", qty, Side.BUY, ask_c),
            )

        elif diff < -ETF_COST:
            # ETF underpriced: buy ETF → fromETF swap → sell components
            pos_a   = self.client.positions.get("A", 0)
            pos_b   = self.client.positions.get("B", 0)
            pos_c   = self.client.positions.get("C", 0)
            pos_etf = self.client.positions.get("ETF", 0)
            qty = max(0, min(
                self.max_qty,
                LIMITS["A"]["pos"] + pos_a,
                LIMITS["B"]["pos"] + pos_b,
                LIMITS["C"]["pos"] + pos_c,
                pos_etf,
            ))
            if qty <= 0:
                return
            _, ask_etf, _ = self.client.get_bba_mid("ETF")
            if ask_etf is None:
                return
            print(f"[ETF] REDEEM {qty}  diff={diff:.0f}")
            self.pending_redeem = True
            self.filled_etf     = 0
            self._target_qty    = qty
            await place_safe(self.client, "ETF", qty, Side.BUY, ask_etf)


# ══════════════════════════════════════════════════════════════════════
#  MAIN CLIENT
# ══════════════════════════════════════════════════════════════════════

class MyXchangeClient(XChangeClient):

    def __init__(self, host: str, username: str, password: str):
        super().__init__(host, username, password)
        from utcxchangelib.xchange_client import OrderBook
        for s in ALL_SYMBOLS:
            self.order_books[s] = OrderBook()
        from utcxchangelib.xchange_client import OrderBook
        for s in ALL_SYMBOLS:
            self.order_books[s] = OrderBook()

        # Shared state
        self.fair_values: dict = {
            "A": None, "C": None, "ETF": None,
            "R_HIKE": 0.33, "R_HOLD": 0.33, "R_CUT": 0.34,
        }
        self.eps:         dict = {"A": None, "C": None}
        self.my_quote_ids: set = set()

        # Strategy modules
        self.strat_a    = StockAStrategy(self)
        self.strat_c    = StockCStrategy(self)
        self.strat_opts = OptionsStrategy(self)
        self.strat_fed  = FedStrategy(self)
        self.strat_etf  = ETFStrategy(self)

        # Round timing (for end-of-round logic)
        self._round_start_t: Optional[float] = None
        self._round_duration = 900   # 15 min; tune live if needed
        self._eor_threshold  = 20
        self._ready = False    # seconds before end to trigger EOR positioning

    # ── Helpers ──────────────────────────────────────────────────────

    def get_best_bid_ask(self, symbol: str):
        book = self.order_books[symbol]
        bids = [k for k, v in book.bids.items() if v > 0]
        asks = [k for k, v in book.asks.items() if v > 0]
        return (max(bids) if bids else None), (min(asks) if asks else None)

    def get_bba_mid(self, symbol: str):
        bb, ba = self.get_best_bid_ask(symbol)
        if bb is None or ba is None:
            return None, None, None
        return bb, ba, (bb + ba) / 2

    # ── Event handlers ───────────────────────────────────────────────

    async def bot_handle_cancel_response(
        self, order_id: str, success: bool, error: Optional[str] = None
    ) -> None:
        self.strat_a.on_cancel_confirmed(order_id)
        self.strat_c.on_cancel_confirmed(order_id)
        if success:
            self.my_quote_ids.discard(order_id)
        else:
            print(f"[CANCEL] FAILED {order_id}: {error}")

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int) -> None:
        self.strat_a.pending_cancels.discard(order_id)
        self.strat_c.pending_cancels.discard(order_id)
        print(f"[FILL] {order_id}  qty={qty}  price={price}")

        if order_id not in self.open_orders:
            return
        sym = self.open_orders[order_id][0].symbol

        # ── ETF create: track component fills ────────────────────────
        if self.strat_etf.pending_create and sym in ("A", "B", "C"):
            self.strat_etf.filled_comps[sym] += qty
            if all(
                self.strat_etf.filled_comps[s] >= self.strat_etf._target_qty
                for s in ("A", "B", "C")
            ):
                await self.place_swap_order("toETF", self.strat_etf._target_qty)
                etf_bid, _, _ = self.get_bba_mid("ETF")
                if etf_bid is not None:
                    await place_safe(self, "ETF", self.strat_etf._target_qty, Side.SELL, etf_bid)
                self.strat_etf.pending_create = False

        # ── ETF redeem: track ETF fill ────────────────────────────────
        elif self.strat_etf.pending_redeem and sym == "ETF":
            self.strat_etf.filled_etf += qty
            if self.strat_etf.filled_etf >= self.strat_etf._target_qty:
                await self.place_swap_order("fromETF", self.strat_etf._target_qty)
                bid_a, _, _ = self.get_bba_mid("A")
                bid_b, _, _ = self.get_bba_mid("B")
                bid_c, _, _ = self.get_bba_mid("C")
                if all(x is not None for x in (bid_a, bid_b, bid_c)):
                    await asyncio.gather(
                        place_safe(self, "A", self.strat_etf._target_qty, Side.SELL, bid_a),
                        place_safe(self, "B", self.strat_etf._target_qty, Side.SELL, bid_b),
                        place_safe(self, "C", self.strat_etf._target_qty, Side.SELL, bid_c),
                    )
                self.strat_etf.pending_redeem = False
                self.strat_etf.filled_etf     = 0



    async def handle_order_fill(self, msg) -> None:
        if msg.id not in self.open_orders:
            return
        await super().handle_order_fill(msg)

    async def handle_order_rejected(self, msg) -> None:
        self.open_orders.pop(msg.id, None)
        await self.bot_handle_order_rejected(msg.id, msg.error)

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
        self.strat_a.on_order_rejected(order_id)
        self.strat_c.on_order_rejected(order_id)
        self.my_quote_ids.discard(order_id)
        print(f"[REJECT] {order_id}: {reason}")

    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int) -> None:
        """Public trade feed — used for OFI on A."""
        if symbol == "A":
            self.strat_a.record_trade(price, qty)

    async def bot_handle_book_update(self, symbol: str) -> None:
        if symbol == "A":
            await self.strat_a.on_book_update()

        elif symbol in FED_CONTRACTS:
            # R book update: refresh Fed probs → may shift C FV
            self.strat_fed.update_from_book()
            await self.strat_c.on_fed_update()             # sweep C if FV moved
            asyncio.create_task(self.strat_etf.check_arb()) # ETF NAV may have changed

        elif symbol in ("B",) or symbol.startswith("B_"):
            # Options book: check arbs immediately (don't wait for run() poll)
            asyncio.create_task(self.strat_opts.run())

    async def bot_handle_swap_response(self, swap: str, qty: int, success: bool) -> None:
        status = "OK" if success else "FAIL"
        print(f"[SWAP] {swap} ×{qty}  {status}")

    async def bot_handle_news(self, news_release: dict) -> None:
        kind = news_release["kind"]
        data = news_release["new_data"]

        if kind == "structured":
            subtype = data["structured_subtype"]

            if subtype == "earnings":
                asset = data["asset"]
                value = data["value"]
                if asset == "A":
                    await self.strat_a.on_earnings(value)
                elif asset == "C":
                    await self.strat_c.on_earnings(value)
                    asyncio.create_task(self.strat_etf.check_arb())

            elif subtype == "cpi_print":
                self.strat_fed.on_cpi_news(data["forecast"], data["actual"])
                # CPI shifts Fed probs → C FV changes → sweep if needed
                changed = self.strat_c.recalc_fv()
                if changed:
                    await self.strat_c._sweep()
                asyncio.create_task(self.strat_etf.check_arb())

        else:
            # Unstructured news — FEDSPEAK
            content = data.get("content", "")
            self.strat_fed.on_fedspeak(content)
            changed = self.strat_c.recalc_fv()
            if changed:
                await self.strat_c._sweep()

    async def bot_handle_market_resolved(
        self, market_id: str, winning_symbol: str, tick: int
    ) -> None:
        print(f"[RESOLVED] {market_id} → {winning_symbol}")

    async def bot_handle_settlement_payout(
        self, user: str, market_id: str, amount: int, tick: int
    ) -> None:
        print(f"[PAYOUT] {amount} from {market_id}")

    # ── Main trade loop ──────────────────────────────────────────────

    async def _maybe_end_of_round(self) -> None:
        """Fire end-of-round positioning in the final ~20 seconds of a round."""
        if self._round_start_t is None:
            return
        elapsed   = asyncio.get_event_loop().time() - self._round_start_t
        remaining = self._round_duration - elapsed
        if 0 < remaining < self._eor_threshold:
            await self.strat_fed.end_of_round_positioning()

    async def trade(self) -> None:
        # Brief startup delay so the exchange has time to send initial state
        await asyncio.sleep(8)
        self._ready = True
        self._round_start_t = asyncio.get_event_loop().time()

        while True:
            # ── Per-iteration tasks ─────────────────────────────────
            await self.strat_a.update_quotes()
            await self.strat_c.update_quotes()
            await self.strat_c.record_observation()   # accumulate data for BETA/GAMMA fit
            await self.strat_etf.check_arb()
            await self.strat_opts.run()
            await self.strat_fed.trade_contracts()
            await self._maybe_end_of_round()
            # ───────────────────────────────────────────────────────
            await asyncio.sleep(0.1)   # 10 Hz; stays well under 200 ms tick

    async def start(self) -> None:
        def _on_exception(task):
            if not task.cancelled() and task.exception():
                import traceback
                traceback.print_exception(type(task.exception()), task.exception(),
                                          task.exception().__traceback__)

        self._trade_task = asyncio.create_task(self.trade())
        self._trade_task.add_done_callback(_on_exception)
        await self.connect()


# ══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

async def main():
    SERVER = "practice.uchicago.exchange:3333"
    client = MyXchangeClient(SERVER, "chicago6", "bolt-nova-rocket")
    await client.start()


if __name__ == "__main__":
    asyncio.run(main())
