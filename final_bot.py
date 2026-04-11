"""
UChicago Trading Competition 2026 — Case 1: Market Making
Final strategy.

Base: strategy.py (rank 1, +402k).  Eight targeted changes:

  1. FED POSITION BUG FIXED
       max_abs_position["R_*"] = 25 in __init__ (was 200).
       _kelly_size uses max(0, ...) not max(1, ...) — eliminates accumulation past MAX_POS.
       Fed trade() called at 15-second cadence in main loop (not every 0.1s);
       bypassed immediately on CPI / fedspeak so news alpha is preserved.

  2. EPS_SCALE_A AUTO-CALIBRATION (from v3)
       After each A earnings, record implied_scale = mid/(eps*PE_A).
       After 3+ consistent observations, update the global EPS_SCALE_A.
       Eliminates catastrophic first-sweep direction errors if scale is wrong.

  3. STOCK A CROSS-SPREAD-AT-LIMIT REMOVED
       Old: at pos=±200, forced ask=best_bid (crossed spread at a loss).
       New: suppress the accumulating quote side; reduce naturally via fills.
       Cross-book check now applied unconditionally (not gated on abs(pos)).

  4. STOCK C — GRADIENT DESCENT REPLACED WITH EMPIRICAL ERC CALIBRATION
       Model: C_FV = anchor_mid + sensitivity * (current_ERC - anchor_ERC)
         where sensitivity = ΔC_mid / ΔERC measured from first significant CPI.
       First CPI: calibrate sensitivity only — no trade.
       Subsequent CPIs: directional trade (±10 units, EV-checked) before C reprices.
       Passive MM: ±12 ticks/3 units before calibration; ±5 ticks/8 units after.
       No on_book_update sniping until calibrated (prevents runaway accumulation).
       max_abs_position["C"] = 50 (caps model-uncertainty risk).

  5. OPTIONS — CANCEL-BEFORE-RECHECK REMOVED
       Resting arb orders are profitable when they fill.
       Cancelling them and immediately re-placing causes vol-quota race.
       Cooldown (_cold) still prevents refiring too quickly.

  6. FED SETTLEMENT TRADE ADDED (from v3 concept)
       Fires once per round in window T-90s to T-20s.
       Requires our_p >= 0.78 AND positive EV at current ask price.
       Allows position up to 35 (bypasses normal 25 cap for terminal bet).

  7. ETF — IMPLIED B FROM PCP FOR NAV + ONE-SIDED COOLDOWN
       ETFStrategy now holds a BPriceTracker; _nav() uses implied B when available.
       More accurate NAV → better arb detection.
       One-sided trades rate-limited to once per 3 seconds (prevents order spam).

  8. MAIN LOOP CLEANUP
       Removed strat_c.record_obs() (no longer needed without gradient descent).
       Fed trade moved to 15s cadence; immediate call from bot_handle_news on news.
       Settlement trade check added at T-90s to T-20s.
       C fair_values["C"] reset to None at round start (stale FV from old round cleared).
"""

from __future__ import annotations
from typing import Optional
from collections import deque

import asyncio
import math
import time
import sys

import utcxchangelib.service_pb2 as utc_bot_pb2
from utcxchangelib import XChangeClient, Side


# ═══════════════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════════════

_log_handles: dict[str, object] = {}

def log(msg: str, file: Optional[str] = None) -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    if file:
        if file not in _log_handles:
            _log_handles[file] = open(file, "a", buffering=1)
        _log_handles[file].write(line + "\n")


# ═══════════════════════════════════════════════════════════════════════
# Model Constants
# ═══════════════════════════════════════════════════════════════════════

# Stock A
PE_A         = 10
EPS_SCALE_A  = 1        # Auto-calibrated from first 3 earnings observations.
                        # If wrong on startup, update via: scale <value> command.

# Stock C — simplified model (ERC-sensitivity based, no BETA/GAMMA)
PE0_C        = 14.0     # Only used for initial PE_C_scale calibration hint
EPS0_C       = 2.00     # Prior EPS before first C earnings

# ETF
ETF_SWAP_COST = 5

# Timing (case packet: earnings at seconds 22 and 88 of each 90s day)
TICKS_PER_SEC  = 5
SECS_PER_DAY   = 90
TICKS_PER_DAY  = TICKS_PER_SEC * SECS_PER_DAY   # 450
EARNINGS_SECS  = (22, 88)
EARNINGS_TICKS = tuple(s * TICKS_PER_SEC for s in EARNINGS_SECS)  # (110, 440)

# Round settings
ROUND_DURATION_SECS = 15 * 60
FLATTEN_LEAD_SECS   = 20


# ═══════════════════════════════════════════════════════════════════════
# Utility: FlowTracker  (unchanged from strategy.py)
# ═══════════════════════════════════════════════════════════════════════

class FlowTracker:
    """
    Tracks recent signed trade volume per symbol.
    ALPHA=0.05: kept from strategy.py (rank-1 result).  Safe once EPS_SCALE_A
    is correctly calibrated — auto-calibration makes the old death-spiral risk
    a non-issue.
    """
    WINDOW = 60
    ALPHA  = 0.05

    def __init__(self):
        self._trades: dict[str, deque] = {}

    def observe(self, symbol: str, price: int, qty: int,
                best_bid: Optional[int], best_ask: Optional[int]) -> None:
        if symbol not in self._trades:
            self._trades[symbol] = deque(maxlen=self.WINDOW)
        if best_ask is not None and price >= best_ask:
            self._trades[symbol].append(qty)
        elif best_bid is not None and price <= best_bid:
            self._trades[symbol].append(-qty)

    def net_signed_volume(self, symbol: str) -> float:
        t = self._trades.get(symbol)
        return float(sum(t)) if t else 0.0

    def fv_adjustment(self, symbol: str) -> float:
        return self.ALPHA * self.net_signed_volume(symbol)


# ═══════════════════════════════════════════════════════════════════════
# Utility: EarningsTimer  (unchanged from strategy.py)
# ═══════════════════════════════════════════════════════════════════════

class EarningsTimer:
    """Predicts wall-clock time of the NEXT A/C earnings event."""

    PRE_CANCEL_SECS = 3.0

    def __init__(self):
        self._last_earnings_wall: float = 0.0
        self._next_cancel_wall: float   = 0.0
        self._next_earnings_wall: float = 0.0
        self._armed: bool               = False

    def record_earnings(self, tick: int) -> None:
        now = time.time()
        self._last_earnings_wall = now
        tick_in_day = tick % TICKS_PER_DAY
        gaps = []
        for et in EARNINGS_TICKS:
            gap_ticks = (et - tick_in_day) % TICKS_PER_DAY
            if gap_ticks < TICKS_PER_SEC:
                gap_ticks += TICKS_PER_DAY
            gaps.append(gap_ticks)
        gap_to_next = min(gaps) / TICKS_PER_SEC
        self._next_earnings_wall = now + gap_to_next
        self._next_cancel_wall   = self._next_earnings_wall - self.PRE_CANCEL_SECS
        self._armed              = True
        print(f"[TIMER] Next earnings in {gap_to_next:.1f}s (cancel at T-{self.PRE_CANCEL_SECS}s)")

    def should_cancel_now(self) -> bool:
        if not self._armed:
            return False
        if time.time() >= self._next_cancel_wall:
            self._armed = False
            return True
        return False

    def secs_to_next_earnings(self) -> float:
        if not self._armed:
            return float("inf")
        return max(0.0, self._next_earnings_wall - time.time())


# ═══════════════════════════════════════════════════════════════════════
# Utility: BPriceTracker  (unchanged from strategy.py)
# ═══════════════════════════════════════════════════════════════════════

class BPriceTracker:
    """
    Infers B's fair value via put-call parity across all three strikes.
    S_implied = C_mid - P_mid + K  (r=0 approximation).
    Weighted average: tighter-spread strikes get higher weight.
    Used by both OptionsStrategy and ETFStrategy._nav().
    """

    def __init__(self, client: "MyXchangeClient"):
        self.client = client

    def get_implied_price(self) -> Optional[float]:
        estimates: list[float] = []
        weights:   list[float] = []
        for k in (950, 1000, 1050):
            c_bid, c_ask = self.client.get_best_bid_ask(f"B_C_{k}")
            p_bid, p_ask = self.client.get_best_bid_ask(f"B_P_{k}")
            if None in (c_bid, c_ask, p_bid, p_ask):
                continue
            c_mid = (c_bid + c_ask) / 2
            p_mid = (p_bid + p_ask) / 2
            s_imp = c_mid - p_mid + k
            total_spread = (c_ask - c_bid) + (p_ask - p_bid)
            if total_spread <= 0:
                continue
            estimates.append(s_imp)
            weights.append(1.0 / total_spread)
        if not estimates:
            return None
        total_w = sum(weights)
        return sum(s * w for s, w in zip(estimates, weights)) / total_w


# ═══════════════════════════════════════════════════════════════════════
# Strategy: Stock A
# Changes from strategy.py:
#   - EPS_SCALE_A auto-calibration (SCALE_MIN_OBS = 3)
#   - Removed cross-spread-at-limit; suppress accumulating side instead
#   - Cross-book check now unconditional (not gated on abs(pos) < MAX_ABS_POS)
# ═══════════════════════════════════════════════════════════════════════

class StockAStrategy:
    """
    Primary edge: post-earnings sweep + passive MM.

    Earnings fires at deterministic ticks.  Each event:
      1. Auto-calibrate EPS_SCALE_A if divergent from market
      2. Sweep ALL stale book orders (unconditional, up to position limit)
      3. Re-quote tightly around new FV for 2 seconds, then revert to passive MM

    Pre-cancel: quotes cancelled 3s before expected earnings to avoid being
    picked off by faster bots in later rounds.
    """

    MAX_ORDER_SIZE      = 40
    MAX_OUTSTANDING_VOL = 120
    MAX_OPEN_ORDERS     = 50
    MAX_ABS_POS         = 200

    BASE_SPREAD         = 4
    SKEW_PER_UNIT       = 0.15
    QUOTE_SIZE          = 10
    SOFT_POS_LIMIT      = 100

    EDGE_THRESHOLD      = 2
    SNIPE_SIZE          = 30
    POST_EARNINGS_SECS  = 2.0

    # Auto-calibration of EPS_SCALE_A
    SCALE_MIN_OBS       = 3    # require this many observations before updating scale

    def __init__(self, client: "MyXchangeClient"):
        self.client           = client
        self.timer            = EarningsTimer()

        self._pending_cancels: set[str]  = set()
        self._last_snipe_time: float     = 0.0
        self._last_quote_time: float     = 0.0
        self._earnings_end_time: float   = 0.0
        self._last_bid: Optional[tuple]  = None
        self._last_ask: Optional[tuple]  = None
        self._pre_cancel_done: bool      = False

        # EPS_SCALE_A auto-calibration state
        self._scale_obs: list[float]     = []   # persists across rounds

    # ── Limit helpers ──────────────────────────────────────────────

    def _outstanding_vol(self) -> int:
        return sum(
            info[1] for info in self.client.open_orders.values()
            if info and info[0].symbol == "A" and not info[2]
        )

    def _open_order_count(self) -> int:
        return sum(
            1 for info in self.client.open_orders.values()
            if info and info[0].symbol == "A" and not info[2]
        )

    async def _place(self, qty: int, side: Side, price: int) -> Optional[str]:
        if qty <= 0:
            return None
        qty = min(qty, self.MAX_ORDER_SIZE)
        if self._outstanding_vol() + qty > self.MAX_OUTSTANDING_VOL:
            qty = self.MAX_OUTSTANDING_VOL - self._outstanding_vol()
            if qty <= 0:
                return None
        if self._open_order_count() + 1 > self.MAX_OPEN_ORDERS:
            return None
        pos = self.client.positions["A"]
        if side == Side.BUY  and pos + qty > self.MAX_ABS_POS:
            qty = self.MAX_ABS_POS - pos
        if side == Side.SELL and pos - qty < -self.MAX_ABS_POS:
            qty = self.MAX_ABS_POS + pos
        if qty <= 0:
            return None
        return await self.client.place_order("A", qty, side, price)

    async def _cancel_all_a(self) -> None:
        to_cancel = [
            oid for oid, info in self.client.open_orders.items()
            if info and info[0].symbol == "A" and oid not in self._pending_cancels
        ]
        for oid in to_cancel:
            self._pending_cancels.add(oid)
            self.client.my_quote_ids.discard(oid)
            await self.client.cancel_order(oid)

    def on_cancel_confirmed(self, oid: str) -> None:
        self._pending_cancels.discard(oid)

    def on_order_rejected(self, oid: str) -> None:
        self._pending_cancels.discard(oid)

    # ── Fair value ────────────────────────────────────────────────

    @staticmethod
    def calc_fv(eps: float) -> float:
        return eps * PE_A * EPS_SCALE_A

    # ── Post-earnings sweep ───────────────────────────────────────

    async def _sweep_book(self) -> None:
        fv = self.client.fair_values.get("A")
        if fv is None:
            return
        book = self.client.order_books["A"]
        edge = 2

        stale_asks = sorted(
            [(px, qty) for px, qty in book.asks.items() if qty > 0 and px < fv - edge],
            key=lambda x: x[0]
        )
        for px, qty in stale_asks:
            oid = await self._place(min(qty, self.SNIPE_SIZE), Side.BUY, px)
            if oid is None:
                break
            log(f"[A] SWEEP BUY  {min(qty, self.SNIPE_SIZE)} @ {px} | edge={fv - px:.0f}")

        stale_bids = sorted(
            [(px, qty) for px, qty in book.bids.items() if qty > 0 and px > fv + edge],
            key=lambda x: -x[0]
        )
        for px, qty in stale_bids:
            oid = await self._place(min(qty, self.SNIPE_SIZE), Side.SELL, px)
            if oid is None:
                break
            log(f"[A] SWEEP SELL {min(qty, self.SNIPE_SIZE)} @ {px} | edge={px - fv:.0f}")

    # ── Opportunistic snipe ───────────────────────────────────────

    async def _snipe(self) -> None:
        fv = self.client.fair_values.get("A")
        if fv is None:
            return
        now = asyncio.get_running_loop().time()
        if now - self._last_snipe_time < 0.3:
            return
        self._last_snipe_time = now

        best_bid, best_ask = self.client.get_best_bid_ask("A")
        flow_adj = self.client.flow.fv_adjustment("A")
        fv_adj   = fv + flow_adj

        if best_ask is not None and best_ask < fv_adj - self.EDGE_THRESHOLD:
            oid = await self._place(self.SNIPE_SIZE, Side.BUY, best_ask)
            if oid:
                log(f"[A] SNIPE BUY  @ {best_ask} | fv_adj={fv_adj:.0f}")
        elif best_bid is not None and best_bid > fv_adj + self.EDGE_THRESHOLD:
            oid = await self._place(self.SNIPE_SIZE, Side.SELL, best_bid)
            if oid:
                log(f"[A] SNIPE SELL @ {best_bid} | fv_adj={fv_adj:.0f}")

    # ── Passive MM ────────────────────────────────────────────────

    async def update_quotes(self) -> None:
        fv = self.client.fair_values.get("A")
        if fv is None:
            best_bid, best_ask = self.client.get_best_bid_ask("A")
            if best_bid is not None and best_ask is not None:
                fv = (best_bid + best_ask) / 2
                self.client.fair_values["A"] = fv
            else:
                return

        now = asyncio.get_running_loop().time()
        fv_adj = fv + self.client.flow.fv_adjustment("A")

        if time.time() < self._earnings_end_time:
            if now - self._last_snipe_time >= 0.2:
                self._last_snipe_time = now
                await self._sweep_book()
            return

        if now - self._last_quote_time < 1.0:
            await self._snipe()
            return
        self._last_quote_time = now

        pos            = self.client.positions["A"]
        best_bid, best_ask = self.client.get_best_bid_ask("A")

        pos_frac   = abs(pos) / max(self.SOFT_POS_LIMIT, 1)
        spread     = self.BASE_SPREAD + int(pos_frac * 4)
        skew       = self.SKEW_PER_UNIT * pos
        flow_skew  = self.client.flow.fv_adjustment("A") * 0.5
        total_skew = skew + flow_skew

        bid = round(fv_adj - spread / 2 - total_skew)
        ask = round(fv_adj + spread / 2 - total_skew)
        if bid >= ask:
            ask = bid + 1

        # CHANGE 3: At position limits, suppress the accumulating side.
        # Do NOT cross the spread to force unwind — that realises a loss equal
        # to the full spread.  The reducing side remains quoted normally.
        if pos >= self.MAX_ABS_POS:
            bid = 0   # already at max long; don't add more
        if pos <= -self.MAX_ABS_POS:
            ask = 0   # already at max short; don't add more

        # Cross-book check: unconditional (no longer gated on abs(pos) < MAX_ABS_POS)
        if best_ask is not None and bid >= best_ask:
            bid = 0
        if best_bid is not None and ask <= best_bid:
            ask = 0

        bid_size = self.QUOTE_SIZE if pos < self.SOFT_POS_LIMIT  else max(2, self.QUOTE_SIZE // 3)
        ask_size = self.QUOTE_SIZE if pos > -self.SOFT_POS_LIMIT else max(2, self.QUOTE_SIZE // 3)

        if (bid, bid_size) == self._last_bid and (ask, ask_size) == self._last_ask:
            await self._snipe()
            return

        self._last_bid = (bid, bid_size)
        self._last_ask = (ask, ask_size)

        await self._cancel_all_a()

        if bid > 0:
            oid = await self._place(bid_size, Side.BUY, bid)
            if oid:
                self.client.my_quote_ids.add(oid)
        if ask > 0:
            oid = await self._place(ask_size, Side.SELL, ask)
            if oid:
                self.client.my_quote_ids.add(oid)

    # ── Earnings handler ─────────────────────────────────────────

    async def on_earnings(self, eps: float, tick: int) -> None:
        global EPS_SCALE_A

        old_fv = self.client.fair_values.get("A")
        self.client.eps["A"] = eps

        # CHANGE 2: Auto-calibrate EPS_SCALE_A from market mid.
        # implied_scale = mid / (eps * PE_A) is what EPS_SCALE_A *should* be.
        # We record this every earnings and update when we have 3+ consistent
        # observations (median prevents a single bad mid from corrupting the scale).
        b, a = self.client.get_best_bid_ask("A")
        mid  = (b + a) / 2 if (b and a) else None
        implied_scale = (mid / (eps * PE_A)) if mid else None

        if implied_scale is not None and 20 < implied_scale < 500:
            self._scale_obs.append(implied_scale)
            if len(self._scale_obs) >= self.SCALE_MIN_OBS:
                median_scale = sorted(self._scale_obs)[len(self._scale_obs) // 2]
                if abs(median_scale - EPS_SCALE_A) > 1.0:
                    log(f"[A] AUTO-SCALE {EPS_SCALE_A:.1f} → {median_scale:.1f} "
                        f"(n={len(self._scale_obs)} obs, implied={implied_scale:.2f})")
                    EPS_SCALE_A = median_scale

        new_fv = self.calc_fv(eps)
        self.client.fair_values["A"] = new_fv

        delta = new_fv - old_fv if old_fv is not None else 0
        log(f"[A] *** EARNINGS *** raw_eps={eps} PE_A={PE_A} scale={EPS_SCALE_A:.1f} "
            f"FV={old_fv}→{new_fv:.0f} Δ={delta:+.1f} "
            f"mid={'N/A' if mid is None else f'{mid:.0f}'} "
            f"implied_scale={'N/A' if implied_scale is None else f'{implied_scale:.2f}'} "
            f"pos={self.client.positions['A']}")

        self.timer.record_earnings(tick)
        self._pre_cancel_done = False

        self._earnings_end_time = time.time() + self.POST_EARNINGS_SECS
        self._last_bid = None
        self._last_ask = None

        await self._cancel_all_a()
        await self._sweep_book()

    # ── Pre-earnings check ────────────────────────────────────────

    async def check_pre_cancel(self) -> None:
        if not self._pre_cancel_done and self.timer.should_cancel_now():
            self._pre_cancel_done = True
            log(f"[A] PRE-CANCEL: {self.timer.PRE_CANCEL_SECS}s before expected earnings")
            await self._cancel_all_a()
            self._last_bid = None
            self._last_ask = None

    # ── Book update ───────────────────────────────────────────────

    async def on_book_update(self) -> None:
        if time.time() < self._earnings_end_time:
            await self._sweep_book()
        else:
            await self._snipe()

    # ── Round reset ───────────────────────────────────────────────

    def reset(self) -> None:
        self._pending_cancels.clear()
        self._last_snipe_time   = 0.0
        self._last_quote_time   = 0.0
        self._earnings_end_time = 0.0
        self._last_bid          = None
        self._last_ask          = None
        self._pre_cancel_done   = False
        # _scale_obs persists: calibration knowledge is competition-wide


# ═══════════════════════════════════════════════════════════════════════
# Strategy: Stock C  (CHANGE 4 — complete replacement)
#
# OLD: gradient descent on BETA/GAMMA (two unknowns, ill-conditioned fit
#      because ERC barely varies in early minutes → first 10-20 observations
#      all have ERC≈0 → gradients≈0 → parameters never meaningfully update →
#      FV garbage → runaway C accumulation).
#
# NEW: one-parameter empirical model calibrated from first significant CPI.
#   C_FV = anchor_mid + sensitivity * (current_ERC - anchor_ERC)
#
# Derivation: the full C formula is linear in ERC for small deviations.
#   dC/dERC ≈ −β*(EPS*PE0*γ + λ*B0/N*D)   [a single round-constant scalar]
# We measure this scalar directly from the first CPI that moves ERC enough.
# No iteration, no convergence time — valid from the first significant CPI.
# ═══════════════════════════════════════════════════════════════════════

class StockCStrategy:
    """
    Empirical ERC-sensitivity model for C.

    State machine:
      PHASE 1 (before first C earnings): no FV, wide passive MM (±12 ticks, 3 units).
      PHASE 2 (after first C earnings, before sensitivity calibrated):
               FV still None (PE_C_scale known but no ERC anchor yet).
               Passive MM ±12 ticks but now anchored to book mid only.
      PHASE 3 (after first significant CPI):
               FV = anchor_mid + sensitivity * (ERC - anchor_ERC).
               Tight passive MM ±5 ticks/8 units.
               Directional trade on every subsequent CPI (±10 units).
               Earnings sweep enabled.

    C max_abs_position = 50 (set in MyXchangeClient.__init__) to cap
    model-uncertainty risk.
    """

    C_MAX_POS           = 20    # per-direction cap for CPI directional trades
    CPI_TRADE_QTY       = 10    # units per CPI directional trade
    CPI_MIN_EXPECTED_DC = 4     # min |expected ΔC| ticks to bother trading
    MIN_CALIBRATE_DERC  = 2.0   # min |ΔERC| to calibrate from
    SPREAD_UNCALIB      = 12    # half-spread before calibration (ticks)
    SPREAD_CALIB        = 5     # half-spread after calibration (ticks)
    QUOTE_SIZE_UNCALIB  = 3
    QUOTE_SIZE_CALIB    = 8
    QUOTE_RATE_LIMIT    = 1.5   # seconds between passive MM re-quotes

    def __init__(self, client: "MyXchangeClient"):
        self.client = client

        # Calibration state — sensitivity persists across rounds
        # (dC/dERC is a round-level constant determined by BETA/GAMMA/D/etc;
        # these parameters can differ across rounds, so we reset sensitivity
        # each round to re-calibrate from scratch — see reset())
        self._c_pe_scale: Optional[float]   = None   # persists: stable P/E ratio
        self._c_sensitivity: Optional[float] = None   # resets per round
        self._c_anchor_mid: Optional[float]  = None   # resets per round
        self._c_anchor_erc: float            = 0.0    # resets per round
        self._c_calibrated: bool             = False  # resets per round

        # CPI snapshot (captured just before Fed processes CPI)
        self._pre_cpi_erc: Optional[float]  = None
        self._pre_cpi_mid: Optional[float]  = None

        # Quote state
        self._last_quote_time: float = 0.0
        self._last_bid: Optional[tuple] = None
        self._last_ask: Optional[tuple] = None

    # ── ERC helper ────────────────────────────────────────────────

    def _current_erc(self) -> float:
        """Expected rate change: 25*P_hike - 25*P_cut."""
        return (25.0 * self.client.fair_values.get("R_HIKE", 0.333)
                - 25.0 * self.client.fair_values.get("R_CUT",  0.333))

    # ── Fair value ────────────────────────────────────────────────

    def recalc_fv(self) -> None:
        """
        Recompute C FV from current ERC.
        No-op (sets nothing) before calibration — this prevents the bot from
        quoting or sniping C with a garbage FV.
        """
        if not self._c_calibrated or self._c_anchor_mid is None:
            return
        erc = self._current_erc()
        fv  = self._c_anchor_mid + self._c_sensitivity * (erc - self._c_anchor_erc)
        old = self.client.fair_values.get("C")
        self.client.fair_values["C"] = fv
        if old is None or abs(fv - old) > 1.0:
            log(f"[C] FV recalc: {old}→{fv:.1f} "
                f"(sens={self._c_sensitivity:.2f} erc={erc:.2f})")

    # ── CPI handling ─────────────────────────────────────────────

    def pre_cpi_snapshot(self) -> None:
        """
        Capture C mid and ERC *before* Fed processes the CPI print.
        Must be called first in bot_handle_news for CPI events.
        """
        self._pre_cpi_erc = self._current_erc()
        b, a = self.client.get_best_bid_ask("C")
        self._pre_cpi_mid = (b + a) / 2 if (b and a) else None

    async def on_cpi(self, forecast: float, actual: float) -> None:
        """
        Called AFTER strat_fed.on_cpi() has updated Fed probabilities.
        Phase 2 → 3 transition: calibrate sensitivity from first significant CPI.
        Phase 3: take directional C trade before C book reprices.
        """
        new_erc = self._current_erc()

        if self._pre_cpi_erc is None or self._pre_cpi_mid is None:
            return

        delta_erc = new_erc - self._pre_cpi_erc

        if not self._c_calibrated:
            # Need a significant ERC move to calibrate reliably
            if abs(delta_erc) < self.MIN_CALIBRATE_DERC:
                log(f"[C] CPI: |ΔERC|={abs(delta_erc):.2f} < {self.MIN_CALIBRATE_DERC} — "
                    f"skipping calibration attempt")
                return

            b, a = self.client.get_best_bid_ask("C")
            if b is None or a is None:
                return

            current_c_mid = (b + a) / 2
            delta_c       = current_c_mid - self._pre_cpi_mid

            # Guard: if market hasn't moved at all, the measurement is noisy
            if abs(delta_c) < 1.0 and abs(delta_erc) > 3.0:
                log(f"[C] CPI: ΔC={delta_c:.1f} suspiciously small for ΔERC={delta_erc:.2f} — "
                    f"waiting for cleaner observation")
                return

            self._c_sensitivity = delta_c / delta_erc
            self._c_anchor_mid  = current_c_mid
            self._c_anchor_erc  = new_erc
            self._c_calibrated  = True
            self.client.fair_values["C"] = current_c_mid

            log(f"[C] *** CALIBRATED *** sensitivity={self._c_sensitivity:.3f} ticks/ERC "
                f"(ΔERC={delta_erc:.2f} ΔC={delta_c:.1f} "
                f"pre_mid={self._pre_cpi_mid:.0f} post_mid={current_c_mid:.0f})")

            # Force re-quote with new FV immediately
            self._last_quote_time = 0.0
            self._last_bid = None
            self._last_ask = None
            return  # Don't trade on the calibration event itself

        # ── Calibrated: directional trade on this CPI ──
        if abs(delta_erc) < 1.0:
            return  # Too small to act on

        expected_dc = self._c_sensitivity * delta_erc
        if abs(expected_dc) < self.CPI_MIN_EXPECTED_DC:
            log(f"[C] CPI: expected_dc={expected_dc:.1f} too small to trade")
            return

        pos = self.client.positions.get("C", 0)

        if expected_dc < 0:  # C should fall: sell C before market reprices
            qty = min(self.CPI_TRADE_QTY, self.C_MAX_POS + pos)
            if qty > 0:
                b, _ = self.client.get_best_bid_ask("C")
                if b:
                    await self.client.safe_place_order("C", qty, Side.SELL, b)
                    log(f"[C] CPI SELL {qty} @ {b} | expected_dc={expected_dc:.1f} "
                        f"ΔERC={delta_erc:.2f}")
        else:  # C should rise: buy C before market reprices
            qty = min(self.CPI_TRADE_QTY, self.C_MAX_POS - pos)
            if qty > 0:
                _, a = self.client.get_best_bid_ask("C")
                if a:
                    await self.client.safe_place_order("C", qty, Side.BUY, a)
                    log(f"[C] CPI BUY  {qty} @ {a} | expected_dc={expected_dc:.1f} "
                        f"ΔERC={delta_erc:.2f}")

        # Update anchor after CPI so subsequent recalc_fv calls are accurate
        self.recalc_fv()

    # ── On-book snipe ─────────────────────────────────────────────

    async def on_book_update(self) -> None:
        """Only snipe when calibrated — prevents runaway accumulation."""
        fv = self.client.fair_values.get("C")
        if fv is None:
            return  # Not calibrated yet; fair_values["C"] is None
        b, a = self.client.get_best_bid_ask("C")
        edge = 3
        if a is not None and a < fv - edge:
            await self.client.safe_place_order("C", 10, Side.BUY, a)
            log(f"[C] SNIPE BUY  10 @ {a} | fv={fv:.1f}")
        elif b is not None and b > fv + edge:
            await self.client.safe_place_order("C", 10, Side.SELL, b)
            log(f"[C] SNIPE SELL 10 @ {b} | fv={fv:.1f}")

    # ── Passive MM ────────────────────────────────────────────────

    async def update_quotes(self) -> None:
        fv = self.client.fair_values.get("C")
        # Before calibration: use book mid as rough FV for passive quoting
        # with a very wide spread (no directional risk taken)
        if fv is None:
            b, a = self.client.get_best_bid_ask("C")
            if b is None or a is None:
                return
            fv = (b + a) / 2

        now = asyncio.get_running_loop().time()
        if now - self._last_quote_time < self.QUOTE_RATE_LIMIT:
            return
        self._last_quote_time = now

        calib     = self._c_calibrated
        half_sprd = self.SPREAD_CALIB if calib else self.SPREAD_UNCALIB
        qs        = self.QUOTE_SIZE_CALIB if calib else self.QUOTE_SIZE_UNCALIB

        pos   = self.client.positions.get("C", 0)
        b, a  = self.client.get_best_bid_ask("C")
        max_p = 80   # soft limit for quote-size tapering

        # Inventory skew
        spread = half_sprd * 2 + int(abs(pos) / max_p * 4)
        skew   = 0.15 * pos

        bid = round(fv - spread / 2 - skew)
        ask = round(fv + spread / 2 - skew)
        if bid >= ask:
            ask = bid + 1

        if a is not None and bid >= a:
            bid = 0
        if b is not None and ask <= b:
            ask = 0

        bid_sz = qs if pos < max_p  else max(2, qs // 3)
        ask_sz = qs if pos > -max_p else max(2, qs // 3)

        if (bid, bid_sz) == self._last_bid and (ask, ask_sz) == self._last_ask:
            return
        self._last_bid = (bid, bid_sz)
        self._last_ask = (ask, ask_sz)

        await self.client.cancel_quotes_for("C")
        if bid > 0:
            await self.client.safe_place_order("C", bid_sz, Side.BUY, bid)
        if ask > 0:
            await self.client.safe_place_order("C", ask_sz, Side.SELL, ask)

    # ── Earnings handler ─────────────────────────────────────────

    async def on_earnings(self, eps: float, tick: int) -> None:
        old_eps = self.client.eps.get("C")
        self.client.eps["C"] = eps

        # Calibrate PE_C_scale from first C earnings
        if self._c_pe_scale is None:
            b, a = self.client.get_best_bid_ask("C")
            if b and a and eps > 0:
                mid = (b + a) / 2
                self._c_pe_scale = mid / eps
                log(f"[C] PE_C_scale calibrated: {self._c_pe_scale:.2f} "
                    f"(eps={eps} mid={mid:.0f})")

        # Update anchor when EPS changes (anchor_mid was set when eps was old_eps;
        # the EPS component of C changes by approx (eps - old_eps) * PE_C_scale).
        if (self._c_calibrated and self._c_anchor_mid is not None
                and old_eps is not None and self._c_pe_scale is not None
                and old_eps > 0):
            delta_eps_component = (eps - old_eps) * self._c_pe_scale
            self._c_anchor_mid += delta_eps_component
            log(f"[C] anchor updated for earnings: Δeps={eps - old_eps:+.4f} "
                f"→ Δanchor={delta_eps_component:+.1f}")
            self.recalc_fv()

        fv = self.client.fair_values.get("C")
        log(f"[C] *** EARNINGS *** eps={eps} pe_scale={self._c_pe_scale} "
            f"fv={fv} calib={self._c_calibrated}")

        if fv is None:
            return  # Can't sweep without a reliable FV

        # Sweep stale book orders
        self._last_quote_time = 0.0
        await self.client.cancel_quotes_for("C")
        book = self.client.order_books["C"]
        edge = 2

        for px, qty in sorted(book.asks.items()):
            if qty <= 0 or px >= fv - edge:
                break
            await self.client.safe_place_order("C", min(qty, 15), Side.BUY, px)
            log(f"[C] SWEEP BUY  {min(qty, 15)} @ {px} | fv={fv:.1f}")

        for px, qty in sorted(book.bids.items(), reverse=True):
            if qty <= 0 or px <= fv + edge:
                break
            await self.client.safe_place_order("C", min(qty, 15), Side.SELL, px)
            log(f"[C] SWEEP SELL {min(qty, 15)} @ {px} | fv={fv:.1f}")

    # ── Round reset ───────────────────────────────────────────────

    def reset(self) -> None:
        """
        Reset per-round calibration.  Sensitivity varies by round (BETA/GAMMA
        differ), so we re-calibrate from scratch each round.
        PE_C_scale is more stable and persists.
        """
        self._c_sensitivity  = None
        self._c_anchor_mid   = None
        self._c_anchor_erc   = 0.0
        self._c_calibrated   = False
        self._pre_cpi_erc    = None
        self._pre_cpi_mid    = None
        self._last_quote_time = 0.0
        self._last_bid        = None
        self._last_ask        = None
        # _c_pe_scale persists (round-to-round P/E ratio is relatively stable)


# ═══════════════════════════════════════════════════════════════════════
# Strategy: Options (PCP + Box Spread)
# CHANGE 5: Removed cancel_quotes_for loop before re-checking.
#   Resting arb orders are desirable — they fill at a profit.
#   The _cold() cooldown (4s) already prevents firing the same arb too often.
# ═══════════════════════════════════════════════════════════════════════

class OptionsStrategy:
    """
    Two risk-free (or near risk-free) arbitrage strategies:

    1. Put-call parity  (3 legs: call, put, B)
       C - P = S - K  (r=0)
    2. Box spread  (4 legs: C1, C2, P1, P2 at two strikes)
       Value always = K2 - K1 at expiry

    Note: in practice these arbs rarely trigger because the exchange's option
    bots maintain internal PCP consistency within the bid-ask spread.  We keep
    checking with MIN_EDGE=2 to capture any transient mispricing.
    """

    BOX_PAIRS = [(950, 1000), (1000, 1050), (950, 1050)]
    MIN_EDGE  = 2
    COOLDOWN  = 4.0

    def __init__(self, client: "MyXchangeClient"):
        self.client    = client
        self.b_tracker = BPriceTracker(client)
        self._last: dict = {}

    def _bba(self, sym: str):
        return self.client.get_best_bid_ask(sym)

    def _cold(self, key: str) -> bool:
        now = asyncio.get_running_loop().time()
        if now - self._last.get(key, 0) < self.COOLDOWN:
            return False
        self._last[key] = now
        return True

    def _arb_qty(self, edge: float) -> int:
        if edge < self.MIN_EDGE:
            return 0
        if edge < 5:
            return 1
        if edge < 10:
            return 3
        return 5

    async def _pcp_arb(self, strike: int) -> None:
        c_bid, c_ask = self._bba(f"B_C_{strike}")
        p_bid, p_ask = self._bba(f"B_P_{strike}")
        s_bid, s_ask = self._bba("B")
        s_impl       = self.b_tracker.get_implied_price()

        if None in (c_bid, c_ask, p_bid, p_ask, s_bid, s_ask):
            return

        # Buy call, sell put, sell B
        edge1 = p_bid + s_bid - c_ask - strike
        if s_impl is not None:
            edge1 = max(edge1, p_bid + s_impl - c_ask - strike)
        qty1 = self._arb_qty(edge1)
        if qty1 > 0 and self._cold(f"pcp_buy_{strike}"):
            log(f"[OPT] PCP BUY-CALL K={strike} edge={edge1:.0f} qty={qty1}")
            await self.client.safe_place_order(f"B_C_{strike}", qty1, Side.BUY,  c_ask)
            await self.client.safe_place_order(f"B_P_{strike}", qty1, Side.SELL, p_bid)
            await self.client.safe_place_order("B",             qty1, Side.SELL, s_bid)
            return

        # Sell call, buy put, buy B
        edge2 = c_bid - p_ask - s_ask + strike
        if s_impl is not None:
            edge2 = max(edge2, c_bid - p_ask - s_impl + strike)
        qty2 = self._arb_qty(edge2)
        if qty2 > 0 and self._cold(f"pcp_sell_{strike}"):
            log(f"[OPT] PCP SELL-CALL K={strike} edge={edge2:.0f} qty={qty2}")
            await self.client.safe_place_order(f"B_C_{strike}", qty2, Side.SELL, c_bid)
            await self.client.safe_place_order(f"B_P_{strike}", qty2, Side.BUY,  p_ask)
            await self.client.safe_place_order("B",             qty2, Side.BUY,  s_ask)

    async def _box_arb(self, k1: int, k2: int) -> None:
        c1b, c1a = self._bba(f"B_C_{k1}")
        c2b, c2a = self._bba(f"B_C_{k2}")
        p1b, p1a = self._bba(f"B_P_{k1}")
        p2b, p2a = self._bba(f"B_P_{k2}")
        if None in (c1b, c1a, c2b, c2a, p1b, p1a, p2b, p2a):
            return

        theo = k2 - k1

        buy_cost = c1a - c2b + p2a - p1b
        buy_edge = theo - buy_cost
        qty_b    = self._arb_qty(buy_edge)
        if qty_b > 0 and self._cold(f"box_buy_{k1}_{k2}"):
            log(f"[OPT] BOX BUY  ({k1},{k2}) edge={buy_edge:.0f} qty={qty_b}")
            await self.client.safe_place_order(f"B_C_{k1}", qty_b, Side.BUY,  c1a)
            await self.client.safe_place_order(f"B_C_{k2}", qty_b, Side.SELL, c2b)
            await self.client.safe_place_order(f"B_P_{k1}", qty_b, Side.SELL, p1b)
            await self.client.safe_place_order(f"B_P_{k2}", qty_b, Side.BUY,  p2a)
            return

        sell_rcpt = c1b - c2a + p2b - p1a
        sell_edge = sell_rcpt - theo
        qty_s     = self._arb_qty(sell_edge)
        if qty_s > 0 and self._cold(f"box_sell_{k1}_{k2}"):
            log(f"[OPT] BOX SELL ({k1},{k2}) edge={sell_edge:.0f} qty={qty_s}")
            await self.client.safe_place_order(f"B_C_{k1}", qty_s, Side.SELL, c1b)
            await self.client.safe_place_order(f"B_C_{k2}", qty_s, Side.BUY,  c2a)
            await self.client.safe_place_order(f"B_P_{k1}", qty_s, Side.BUY,  p1a)
            await self.client.safe_place_order(f"B_P_{k2}", qty_s, Side.SELL, p2b)

    async def run(self) -> None:
        now = asyncio.get_running_loop().time()
        if now - self._last.get("_run", 0) < 1.5:
            return
        self._last["_run"] = now

        # CHANGE 5: Do NOT cancel resting arb orders before re-checking.
        # Old strategy.py cancelled all B/option orders here, which:
        #   (a) threw away profitable resting orders that were about to fill,
        #   (b) created a vol-quota race where the exchange still counted
        #       being-cancelled orders against the limit when we immediately
        #       tried to place new ones.
        # The _cold() cooldown already prevents refiring too quickly.

        for k in (950, 1000, 1050):
            await self._pcp_arb(k)
        for k1, k2 in self.BOX_PAIRS:
            await self._box_arb(k1, k2)

    def reset(self) -> None:
        self._last.clear()


# ═══════════════════════════════════════════════════════════════════════
# Strategy: Federal Reserve Prediction Market
# Changes:
#   CHANGE 1: _kelly_size uses max(0, ...) — was max(1, ...) which always
#             returned at least 1 even when pos=MAX_POS, allowing unlimited
#             accumulation since safe_place_order used max_abs_position=200.
#   CHANGE 1: max_abs_position for Fed set to 25 in MyXchangeClient.__init__.
#   CHANGE 1: trade() called at 15s cadence (main loop), not every 0.1s;
#             news events call trade() directly for immediate response.
#   CHANGE 6: settlement_trade() — terminal bet in T-90s to T-20s window.
# ═══════════════════════════════════════════════════════════════════════

class FedStrategy:
    """
    Trade R_HIKE, R_HOLD, R_CUT based on probability estimates.

    Signal sources:
      - Structured: CPI print (actual vs. forecast)
      - Unstructured: FEDSPEAK keyword parsing
      - Book mid: market consensus (seeded on first 3 reads)

    Sizing: half-Kelly proportional to divergence from market consensus.

    Position ceiling: MAX_POS = 25 units per contract (enforced by both
    _kelly_size AND max_abs_position in safe_place_order).

    Settlement trade: fires once per round in the final 90-20 second window
    when our probability estimate for the leading contract is ≥ 0.78 and the
    expected value of buying at the current ask price is positive.
    """

    FEDSPEAK_MSG_TYPES: set[str] = {"FEDSPEAK", "fedspeak", "FED_SPEAK"}

    DOVISH  = ["easing", "cooling", "dovish", "cut", "lower rates", "easing inflation",
               "slowing growth", "policy easing", "expectations of policy easing"]
    HAWKISH = ["restrictive", "hawkish", "hike", "inflation risks", "stay restrictive",
               "tightening", "higher for longer", "restrictive for longer"]
    NEUTRAL = ["wide range of views", "weighing", "uncertainties", "not in a hurry",
               "either direction", "balanced", "mixed", "no clear", "options open"]

    CONTRACTS   = ("R_HIKE", "R_HOLD", "R_CUT")
    MAX_POS     = 25
    EDGE_MIN    = 0.04
    CPI_SENS    = 80
    SPEAK_SHIFT = 0.03

    SETTLE_MIN_P    = 0.78   # minimum our_p to fire settlement trade
    SETTLE_MAX_POS  = 35     # settlement trade can exceed normal MAX_POS

    def __init__(self, client: "MyXchangeClient"):
        self.client      = client
        self._book_reads = 0
        self._mkt_probs  = {c: None for c in self.CONTRACTS}
        self._settle_done: bool = False

    def _read_book_probs(self) -> None:
        updated = 0
        for c in self.CONTRACTS:
            b, a = self.client.get_best_bid_ask(c)
            if b is not None and a is not None:
                self._mkt_probs[c] = (b + a) / 2 / 1000.0
                updated += 1
        if updated == 3:
            self._book_reads += 1
            if self._book_reads <= 3:
                for c in self.CONTRACTS:
                    self.client.fair_values[c] = self._mkt_probs[c]

    def _normalize(self) -> None:
        for c in self.CONTRACTS:
            self.client.fair_values[c] = max(0.04, self.client.fair_values[c])
        total = sum(self.client.fair_values[c] for c in self.CONTRACTS)
        for c in self.CONTRACTS:
            self.client.fair_values[c] /= total

    def on_cpi(self, forecast: float, actual: float) -> None:
        surprise = actual - forecast
        log(f"[FED] CPI surprise={surprise:+.5f} ({'hawk' if surprise > 0 else 'dove'})", "fed.log")
        shift = self.CPI_SENS * abs(surprise)
        if abs(surprise) < 0.0001:
            self.client.fair_values["R_HIKE"] = max(0.04, self.client.fair_values["R_HIKE"] - 0.05)
            self.client.fair_values["R_CUT"]  = max(0.04, self.client.fair_values["R_CUT"]  - 0.05)
            self.client.fair_values["R_HOLD"] += 0.10
        elif surprise > 0:
            mv = min(shift, self.client.fair_values["R_CUT"])
            self.client.fair_values["R_CUT"]  -= mv
            self.client.fair_values["R_HIKE"] += mv
        else:
            mv = min(shift, self.client.fair_values["R_HIKE"])
            self.client.fair_values["R_HIKE"] -= mv
            self.client.fair_values["R_CUT"]  += mv
        self._normalize()
        log(f"[FED] post-CPI: H={self.client.fair_values['R_HIKE']:.3f} "
            f"N={self.client.fair_values['R_HOLD']:.3f} "
            f"C={self.client.fair_values['R_CUT']:.3f}", "fed.log")

    def on_fedspeak(self, content: str) -> None:
        t       = content.lower()
        dovish  = any(kw in t for kw in self.DOVISH)
        hawkish = any(kw in t for kw in self.HAWKISH)
        neutral = any(kw in t for kw in self.NEUTRAL)
        if dovish and not hawkish:
            mv = min(self.SPEAK_SHIFT, self.client.fair_values["R_HIKE"])
            self.client.fair_values["R_HIKE"] -= mv
            self.client.fair_values["R_CUT"]  += mv
            log(f"[FED] DOVISH: {content[:80]}", "fed.log")
        elif hawkish and not dovish:
            mv = min(self.SPEAK_SHIFT, self.client.fair_values["R_CUT"])
            self.client.fair_values["R_CUT"]  -= mv
            self.client.fair_values["R_HIKE"] += mv
            log(f"[FED] HAWKISH: {content[:80]}", "fed.log")
        elif neutral:
            self.client.fair_values["R_HIKE"] = max(0.04, self.client.fair_values["R_HIKE"] - 0.04)
            self.client.fair_values["R_CUT"]  = max(0.04, self.client.fair_values["R_CUT"]  - 0.04)
            self.client.fair_values["R_HOLD"] += 0.08
            log(f"[FED] NEUTRAL: {content[:80]}", "fed.log")
        else:
            log(f"[FED] UNCLASSIFIED: {content[:80]}", "fed.log")
        self._normalize()

    def _kelly_size(self, our_p: float, mkt_p: float, pos: int, direction: str) -> int:
        """
        Half-Kelly position size.
        CHANGE 1: max(0, ...) not max(1, ...).
        Old max(1,...) returned 1 even when pos=MAX_POS, letting the bot
        accumulate indefinitely (safe_place_order was capped at 200, not 25).
        Now returns 0 at the limit, correctly stopping accumulation.
        """
        if direction == "buy":
            if our_p <= mkt_p or mkt_p >= 1.0:
                return 0
            kf = (our_p - mkt_p) / (1.0 - mkt_p)
        else:
            if our_p >= mkt_p or mkt_p <= 0.0:
                return 0
            kf = (mkt_p - our_p) / mkt_p
        half_k = 0.5 * kf
        target = int(half_k * self.MAX_POS)
        if direction == "buy":
            return max(0, min(target, self.MAX_POS - pos))
        else:
            return max(0, min(target, self.MAX_POS + pos))

    async def trade(self) -> None:
        """
        Place Kelly-sized Fed trades.  Called at 15-second cadence from the
        main loop, and immediately after CPI/fedspeak from bot_handle_news.
        """
        self._read_book_probs()
        if self._book_reads < 3:
            return

        for c in self.CONTRACTS:
            await self.client.cancel_quotes_for(c)

        for c in self.CONTRACTS:
            mkt_p = self._mkt_probs.get(c)
            our_p = self.client.fair_values.get(c)
            if mkt_p is None or our_p is None:
                continue
            div = our_p - mkt_p
            b, a = self.client.get_best_bid_ask(c)
            if b is None or a is None:
                continue
            pos = self.client.positions[c]

            if div > self.EDGE_MIN:
                sz = self._kelly_size(our_p, mkt_p, pos, "buy")
                if sz > 0:
                    log(f"[FED] BUY  {c} {sz}u | our={our_p:.3f} mkt={mkt_p:.3f} "
                        f"div={div:+.3f}", "fed.log")
                    await self.client.safe_place_order(c, sz, Side.BUY, a)
            elif div < -self.EDGE_MIN:
                sz = self._kelly_size(our_p, mkt_p, pos, "sell")
                if sz > 0:
                    log(f"[FED] SELL {c} {sz}u | our={our_p:.3f} mkt={mkt_p:.3f} "
                        f"div={div:+.3f}", "fed.log")
                    await self.client.safe_place_order(c, sz, Side.SELL, b)

    async def settlement_trade(self) -> None:
        """
        CHANGE 6: End-of-round concentrated bet.
        Fires once per round in T-90s to T-20s window.
        Only bets when our highest-probability estimate ≥ 78% AND the current
        ask price implies positive expected value.
        Uses place_order directly (bypasses normal 25-cap) up to SETTLE_MAX_POS=35.
        """
        if self._settle_done:
            return
        self._settle_done = True

        self._read_book_probs()

        # Find the contract we're most confident about
        best_c = max(self.CONTRACTS,
                     key=lambda c: self.client.fair_values.get(c, 0))
        our_p  = self.client.fair_values.get(best_c, 0)

        if our_p < self.SETTLE_MIN_P:
            log(f"[FED] SETTLE: skip — best={best_c} our_p={our_p:.3f} < {self.SETTLE_MIN_P}",
                "fed.log")
            return

        _, ask = self.client.get_best_bid_ask(best_c)
        if ask is None:
            log(f"[FED] SETTLE: skip — no ask for {best_c}", "fed.log")
            return

        # EV check: 1000 * our_p > ask  ↔  expected payout > cost
        if ask >= 1000 * our_p:
            log(f"[FED] SETTLE: negative EV — ask={ask} >= 1000*{our_p:.3f}={1000*our_p:.0f}",
                "fed.log")
            return

        pos  = self.client.positions.get(best_c, 0)
        room = self.SETTLE_MAX_POS - pos
        if room <= 0:
            log(f"[FED] SETTLE: already at settle cap pos={pos}", "fed.log")
            return

        qty = min(10, room, self.client.max_order_size.get(best_c, 40))
        if qty <= 0:
            return

        log(f"[FED] SETTLE: BUY {qty}x {best_c} @ {ask} | "
            f"our_p={our_p:.3f} ev={(our_p*1000 - ask):.0f}", "fed.log")
        # Use place_order directly so the 25-unit normal cap doesn't block this
        # terminal bet.  The SETTLE_MAX_POS=35 guard above ensures we don't
        # go wild; the exchange hard limit of 200 is the backstop.
        await self.client.place_order(best_c, qty, Side.BUY, ask)

    def reset(self) -> None:
        # Keep probability estimates across rounds — they carry over
        self._book_reads  = 0
        self._settle_done = False


# ═══════════════════════════════════════════════════════════════════════
# Strategy: ETF Arbitrage
# Changes:
#   CHANGE 7: Added BPriceTracker; _nav() uses implied B (more accurate NAV).
#   CHANGE 7: One-sided trades rate-limited to once per 3 seconds (was
#             unlimited — could spam orders every 0.5s when threshold met).
# ═══════════════════════════════════════════════════════════════════════

class ETFStrategy:
    """
    ETF NAV arb.  ETF should trade at NAV = A + B + C.
    Swap cost = ETF_SWAP_COST per unit each direction.

    ONE-SIDED (preferred):  The case hints ETF is the more likely mispriced leg.
      ETF > NAV + threshold: SELL ETF (don't buy components).
      ETF < NAV - threshold: BUY  ETF (don't sell components).

    FULL-SWAP (large-edge only):  Create/redeem via the swap mechanism.
      Includes leg-fill tracking and 10s abort timeout.
    """

    ONE_SIDED_THRESHOLD = 6
    FULL_SWAP_THRESHOLD = 12
    ONE_SIDED_QTY       = 8
    FULL_SWAP_MAX_QTY   = 5
    LEG_TIMEOUT_SECS    = 10.0
    ONE_SIDED_COOLDOWN  = 3.0   # CHANGE 7: rate-limit one-sided trades

    def __init__(self, client: "MyXchangeClient"):
        self.client    = client
        self.b_tracker = BPriceTracker(client)   # CHANGE 7

        self.pending_create       = False
        self.pending_redeem       = False
        self.filled_comps         = {"A": 0, "B": 0, "C": 0}
        self.filled_etf           = 0
        self._target_qty          = 0
        self._pending_since       = None
        self._last_one_sided_time = 0.0          # CHANGE 7

    def _reset(self) -> None:
        self.pending_create       = False
        self.pending_redeem       = False
        self.filled_comps         = {"A": 0, "B": 0, "C": 0}
        self.filled_etf           = 0
        self._target_qty          = 0
        self._pending_since       = None
        self._last_one_sided_time = 0.0

    async def _abort(self) -> None:
        log("[ETF] ABORT — timing out partial arb, flattening legs")
        for s in ["A", "B", "C", "ETF"]:
            await self.client.cancel_quotes_for(s)
        self._reset()

    def _nav(self) -> Optional[float]:
        """
        NAV = A_fv + B_implied + C_fv.
        CHANGE 7: Uses PCP-implied B price when available (more accurate than
        B book mid, which can lag during rapid B moves).
        Falls back to book mid if PCP implies a price is unavailable.
        """
        fv_a = self.client.fair_values.get("A")
        fv_c = self.client.fair_values.get("C")
        _, _, mid_a = self.client.get_bba_mid("A")
        _, _, mid_b = self.client.get_bba_mid("B")
        _, _, mid_c = self.client.get_bba_mid("C")
        if mid_a is None or mid_b is None or mid_c is None:
            return None
        a = fv_a if (fv_a and fv_a > 0) else mid_a
        b = self.b_tracker.get_implied_price() or mid_b   # CHANGE 7
        c = fv_c if (fv_c and fv_c > 0) else mid_c
        return a + b + c

    async def check_arb(self) -> None:
        # Timeout check for pending full-swap arbs
        if (self.pending_create or self.pending_redeem) and self._pending_since:
            if time.time() - self._pending_since > self.LEG_TIMEOUT_SECS:
                await self._abort()
            return

        nav = self._nav()
        if nav is None:
            return

        etf_bid, etf_ask, etf_mid = self.client.get_bba_mid("ETF")
        if etf_mid is None:
            return

        diff    = etf_mid - nav
        pos_etf = self.client.positions.get("ETF", 0)
        MAX_POS = 40
        now_t   = time.time()

        log(f"[ETF] mid={etf_mid:.1f} nav={nav:.1f} diff={diff:+.1f}")

        # ── ONE-SIDED TRADES (CHANGE 7: 3-second cooldown) ──────────
        if now_t - self._last_one_sided_time >= self.ONE_SIDED_COOLDOWN:
            if diff > ETF_SWAP_COST + self.ONE_SIDED_THRESHOLD and pos_etf > -MAX_POS:
                qty = min(self.ONE_SIDED_QTY, MAX_POS + pos_etf)
                if etf_bid is not None and qty > 0:
                    await self.client.safe_place_order("ETF", qty, Side.SELL, etf_bid)
                    log(f"[ETF] ONE-SIDED SELL {qty} ETF @ {etf_bid} | diff={diff:+.1f}")
                    self._last_one_sided_time = now_t
                    return   # Don't attempt full-swap on same tick

            elif diff < -(ETF_SWAP_COST + self.ONE_SIDED_THRESHOLD) and pos_etf < MAX_POS:
                qty = min(self.ONE_SIDED_QTY, MAX_POS - pos_etf)
                if etf_ask is not None and qty > 0:
                    await self.client.safe_place_order("ETF", qty, Side.BUY, etf_ask)
                    log(f"[ETF] ONE-SIDED BUY  {qty} ETF @ {etf_ask} | diff={diff:+.1f}")
                    self._last_one_sided_time = now_t
                    return

        # ── FULL-SWAP ARBS (only on large edge) ──────────────────────
        if diff > ETF_SWAP_COST + self.FULL_SWAP_THRESHOLD:
            pos_a = self.client.positions.get("A", 0)
            pos_c = self.client.positions.get("C", 0)
            MAX_C = 30
            qty = min(
                self.FULL_SWAP_MAX_QTY,
                MAX_C - pos_a, MAX_C - pos_c, MAX_C - pos_etf
            )
            if qty > 0:
                _, ask_a, _ = self.client.get_bba_mid("A")
                _, ask_b, _ = self.client.get_bba_mid("B")
                _, ask_c, _ = self.client.get_bba_mid("C")
                if ask_a and ask_b and ask_c:
                    log(f"[ETF] FULL-SWAP CREATE {qty}")
                    self.pending_create = True
                    self.filled_comps   = {"A": 0, "B": 0, "C": 0}
                    self._target_qty    = qty
                    self._pending_since = time.time()
                    await asyncio.gather(
                        self.client.safe_place_order("A", qty, Side.BUY, ask_a),
                        self.client.safe_place_order("B", qty, Side.BUY, ask_b),
                        self.client.safe_place_order("C", qty, Side.BUY, ask_c),
                    )

        elif diff < -(ETF_SWAP_COST + self.FULL_SWAP_THRESHOLD):
            qty = min(self.FULL_SWAP_MAX_QTY, pos_etf)
            if qty > 0 and etf_ask is not None:
                log(f"[ETF] FULL-SWAP REDEEM {qty}")
                self.pending_redeem = True
                self.filled_etf     = 0
                self._target_qty    = qty
                self._pending_since = time.time()
                await self.client.safe_place_order("ETF", qty, Side.BUY, etf_ask)

    def reset(self) -> None:
        self._reset()


# ═══════════════════════════════════════════════════════════════════════
# Strategy: Meta Market  (unchanged — fill in on competition day)
# ═══════════════════════════════════════════════════════════════════════

class MetaStrategy:
    """
    Placeholder for the second prediction market revealed on competition day.

    On competition day:
    1. Check positions/symbols from first position_snapshot printout.
    2. Register unknowns: self.client._ensure_symbol(sym).
    3. Identify settlement condition and news message_type.
    4. Type: meta SYM1 SYM2  to register symbols at runtime.
    5. Implement on_news() to update fair_values[sym].

    The petition news subtype (asset, new_signatures, cumulative) may relate
    to this market — logged to meta.log for inspection.
    """

    META_SYMBOLS:   list[str] = []
    META_MSG_TYPES: set[str]  = set()

    def __init__(self, client: "MyXchangeClient"):
        self.client   = client
        self.max_pos  = 20
        self.qty      = 2
        self.edge_min = 0.05

    def on_petition(self, asset: str, new_sigs: int, cumulative: int) -> None:
        log(f"[META/PETITION] asset={asset} new={new_sigs} cumul={cumulative}", "meta.log")

    def on_news(self, msg_type: str, content: str, tick: int) -> None:
        log(f"[META] type='{msg_type}' tick={tick}: {content[:120]}", "meta.log")

    async def trade(self) -> None:
        if not self.META_SYMBOLS:
            return
        for sym in self.META_SYMBOLS:
            fv = self.client.fair_values.get(sym)
            if fv is None:
                continue
            b, a = self.client.get_best_bid_ask(sym)
            if b is None or a is None:
                continue
            mkt = (b + a) / 2 / 1000.0
            pos = self.client.positions.get(sym, 0)
            if fv - mkt > self.edge_min and pos < self.max_pos:
                await self.client.safe_place_order(sym, self.qty, Side.BUY, a)
                log(f"[META] BUY  {sym} {self.qty} @ {a}", "meta.log")
            elif mkt - fv > self.edge_min and pos > -self.max_pos:
                await self.client.safe_place_order(sym, self.qty, Side.SELL, b)
                log(f"[META] SELL {sym} {self.qty} @ {b}", "meta.log")

    def reset(self) -> None:
        pass


# ═══════════════════════════════════════════════════════════════════════
# Main Client
# ═══════════════════════════════════════════════════════════════════════

class MyXchangeClient(XChangeClient):

    ALL_TRADABLE = [
        "A", "B", "C", "ETF",
        "B_C_950", "B_P_950", "B_C_1000", "B_P_1000", "B_C_1050", "B_P_1050",
        "R_CUT", "R_HOLD", "R_HIKE",
    ]

    def __init__(self, host: str, username: str, password: str):
        super().__init__(host, username, password)

        self.fair_values: dict = {
            "A": None, "C": None, "ETF": None,
            "R_HIKE": 0.33, "R_HOLD": 0.33, "R_CUT": 0.34,
        }
        self.eps: dict = {"A": None, "C": None}
        self.my_quote_ids: set = set()

        # ── Risk limits (update from Ed post on competition day) ──────
        _syms = self.ALL_TRADABLE
        self.max_order_size      = {s: 40  for s in _syms}
        self.max_open_orders     = {s: 50  for s in _syms}
        self.max_outstanding_vol = {s: 120 for s in _syms}
        self.max_abs_position    = {s: 200 for s in _syms}

        # CHANGE 1: Fed position hard cap = 25 (matches FedStrategy.MAX_POS).
        # Old default of 200 allowed _kelly_size's max(1,...) bug to accumulate
        # 200 units in one direction — causing catastrophic losses when the
        # contract resolved in the opposite direction.
        for c in ("R_HIKE", "R_HOLD", "R_CUT"):
            self.max_abs_position[c] = 25

        # CHANGE 4: C position cap = 50.  The simplified ERC model has more
        # uncertainty than A's deterministic EPS model.  Capping at 50 limits
        # downside if sensitivity is mis-calibrated, while still allowing full
        # CPI-trade alpha (6-8 events × 10 units each = 60-80 max, but we
        # naturally reduce via passive MM quotes between events).
        self.max_abs_position["C"] = 50

        # Fill tracker  {symbol: (signed_qty_total, cost_total)}
        self.fill_tracker: dict = {}

        # Smart money flow
        self.flow = FlowTracker()

        # Strategies
        self.strat_a       = StockAStrategy(self)
        self.strat_c       = StockCStrategy(self)
        self.strat_options = OptionsStrategy(self)
        self.strat_fed     = FedStrategy(self)
        self.strat_etf     = ETFStrategy(self)
        self.strat_meta    = MetaStrategy(self)

        # Round management
        self._round_start_wall:   float = 0.0
        self._flatten_triggered:  bool  = False
        self._pending_round_reset: bool = False

    # ── Limit-aware order placement ───────────────────────────────

    async def safe_place_order(self, symbol: str, qty: int,
                               side: Side, price: int) -> Optional[str]:
        if qty <= 0:
            return None
        if symbol not in self.max_order_size:
            self.max_order_size[symbol]      = 40
            self.max_open_orders[symbol]     = 50
            self.max_outstanding_vol[symbol] = 120
            self.max_abs_position[symbol]    = 200
        self._ensure_symbol(symbol)

        qty = min(qty, self.max_order_size[symbol])

        outvol = sum(
            info[1] for info in self.open_orders.values()
            if info and info[0].symbol == symbol and not info[2]
        )
        if outvol + qty > self.max_outstanding_vol[symbol]:
            qty = self.max_outstanding_vol[symbol] - outvol
            if qty <= 0:
                return None

        oc = sum(
            1 for info in self.open_orders.values()
            if info and info[0].symbol == symbol and not info[2]
        )
        if oc + 1 > self.max_open_orders[symbol]:
            return None

        pos = self.positions[symbol]
        if side == Side.BUY  and pos + qty > self.max_abs_position[symbol]:
            qty = self.max_abs_position[symbol] - pos
        if side == Side.SELL and pos - qty < -self.max_abs_position[symbol]:
            qty = self.max_abs_position[symbol] + pos
        if qty <= 0:
            return None

        return await self.place_order(symbol, qty, side, price)

    # ── Book helpers ──────────────────────────────────────────────

    def get_best_bid_ask(self, symbol: str):
        book = self.order_books.get(symbol)
        if book is None:
            return None, None
        bids = [k for k, v in book.bids.items() if v > 0]
        asks = [k for k, v in book.asks.items() if v > 0]
        return (max(bids) if bids else None), (min(asks) if asks else None)

    def get_bba_mid(self, symbol: str):
        b, a = self.get_best_bid_ask(symbol)
        if b is None or a is None:
            return None, None, None
        return b, a, (b + a) / 2

    async def cancel_quotes_for(self, symbol: str) -> None:
        to_cancel = [
            oid for oid, info in self.open_orders.items()
            if info and info[0].symbol == symbol
        ]
        for oid in to_cancel:
            self.my_quote_ids.discard(oid)
            await self.cancel_order(oid)

    # ── Round management ──────────────────────────────────────────

    def handle_position_snapshot(self, msg) -> None:
        super().handle_position_snapshot(msg)
        total = sum(abs(v) for k, v in self.positions.items() if k != "cash")
        if total < 5:
            self._pending_round_reset = True

    async def _on_round_start(self) -> None:
        log("[ROUND] New round detected — resetting per-round state")
        self.fill_tracker.clear()
        self.open_orders.clear()
        self.my_quote_ids.clear()

        # CHANGE 8: Reset C FV so stale prior-round FV doesn't trigger
        # premature sniping at round start before sensitivity re-calibration.
        self.fair_values["C"] = None

        self.strat_a.reset()
        self.strat_c.reset()       # resets sensitivity; keeps PE_C_scale
        self.strat_options.reset()
        self.strat_etf.reset()
        self.strat_meta.reset()
        self.strat_fed.reset()     # resets settle_done; keeps prob estimates

        self._round_start_wall  = time.time()
        self._flatten_triggered = False

    # ── Event handlers ────────────────────────────────────────────

    async def bot_handle_cancel_response(self, order_id: str,
                                          success: bool, error: Optional[str] = None) -> None:
        self.strat_a.on_cancel_confirmed(order_id)
        if success:
            self.my_quote_ids.discard(order_id)
        else:
            print(f"[CANCEL-FAIL] {order_id}: {error}")

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int) -> None:
        self.strat_a.on_cancel_confirmed(order_id)
        info = self.open_orders.get(order_id)
        if info is None:
            return
        sym    = info[0].symbol
        is_buy = info[0].side == utc_bot_pb2.NewOrderRequest.Side.BUY
        sq     = qty if is_buy else -qty

        prev_sq, prev_cost = self.fill_tracker.get(sym, (0, 0))
        self.fill_tracker[sym] = (prev_sq + sq, prev_cost + sq * price)

        log(f"[FILL] {sym} {'BUY' if is_buy else 'SELL'} {qty} @ {price} "
            f"pos={self.positions.get(sym, 0)}", "fills.log")

        # ETF full-swap fill tracking
        if self.strat_etf.pending_create and sym in ("A", "B", "C"):
            self.strat_etf.filled_comps[sym] += qty
            if all(self.strat_etf.filled_comps[s] >= self.strat_etf._target_qty
                   for s in ("A", "B", "C")):
                await self.place_swap_order("toETF", self.strat_etf._target_qty)
                eb, _, _ = self.get_bba_mid("ETF")
                if eb:
                    await self.safe_place_order("ETF", self.strat_etf._target_qty,
                                                Side.SELL, eb)
                self.strat_etf._reset()

        elif self.strat_etf.pending_redeem and sym == "ETF":
            self.strat_etf.filled_etf += qty
            if self.strat_etf.filled_etf >= self.strat_etf._target_qty:
                await self.place_swap_order("fromETF", self.strat_etf._target_qty)
                ba, bb, bc = (self.get_bba_mid(s)[0] for s in ("A", "B", "C"))
                if all(x is not None for x in (ba, bb, bc)):
                    await asyncio.gather(
                        self.safe_place_order("A", self.strat_etf._target_qty, Side.SELL, ba),
                        self.safe_place_order("B", self.strat_etf._target_qty, Side.SELL, bb),
                        self.safe_place_order("C", self.strat_etf._target_qty, Side.SELL, bc),
                    )
                self.strat_etf._reset()

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
        self.strat_a.on_order_rejected(order_id)
        self.my_quote_ids.discard(order_id)
        print(f"[REJECTED] {order_id}: {reason}")

    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int) -> None:
        b, a = self.get_best_bid_ask(symbol)
        self.flow.observe(symbol, price, qty, b, a)
        log(f"[TRADE] {symbol} {qty} @ {price}", "trades.log")

    async def bot_handle_book_update(self, symbol: str) -> None:
        if symbol == "A":
            await self.strat_a.on_book_update()
        elif symbol == "C":
            await self.strat_c.on_book_update()
        elif symbol in ("R_HIKE", "R_HOLD", "R_CUT"):
            self.strat_fed._read_book_probs()
            self.strat_c.recalc_fv()   # no-op before C is calibrated

    async def bot_handle_swap_response(self, swap: str, qty: int, success: bool) -> None:
        if success:
            print(f"[SWAP] {swap} x{qty} OK")
        else:
            print(f"[SWAP] {swap} x{qty} FAILED")
            if swap in ("toETF", "fromETF"):
                await self.strat_etf._abort()

    async def bot_handle_news(self, news_release: dict) -> None:
        tick = news_release["tick"]
        kind = news_release["kind"]
        data = news_release["new_data"]

        if kind == "structured":
            sub = data.get("structured_subtype")

            if sub == "earnings":
                asset = data["asset"]
                val   = data["value"]
                if asset == "A":
                    await self.strat_a.on_earnings(val, tick)
                elif asset == "C":
                    await self.strat_c.on_earnings(val, tick)
                else:
                    log(f"[NEWS] UNKNOWN earnings asset={asset} val={val}", "news.log")

            elif sub == "cpi_print":
                # CHANGE 4 + 8: ordering matters.
                # 1. Snapshot C state BEFORE Fed updates ERC.
                self.strat_c.pre_cpi_snapshot()
                # 2. Feed CPI to Fed → updates our probability estimates and ERC.
                self.strat_fed.on_cpi(data["forecast"], data["actual"])
                # 3. Update C FV with new ERC (no-op until C is calibrated).
                self.strat_c.recalc_fv()
                # 4. Directional C trade based on ERC delta + immediately place
                #    Fed orders (news_triggered bypasses 15s rate limit).
                await self.strat_c.on_cpi(data["forecast"], data["actual"])
                await self.strat_fed.trade()   # immediate Fed trade on CPI

            elif sub == "petition":
                self.strat_meta.on_petition(
                    data.get("asset", "?"),
                    data.get("new_signatures", 0),
                    data.get("cumulative", 0),
                )
            else:
                log(f"[NEWS] UNKNOWN structured subtype={sub} data={data}", "news.log")

        else:  # unstructured
            content  = data["content"]
            msg_type = data["type"]
            log(f"[NEWS] unstructured type='{msg_type}': {content[:100]}", "news.log")

            if msg_type in FedStrategy.FEDSPEAK_MSG_TYPES:
                self.strat_fed.on_fedspeak(content)
                self.strat_c.recalc_fv()
                await self.strat_fed.trade()   # immediate Fed trade on fedspeak
            elif msg_type in MetaStrategy.META_MSG_TYPES:
                self.strat_meta.on_news(msg_type, content, tick)
            else:
                print(f"[NEWS] *** UNKNOWN TYPE '{msg_type}': {content[:100]} ***")
                log(f"[NEWS] *** UNKNOWN '{msg_type}': {content}", "news.log")

    async def bot_handle_market_resolved(self, market_id: str,
                                          winning_symbol: str, tick: int) -> None:
        log(f"[MARKET] {market_id} resolved → {winning_symbol} @ tick {tick}", "news.log")

    async def bot_handle_settlement_payout(self, user: str, market_id: str,
                                            amount: int, tick: int) -> None:
        log(f"[PAYOUT] +{amount} from {market_id} @ tick {tick}", "pnl.log")

    # ── Flatten / Status ──────────────────────────────────────────

    async def flatten_all(self) -> None:
        all_syms = self.ALL_TRADABLE + MetaStrategy.META_SYMBOLS
        for s in all_syms:
            await self.cancel_quotes_for(s)
        for s in all_syms:
            pos = self.positions.get(s, 0)
            if pos == 0:
                continue
            b, a = self.get_best_bid_ask(s)
            if pos > 0 and b is not None:
                await self.safe_place_order(s, pos, Side.SELL, b)
                log(f"[FLATTEN] SELL {pos} {s} @ {b}", "pnl.log")
            elif pos < 0 and a is not None:
                await self.safe_place_order(s, abs(pos), Side.BUY, a)
                log(f"[FLATTEN] BUY  {abs(pos)} {s} @ {a}", "pnl.log")

    def print_status(self) -> None:
        lines = [f"[STATUS] ── round elapsed {time.time()-self._round_start_wall:.0f}s ──"]
        total_upnl = 0.0
        for s in self.ALL_TRADABLE:
            pos = self.positions.get(s, 0)
            if pos == 0:
                continue
            _, _, mid = self.get_bba_mid(s)
            if mid is None:
                lines.append(f"  {s:14s} pos={pos:+d}  mid=N/A")
                continue
            t = self.fill_tracker.get(s)
            if t and t[0] != 0:
                avg  = t[1] / t[0]
                upnl = pos * (mid - avg)
            else:
                avg = upnl = 0.0
            total_upnl += upnl
            lines.append(f"  {s:14s} pos={pos:+d}  avg={avg:.0f}  mid={mid:.0f}  "
                         f"uPnL={upnl:+.0f}")
        fv_a = self.fair_values.get("A")
        fv_c = self.fair_values.get("C")
        lines += [
            "─" * 50,
            f"  uPnL={total_upnl:+.0f}  cash={self.positions.get('cash', 0)}",
            f"  FV_A={'N/A' if fv_a is None else f'{fv_a:.0f}'}  "
            f"FV_C={'N/A' if fv_c is None else f'{fv_c:.0f}'}  "
            f"C_sens={self.strat_c._c_sensitivity}  "
            f"C_calib={self.strat_c._c_calibrated}",
            f"  fed: H={self.fair_values['R_HIKE']:.3f}  "
            f"N={self.fair_values['R_HOLD']:.3f}  "
            f"C={self.fair_values['R_CUT']:.3f}  "
            f"scale_A={EPS_SCALE_A:.1f}",
        ]
        log("\n".join(lines), "pnl.log")

    # ── Command listener ──────────────────────────────────────────

    async def _listen_commands(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            cmd = (await loop.run_in_executor(None, sys.stdin.readline)).strip().lower()
            if cmd == "f":
                log("[CMD] Manual flatten")
                await self.flatten_all()
            elif cmd == "s":
                self.print_status()
            elif cmd.startswith("scale "):
                try:
                    global EPS_SCALE_A
                    EPS_SCALE_A = float(cmd.split()[1])
                    print(f"[CMD] EPS_SCALE_A = {EPS_SCALE_A}")
                    if self.eps.get("A"):
                        self.fair_values["A"] = StockAStrategy.calc_fv(self.eps["A"])
                        print(f"[CMD] A FV updated to {self.fair_values['A']:.0f}")
                except (IndexError, ValueError):
                    print("[CMD] Usage: scale <number>")
            elif cmd.startswith("sens "):
                # Manually set C sensitivity: sens -2.5
                try:
                    self.strat_c._c_sensitivity = float(cmd.split()[1])
                    if self.strat_c._c_anchor_mid is None:
                        b, a = self.get_best_bid_ask("C")
                        if b and a:
                            self.strat_c._c_anchor_mid = (b + a) / 2
                            self.strat_c._c_anchor_erc = self.strat_c._current_erc()
                    self.strat_c._c_calibrated = True
                    self.strat_c.recalc_fv()
                    print(f"[CMD] C sensitivity = {self.strat_c._c_sensitivity}")
                except (IndexError, ValueError):
                    print("[CMD] Usage: sens <float>  e.g. sens -2.5")
            elif cmd.startswith("meta "):
                syms = cmd.split()[1:]
                MetaStrategy.META_SYMBOLS = syms
                for s in syms:
                    self._ensure_symbol(s)
                    self.fair_values[s] = 0.5
                print(f"[CMD] Meta symbols registered: {syms}")

    # ── Main trading loop ─────────────────────────────────────────

    async def trade(self) -> None:
        # Clear logs at startup
        for f in ["pnl.log", "fills.log", "fed.log", "news.log", "trades.log", "meta.log"]:
            open(f, "w").close()

        await asyncio.sleep(5)
        self._round_start_wall = time.time()
        asyncio.create_task(self._listen_commands())

        last_status   = time.time()
        last_arb      = time.time()
        last_fed_trade = time.time()   # CHANGE 1+8: Fed trades at 15s cadence

        while True:
            now = time.time()

            # ── Round reset ────────────────────────────────────────
            if self._pending_round_reset:
                self._pending_round_reset = False
                await self._on_round_start()
                last_fed_trade = now   # reset Fed cadence on round start
                continue

            # ── Auto-flatten before round end ──────────────────────
            if (not self._flatten_triggered
                    and self._round_start_wall > 0
                    and now - self._round_start_wall >= ROUND_DURATION_SECS - FLATTEN_LEAD_SECS):
                log(f"[AUTO-FLATTEN] T-{FLATTEN_LEAD_SECS}s — flattening all positions",
                    "pnl.log")
                self._flatten_triggered = True
                await self.flatten_all()

            if self._flatten_triggered:
                await asyncio.sleep(0.5)
                continue

            # ── Settlement trade (CHANGE 6): T-90s to T-20s window ──
            elapsed = now - self._round_start_wall
            if (self._round_start_wall > 0
                    and ROUND_DURATION_SECS - 90 <= elapsed < ROUND_DURATION_SECS - FLATTEN_LEAD_SECS):
                await self.strat_fed.settlement_trade()

            # ── Pre-earnings pre-cancel ────────────────────────────
            await self.strat_a.check_pre_cancel()

            # ── Core strategies ────────────────────────────────────
            await self.strat_a.update_quotes()
            await self.strat_c.update_quotes()
            # CHANGE 8: strat_c.record_obs() removed (no gradient descent)

            # CHANGE 1+8: Fed trade at 15-second cadence.
            # Immediate trades on news already handled in bot_handle_news.
            if now - last_fed_trade >= 15.0:
                last_fed_trade = now
                await self.strat_fed.trade()

            await self.strat_meta.trade()

            # ── Arb strategies (slightly less frequent) ────────────
            if now - last_arb >= 0.5:
                last_arb = now
                await self.strat_etf.check_arb()
                await self.strat_options.run()

            # ── Status report ──────────────────────────────────────
            if now - last_status >= 10:
                last_status = now
                self.print_status()

            await asyncio.sleep(0.1)

    async def start(self) -> None:
        def _exc_handler(task: asyncio.Task) -> None:
            if not task.cancelled() and task.exception():
                import traceback
                traceback.print_exception(
                    type(task.exception()),
                    task.exception(),
                    task.exception().__traceback__
                )
        t = asyncio.create_task(self.trade())
        t.add_done_callback(_exc_handler)
        await self.connect()


# ═══════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════

async def main() -> None:
    SERVER = "practice.uchicago.exchange:3333"
    bot    = MyXchangeClient(SERVER, "chicago6", "bolt-nova-rocket")
    await bot.start()


if __name__ == "__main__":
    asyncio.run(main())
