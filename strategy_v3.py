"""
UChicago Trading Competition 2026 — Case 1: Market Making
Full strategy implementation.

PnL hierarchy (highest to lowest reliability):
  1. Stock A earnings sweep        — deterministic FV, 20 events/round
  2. Options box spread            — risk-free arbitrage
  3. Options PCP arbitrage         — near risk-free, 3 strikes
  4. ETF NAV arbitrage             — low risk
  5. Stock A passive MM            — reliable with good FV
  6. Fed prediction market         — Kelly-sized, model-dependent
  7. Stock C passive MM            — reliable once BETA/GAMMA calibrated
  8. Meta market                   — fill in on competition day

Key edges over a naive implementation:
  - Pre-earnings timing: cancel A/C quotes 3s before known earnings time
  - Unconditional sweep: grab ALL mispriced book orders post-earnings
  - B implied price: triangulate B's fair value from PCP across 3 strikes
  - Kelly fed sizing: 5-6x improvement over fixed qty=1
  - Smart money flow: adjust FV from observed aggressive trade prints
  - ETF one-sided trades: directional ETF vs NAV, lower execution risk
  - Round reset detection: per-round state reset via position_snapshot
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
_BOT_LOG = "bot.log"

def log(msg: str, file: Optional[str] = None) -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    # Always write to bot.log so the monitor can see everything
    if _BOT_LOG not in _log_handles:
        _log_handles[_BOT_LOG] = open(_BOT_LOG, "a", buffering=1)
    _log_handles[_BOT_LOG].write(line + "\n")
    # Also write to specific file if one was requested
    if file and file != _BOT_LOG:
        if file not in _log_handles:
            _log_handles[file] = open(file, "a", buffering=1)
        _log_handles[file].write(line + "\n")


# ═══════════════════════════════════════════════════════════════════════
# Model Constants  (calibrate on practice round)
# ═══════════════════════════════════════════════════════════════════════

# Stock A
PE_A         = 10       # constant P/E for A
EPS_SCALE_A  = 1        # 1 if EPS is in same unit as price; 100 if EPS in dollars, price in cents
                        # VERIFY on practice round: print eps * PE_A vs book mid at earnings

# Stock C
PE0_C        = 14.0
Y0           = 0.045
B0_OVER_N    = 40       # bond portfolio value / outstanding shares
D            = 7.5      # bond duration
CONV         = 55.0     # bond convexity
LAMBDA       = 0.65     # bond portfolio weighting constant
EPS0_C       = 2.00     # prior EPS for C before first earnings

# Initial guesses for the two unknowns we fit online
BETA_INIT    = 0.004    # yield sensitivity to rate expectations
GAMMA_INIT   = 15.0     # PE sensitivity to yield changes

# ETF
ETF_SWAP_COST = 5       # cost per unit to swap

# Timing  (case packet: earnings at seconds 22 and 88 of each 90s day)
TICKS_PER_SEC  = 5
SECS_PER_DAY   = 90
TICKS_PER_DAY  = TICKS_PER_SEC * SECS_PER_DAY   # 450
EARNINGS_SECS  = (22, 88)
EARNINGS_TICKS = tuple(s * TICKS_PER_SEC for s in EARNINGS_SECS)  # (110, 440)

# Round settings
ROUND_DURATION_SECS = 15 * 60   # 15 minutes
FLATTEN_LEAD_SECS   = 20        # start flattening this many seconds before round end


# ═══════════════════════════════════════════════════════════════════════
# Utility: FlowTracker
# ═══════════════════════════════════════════════════════════════════════

class FlowTracker:
    """
    Tracks recent signed trade volume per symbol.

    When we observe a trade at price P:
      - P >= best_ask at that moment  →  aggressive buy  → +qty
      - P <= best_bid at that moment  →  aggressive sell → -qty
      - In between                    →  ambiguous       → skip

    Maintains a rolling window.  The resulting flow_signal is used as a
    small adjustment to our FV: if there's persistent buy pressure,
    smart money may know something, so we shade our FV up slightly.
    """

    WINDOW     = 60     # number of recent trades to remember
    # ALPHA: how much 1 unit of net signed volume adjusts FV.
    # DISABLED (0.0) until we confirm EPS_SCALE_A and the A model is correct.
    # With a wrong FV, the flow adjustment creates a death spiral:
    #   wrong FV → sell A → flow sees sell pressure → fv_adj drops → sell more.
    # Re-enable and tune once A FV is calibrated.
    ALPHA      = 0.0

    def __init__(self):
        self._trades: dict[str, deque] = {}   # symbol → deque of signed qty

    def observe(self, symbol: str, price: int, qty: int, best_bid: Optional[int], best_ask: Optional[int]) -> None:
        if symbol not in self._trades:
            self._trades[symbol] = deque(maxlen=self.WINDOW)
        if best_ask is not None and price >= best_ask:
            self._trades[symbol].append(qty)
        elif best_bid is not None and price <= best_bid:
            self._trades[symbol].append(-qty)
        # else ambiguous — skip

    def net_signed_volume(self, symbol: str) -> float:
        trades = self._trades.get(symbol)
        if not trades:
            return 0.0
        return float(sum(trades))

    def fv_adjustment(self, symbol: str) -> float:
        """Returns a small +/- adjustment to add to the FV estimate."""
        return self.ALPHA * self.net_signed_volume(symbol)


# ═══════════════════════════════════════════════════════════════════════
# Utility: EarningsTimer
# ═══════════════════════════════════════════════════════════════════════

class EarningsTimer:
    """
    Predicts wall-clock time of the NEXT A/C earnings event.

    The case spec guarantees earnings fires at seconds 22 and 88 of each
    90-second day.  When we receive an earnings event with absolute tick T:

      tick_in_day = T % 450

      if tick_in_day ≈ 110  (second 22):
          next earnings in (88-22) = 66 seconds
      if tick_in_day ≈ 440  (second 88):
          next earnings in (90 - 88 + 22) = 24 seconds  (wraps to next day)

    We subtract PRE_CANCEL_SECS to get the cancel-quotes deadline.
    """

    PRE_CANCEL_SECS = 3.0   # cancel quotes this many seconds BEFORE earnings

    def __init__(self):
        self._last_earnings_wall: float = 0.0
        self._next_cancel_wall: float   = 0.0
        self._next_earnings_wall: float = 0.0
        self._armed: bool               = False

    def record_earnings(self, tick: int) -> None:
        """Call every time an earnings event fires."""
        now = time.time()
        self._last_earnings_wall = now
        tick_in_day = tick % TICKS_PER_DAY

        # Determine which earnings just fired and compute gap to next
        gaps = []
        for et in EARNINGS_TICKS:
            raw = et - tick_in_day
            # raw can be ≤ 0 if this IS that earnings tick (we're at it now)
            # Normalize to a small positive value (time until the NEXT occurrence)
            gap_ticks = raw % TICKS_PER_DAY
            if gap_ticks < TICKS_PER_SEC:   # less than 1 second — that's this event
                gap_ticks += TICKS_PER_DAY  # skip to the one after next
            gaps.append(gap_ticks)

        gap_to_next = min(gaps) / TICKS_PER_SEC   # in seconds
        self._next_earnings_wall = now + gap_to_next
        self._next_cancel_wall   = self._next_earnings_wall - self.PRE_CANCEL_SECS
        self._armed              = True
        print(f"[TIMER] Next earnings in {gap_to_next:.1f}s  "
              f"(cancel at T-{self.PRE_CANCEL_SECS}s)")

    def should_cancel_now(self) -> bool:
        """Returns True once, in the window PRE_CANCEL_SECS before next earnings."""
        if not self._armed:
            return False
        now = time.time()
        if now >= self._next_cancel_wall:
            self._armed = False   # disarm until next earnings fires and re-arms
            return True
        return False

    def secs_to_next_earnings(self) -> float:
        if not self._armed:
            return float("inf")
        return max(0.0, self._next_earnings_wall - time.time())


# ═══════════════════════════════════════════════════════════════════════
# Utility: BPriceTracker
# ═══════════════════════════════════════════════════════════════════════

class BPriceTracker:
    """
    Infers B's fair value from put-call parity across all three strikes.

    With r = 0:  C - P = S - K  →  S_implied = C_mid - P_mid + K

    We collect all three estimates and return a weighted average.
    More liquid strikes (tighter spread) get higher weight.
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
            c_spread = c_ask - c_bid
            p_spread = p_ask - p_bid

            # Weight: inverse of total spread (tighter = more reliable)
            total_spread = c_spread + p_spread
            if total_spread <= 0:
                continue
            w = 1.0 / total_spread

            estimates.append(s_imp)
            weights.append(w)

        if not estimates:
            return None

        total_w = sum(weights)
        return sum(s * w for s, w in zip(estimates, weights)) / total_w


# ═══════════════════════════════════════════════════════════════════════
# Strategy: Stock A
# ═══════════════════════════════════════════════════════════════════════

class StockAStrategy:
    """
    Primary edge: earnings arbitrage + passive market making.

    Earnings fires at seconds 22 and 88 (known timing).
    After each earnings:
      1. FV updates to EPS * PE_A * EPS_SCALE_A
      2. Sweep ALL mispriced orders unconditionally up to position limit
      3. Immediately begin quoting tightly around new FV

    Pre-cancellation: quotes cancelled 3s before expected earnings to
    avoid being picked off by faster bots in later rounds.

    Passive MM: inventory-skewed bid/ask around FV, spread widens with
    position size and with flow imbalance (adverse selection protection).
    """

    MAX_ORDER_SIZE      = 40
    MAX_OUTSTANDING_VOL = 120
    MAX_OPEN_ORDERS     = 50
    MAX_ABS_POS         = 200

    BASE_SPREAD         = 4
    SKEW_PER_UNIT       = 0.15
    QUOTE_SIZE          = 10
    SOFT_POS_LIMIT      = 100    # beyond this, reduce quote size and widen spread

    EDGE_THRESHOLD      = 2      # min edge to snipe outside post-earnings
    SNIPE_SIZE          = 30     # per-order snipe size post-earnings
    POST_EARNINGS_SECS  = 2.0    # aggressive mode duration after earnings (wall clock)

    def __init__(self, client: "MyXchangeClient"):
        self.client          = client
        self.timer           = EarningsTimer()

        self._pending_cancels: set[str]   = set()
        self._last_snipe_time: float      = 0.0
        self._last_quote_time: float      = 0.0
        self._earnings_end_time: float    = 0.0   # wall clock: when aggressive mode ends
        self._last_bid: Optional[tuple]   = None
        self._last_ask: Optional[tuple]   = None
        self._pre_cancel_done: bool       = False  # True if we already fired pre-cancel this cycle

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

    # ── Post-earnings sweep (UNCONDITIONAL) ───────────────────────
    # Key improvement: the old code only swept in the direction that
    # reduced our position.  This leaves money on the table.  If FV
    # moves from 200 → 250, ANY ask below 248 is profitable to buy
    # regardless of our current position.  We sweep to the position
    # limit and then make markets at the new price.

    def _snipe_qty(self, book_qty: int, side: Side) -> int:
        """Compute the right snipe size given actual book depth and our limit headroom.

        Clamping to book_qty prevents resting leftover units from eating outstanding vol.
        Clamping to vol headroom prevents hitting the 120-unit limit mid-sweep.
        Clamping to pos room prevents hitting MAX_ABS_POS and triggering rejections.
        """
        pos      = self.client.positions["A"]
        vol_room = max(0, self.MAX_OUTSTANDING_VOL - self._outstanding_vol())
        if side == Side.BUY:
            pos_room = max(0, self.MAX_ABS_POS - pos)
        else:
            pos_room = max(0, self.MAX_ABS_POS + pos)
        return min(book_qty, self.SNIPE_SIZE, vol_room, pos_room)

    async def _sweep_book(self) -> None:
        fv = self.client.fair_values.get("A")
        if fv is None:
            return
        book = self.client.order_books["A"]
        edge = 2

        # Buy side: all asks below FV - edge  →  unconditional up to pos limit
        stale_asks = sorted(
            [(px, qty) for px, qty in book.asks.items() if qty > 0 and px < fv - edge],
            key=lambda x: x[0]
        )
        for px, qty in stale_asks:
            buy_qty = self._snipe_qty(qty, Side.BUY)
            if buy_qty <= 0:
                break
            oid = await self._place(buy_qty, Side.BUY, px)
            if oid is None:
                break
            log(f"[A] SWEEP BUY  {buy_qty} @ {px} | edge={fv - px:.0f}")

        # Sell side: all bids above FV + edge → unconditional down to -pos limit
        stale_bids = sorted(
            [(px, qty) for px, qty in book.bids.items() if qty > 0 and px > fv + edge],
            key=lambda x: -x[0]
        )
        for px, qty in stale_bids:
            sell_qty = self._snipe_qty(qty, Side.SELL)
            if sell_qty <= 0:
                break
            oid = await self._place(sell_qty, Side.SELL, px)
            if oid is None:
                break
            log(f"[A] SWEEP SELL {sell_qty} @ {px} | edge={px - fv:.0f}")

    # ── Opportunistic snipe (outside post-earnings window) ────────

    async def _snipe(self) -> None:
        fv = self.client.fair_values.get("A")
        if fv is None:
            return
        now = asyncio.get_running_loop().time()
        if now - self._last_snipe_time < 0.3:
            return
        self._last_snipe_time = now

        best_bid, best_ask = self.client.get_best_bid_ask("A")
        # Include flow adjustment (smart money signal)
        flow_adj = self.client.flow.fv_adjustment("A")
        fv_adj   = fv + flow_adj

        if best_ask is not None and best_ask < fv_adj - self.EDGE_THRESHOLD:
            book_qty = self.client.order_books["A"].asks.get(best_ask, 0)
            qty = self._snipe_qty(book_qty if book_qty > 0 else self.SNIPE_SIZE, Side.BUY)
            if qty > 0:
                oid = await self._place(qty, Side.BUY, best_ask)
                if oid:
                    log(f"[A] SNIPE BUY  {qty} @ {best_ask} | fv={fv:.0f} flow={flow_adj:+.0f} fv_adj={fv_adj:.0f}")
        elif best_bid is not None and best_bid > fv_adj + self.EDGE_THRESHOLD:
            book_qty = self.client.order_books["A"].bids.get(best_bid, 0)
            qty = self._snipe_qty(book_qty if book_qty > 0 else self.SNIPE_SIZE, Side.SELL)
            if qty > 0:
                oid = await self._place(qty, Side.SELL, best_bid)
                if oid:
                    log(f"[A] SNIPE SELL {qty} @ {best_bid} | fv={fv:.0f} flow={flow_adj:+.0f} fv_adj={fv_adj:.0f}")

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

        # Apply flow adjustment to FV
        fv_adj = fv + self.client.flow.fv_adjustment("A")

        # Still in aggressive window — don't re-quote yet, keep sweeping
        if time.time() < self._earnings_end_time:
            if now - self._last_snipe_time >= 0.2:
                self._last_snipe_time = now
                await self._sweep_book()
            return

        # Rate-limit quotes to avoid cancelling/replacing every 100ms
        if now - self._last_quote_time < 1.0:
            await self._snipe()
            return
        self._last_quote_time = now

        pos            = self.client.positions["A"]
        best_bid, best_ask = self.client.get_best_bid_ask("A")

        # Dynamic spread: widen with inventory size
        pos_frac = abs(pos) / max(self.SOFT_POS_LIMIT, 1)
        spread   = self.BASE_SPREAD + int(pos_frac * 4)

        # Inventory skew: shift quotes toward reducing inventory
        skew = self.SKEW_PER_UNIT * pos

        # Also shade for flow: if strong buy flow, we prefer to sell so raise bid/ask
        flow_skew = self.client.flow.fv_adjustment("A") * 0.5
        total_skew = skew + flow_skew

        bid = round(fv_adj - spread / 2 - total_skew)
        ask = round(fv_adj + spread / 2 - total_skew)
        if bid >= ask:
            ask = bid + 1

        # Don't quote if it would immediately cross the book (applied at ALL position sizes).
        # At ±MAX_ABS_POS with FV far from market, FV-based quotes are on the wrong side
        # of the book and would get zeroed here — which is correct. The bot sits flat,
        # correctly positioned, and earns via snipes. Forcing a market-cross quote at
        # position limits creates a treadmill: snipe sells at 850, MM buys at 860 = -10/share.
        if best_ask is not None and bid >= best_ask:
            bid = 0
        if best_bid is not None and ask <= best_bid:
            ask = 0

        # Quote size: reduce when inventory-constrained
        bid_size = self.QUOTE_SIZE if pos < self.SOFT_POS_LIMIT  else max(2, self.QUOTE_SIZE // 3)
        ask_size = self.QUOTE_SIZE if pos > -self.SOFT_POS_LIMIT else max(2, self.QUOTE_SIZE // 3)

        # Skip cancel+replace if nothing changed
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
        old_fv = self.client.fair_values.get("A")
        new_fv = self.calc_fv(eps)
        self.client.eps["A"]         = eps
        self.client.fair_values["A"] = new_fv

        # ── CALIBRATION INFO ─────────────────────────────────────────────
        # If new_fv is far from market mid, EPS_SCALE_A is wrong.
        # Check market mid vs new_fv, then type: scale <correct_scale>
        # where correct_scale = market_mid / (eps * PE_A)
        b, a = self.client.get_best_bid_ask("A")
        mid = (b + a) / 2 if (b and a) else None
        implied_scale = (mid / (eps * PE_A)) if mid else None
        # ─────────────────────────────────────────────────────────────────

        delta = new_fv - old_fv if old_fv is not None else 0
        log(f"[A] *** EARNINGS *** raw_eps={eps} PE_A={PE_A} scale={EPS_SCALE_A} "
            f"FV={old_fv}→{new_fv:.0f} Δ={delta:+.1f} "
            f"market_mid={'N/A' if mid is None else f'{mid:.0f}'} "
            f"implied_scale={'N/A' if implied_scale is None else f'{implied_scale:.2f}'} "
            f"pos={self.client.positions['A']}")

        # Arm timer for next expected earnings
        self.timer.record_earnings(tick)
        self._pre_cancel_done = False

        # Enter aggressive mode
        self._earnings_end_time = time.time() + self.POST_EARNINGS_SECS
        self._last_bid = None
        self._last_ask = None

        # Cancel all stale quotes immediately
        await self._cancel_all_a()

        # Unconditional sweep
        await self._sweep_book()

    # ── Pre-earnings check (call from main loop) ──────────────────

    async def check_pre_cancel(self) -> None:
        """Cancel quotes if we're within PRE_CANCEL_SECS of expected earnings."""
        if not self._pre_cancel_done and self.timer.should_cancel_now():
            self._pre_cancel_done = True
            log(f"[A] PRE-CANCEL: {self.timer.PRE_CANCEL_SECS}s before expected earnings")
            await self._cancel_all_a()
            self._last_bid = None
            self._last_ask = None

    # ── Book update ───────────────────────────────────────────────

    async def on_book_update(self) -> None:
        now = asyncio.get_running_loop().time()
        if time.time() < self._earnings_end_time:
            # Rate-limit to 5/sec during earnings — book updates can fire 30-50x/sec
            # and each _sweep_book call attempts up to SNIPE_SIZE * book_levels orders.
            # Without this, outstanding vol fills in the first few calls and the rest reject.
            if now - self._last_snipe_time < 0.2:
                return
            self._last_snipe_time = now
            await self._sweep_book()
        else:
            await self._snipe()

    # ── Round reset ───────────────────────────────────────────────

    def reset(self) -> None:
        self._pending_cancels.clear()
        self._last_snipe_time  = 0.0
        self._last_quote_time  = 0.0
        self._earnings_end_time = 0.0
        self._last_bid = None
        self._last_ask = None
        self._pre_cancel_done  = False
        # Don't reset timer — if we carry over mid-round it helps
        # Don't reset FV — initial MM will use market mid until first earnings


# ═══════════════════════════════════════════════════════════════════════
# Strategy: Stock C
# ═══════════════════════════════════════════════════════════════════════

class StockCStrategy:
    """
    C is an insurance company whose price depends on EPS and bond portfolio value.
    Both are sensitive to the Federal Reserve prediction market.

    Pt = EPSt * PE0 * exp(-γ * Δy) + λ * B0/N * (-D*Δy + ½C*(Δy)²) + noise

    where Δy = β * E[Δr]  and  E[Δr] = 25*P(hike) - 25*P(cut)

    BETA (β) and GAMMA (γ) are fit online via gradient descent.
    We start trading immediately using the prior values — don't wait
    for convergence.  Better BETA/GAMMA just tighten our FV.
    """

    MIN_OBS       = 10
    FIT_EVERY     = 20
    STABILITY_WIN = 3
    N_ITER        = 150
    LR_BETA       = 1e-9
    LR_GAMMA      = 1e-6

    def __init__(self, client: "MyXchangeClient"):
        self.client       = client
        self.beta         = BETA_INIT
        self.gamma        = GAMMA_INIT
        self._committed   = False
        self._obs: list   = []        # (erc, c_mid, eps)
        self._obs_since_fit: int = 0
        self._recent_fits: list  = []  # [(beta, gamma), ...]
        self._last_quote_time: float = 0.0
        # Rate limit for on_book_update snipes — prevents runaway spam
        # when the C book has many updates per second
        self._last_snipe_time: float = 0.0
        self._SNIPE_RATE_SECS  = 0.3   # max 3 snipes/sec from book updates
        self._SNIPE_MAX_PENDING = 3    # stop if this many C orders already resting
                                       # (reduced from 6 to free order slots for options)
        self._c_bid_id: Optional[str] = None
        self._c_ask_id: Optional[str] = None
        self._last_bid: Optional[tuple] = None
        self._last_ask: Optional[tuple] = None

    def calc_fv(self, eps: float) -> float:
        p_hike = self.client.fair_values["R_HIKE"]
        p_cut  = self.client.fair_values["R_CUT"]
        erc    = 25.0 * p_hike - 25.0 * p_cut
        dy     = self.beta * erc
        pe     = PE0_C * math.exp(-self.gamma * dy)
        db     = B0_OVER_N * (-D * dy + 0.5 * CONV * dy ** 2)
        return eps * pe + LAMBDA * db

    def recalc_fv(self) -> None:
        eps = self.client.eps.get("C") or EPS0_C
        fv  = self.calc_fv(eps)
        old = self.client.fair_values.get("C")
        self.client.fair_values["C"] = fv
        if old is None or abs(fv - old) > 1.0:
            log(f"[C] FV recalc: {old}→{fv:.1f} (β={self.beta:.5f} γ={self.gamma:.2f})")

    async def record_obs(self) -> None:
        if self._committed:
            return
        eps = self.client.eps.get("C") or EPS0_C
        c_bid, c_ask = self.client.get_best_bid_ask("C")
        if c_bid is None or c_ask is None:
            return
        erc = 25.0 * self.client.fair_values["R_HIKE"] - 25.0 * self.client.fair_values["R_CUT"]
        self._obs.append((erc, (c_bid + c_ask) / 2, eps))
        self._obs_since_fit += 1
        if len(self._obs) >= self.MIN_OBS and self._obs_since_fit >= self.FIT_EVERY:
            self._obs_since_fit = 0
            self._fit()

    def _predict(self, beta: float, gamma: float, erc: float, eps: float) -> float:
        dy = beta * erc
        pe = PE0_C * math.exp(-gamma * dy)
        db = LAMBDA * B0_OVER_N * (-D * dy + 0.5 * CONV * dy ** 2)
        return eps * pe + db

    def _fit(self) -> None:
        data  = self._obs[-300:]
        beta  = self.beta
        gamma = self.gamma

        for _ in range(self.N_ITER):
            gb = gg = 0.0
            for (erc, c_mid, eps) in data:
                dy  = beta * erc
                pred = self._predict(beta, gamma, erc, eps)
                err  = pred - c_mid

                d_dy_d_beta  = erc
                d_pred_d_dy  = (eps * PE0_C * math.exp(-gamma * dy) * (-gamma)
                                + LAMBDA * B0_OVER_N * (-D + CONV * dy))
                d_pred_d_gamma = eps * PE0_C * math.exp(-gamma * dy) * (-dy)

                gb += err * d_pred_d_dy * d_dy_d_beta
                gg += err * d_pred_d_gamma

            n     = len(data)
            beta  = max(0.00005, min(0.05,  beta  - self.LR_BETA  * (2 / n) * gb))
            gamma = max(0.1,     min(100.0, gamma - self.LR_GAMMA * (2 / n) * gg))

        log(f"[C] fit: β={beta:.5f} γ={gamma:.2f}")

        # Sanity-check: predicted FV should be within 25% of market mid
        eps   = self.client.eps.get("C") or EPS0_C
        erc   = 25.0 * self.client.fair_values["R_HIKE"] - 25.0 * self.client.fair_values["R_CUT"]
        pred  = self._predict(beta, gamma, erc, eps)
        c_bid, c_ask = self.client.get_best_bid_ask("C")
        if c_bid and c_ask:
            mid = (c_bid + c_ask) / 2
            if mid > 0 and abs(pred - mid) / mid > 0.25:
                log(f"[C] fit REJECTED (pred={pred:.1f} vs mid={mid:.1f} — {abs(pred-mid)/mid:.0%} off)")
                self._recent_fits.clear()
                return

        self.beta  = beta
        self.gamma = gamma
        self._recent_fits.append((beta, gamma))
        if len(self._recent_fits) > self.STABILITY_WIN:
            self._recent_fits.pop(0)

        if len(self._recent_fits) >= self.STABILITY_WIN:
            betas  = [e[0] for e in self._recent_fits]
            gammas = [e[1] for e in self._recent_fits]
            if max(betas) - min(betas) < 0.0005 and max(gammas) - min(gammas) < 1.0:
                self._committed = True
                log(f"[C] *** COMMITTED β={self.beta:.5f} γ={self.gamma:.2f} ***")

    async def on_book_update(self) -> None:
        fv = self.client.fair_values.get("C")
        if fv is None:
            return

        # ── Rate limit: prevent runaway spam on high-frequency book updates ──
        now = asyncio.get_running_loop().time()
        if now - self._last_snipe_time < self._SNIPE_RATE_SECS:
            return

        # ── Budget: don't pile on if we already have many C orders resting ──
        c_pending = sum(
            1 for info in self.client.open_orders.values()
            if info and info[0].symbol == "C" and not info[2]
        )
        if c_pending >= self._SNIPE_MAX_PENDING:
            return

        self._last_snipe_time = now

        b, a = self.client.get_best_bid_ask("C")
        edge = 3
        if a is not None and a < fv - edge:
            await self.client.safe_place_order("C", 10, Side.BUY, a)
            log(f"[C] SNIPE BUY  10 @ {a} | fv={fv:.1f} pending={c_pending+1}")
        elif b is not None and b > fv + edge:
            await self.client.safe_place_order("C", 10, Side.SELL, b)
            log(f"[C] SNIPE SELL 10 @ {b} | fv={fv:.1f} pending={c_pending+1}")

    async def update_quotes(self) -> None:
        fv = self.client.fair_values.get("C")
        if fv is None:
            return
        now = asyncio.get_running_loop().time()
        if now - self._last_quote_time < 1.5:   # rate-limit C re-quotes
            return
        self._last_quote_time = now

        pos   = self.client.positions.get("C", 0)
        b, a  = self.client.get_best_bid_ask("C")
        max_p = 100
        spread = 5 + int(abs(pos) / max_p * 4)   # wider than A since C has noise term
        skew   = 0.15 * pos

        bid = round(fv - spread / 2 - skew)
        ask = round(fv + spread / 2 - skew)
        if bid >= ask:
            ask = bid + 1

        if a is not None and bid >= a:
            bid = 0
        if b is not None and ask <= b:
            ask = 0

        qs = 8
        bid_sz = qs if pos < max_p  else max(2, qs // 3)
        ask_sz = qs if pos > -max_p else max(2, qs // 3)

        # Skip if quotes haven't changed
        if (bid, bid_sz) == self._last_bid and (ask, ask_sz) == self._last_ask:
            return
        self._last_bid = (bid, bid_sz)
        self._last_ask = (ask, ask_sz)

        # Only cancel our own tracked MM quotes, not all C orders (snipes stay)
        for oid in (self._c_bid_id, self._c_ask_id):
            if oid and oid in self.client.open_orders:
                await self.client.cancel_order(oid)
        self._c_bid_id = self._c_ask_id = None

        if bid > 0:
            self._c_bid_id = await self.client.safe_place_order("C", bid_sz, Side.BUY, bid)
        if ask > 0:
            self._c_ask_id = await self.client.safe_place_order("C", ask_sz, Side.SELL, ask)

    async def on_earnings(self, eps: float, tick: int) -> None:
        self.client.eps["C"] = eps
        self.recalc_fv()
        fv = self.client.fair_values.get("C")

        # ── CALIBRATION INFO for C ────────────────────────────────────────
        b, a = self.client.get_best_bid_ask("C")
        mid = (b + a) / 2 if (b and a) else None
        if mid and fv:
            pct_off = (fv - mid) / mid * 100
            log(f"[C] *** EARNINGS *** raw_eps={eps} FV={fv:.1f} market_mid={mid:.0f} "
                f"gap={fv-mid:+.1f} ({pct_off:+.1f}%) β={self.beta:.5f} γ={self.gamma:.2f}")
            if abs(pct_off) > 15:
                print(f"[C] WARNING: FV is {pct_off:+.1f}% off market. "
                      f"Type 'beta X' or 'gamma Y' to manually adjust.")
        # ─────────────────────────────────────────────────────────────────

        if fv is None:
            return
        self._last_quote_time = 0.0  # force immediate re-quote
        self._last_bid = None
        self._last_ask = None
        for oid in (self._c_bid_id, self._c_ask_id):
            if oid and oid in self.client.open_orders:
                await self.client.cancel_order(oid)
        self._c_bid_id = self._c_ask_id = None
        book  = self.client.order_books["C"]
        edge  = 2

        for px, qty in sorted(book.asks.items()):
            if qty <= 0 or px >= fv - edge:
                break
            await self.client.safe_place_order("C", min(qty, 15), Side.BUY, px)
            log(f"[C] SWEEP BUY  {min(qty,15)} @ {px} | fv={fv:.1f}")

        for px, qty in sorted(book.bids.items(), reverse=True):
            if qty <= 0 or px <= fv + edge:
                break
            await self.client.safe_place_order("C", min(qty, 15), Side.SELL, px)
            log(f"[C] SWEEP SELL {min(qty,15)} @ {px} | fv={fv:.1f}")

    def reset(self) -> None:
        """Call at round start.  Preserve model params — they carry over."""
        self._last_quote_time = 0.0
        self._last_snipe_time = 0.0
        # Do NOT reset beta, gamma, committed, recent_fits — model knowledge persists
        self._last_bid = None
        self._last_ask = None
        self._c_bid_id = None
        self._c_ask_id = None


# ═══════════════════════════════════════════════════════════════════════
# Strategy: Options (PCP + Box Spread)
# ═══════════════════════════════════════════════════════════════════════

class OptionsStrategy:
    """
    Two risk-free (or near risk-free) arbitrage strategies:

    1. Put-call parity  (3 legs: call, put, B stock)
       C - P = S - K  (r=0)
       Edge if:  P + S_bid - C_ask > K  →  buy call, sell put, sell B
       Edge if:  C_bid - P_ask - S_ask + K > 0  →  sell call, buy put, buy B

       We use B's IMPLIED price (from BPriceTracker) for a more accurate
       check alongside the observed B quotes.

    2. Box spread  (4 legs: C1, C2, P1, P2 at two strikes)
       Value always = K2 - K1 at expiry (r=0)
       Buy box if cost < K2-K1; sell box if receipt > K2-K1

    Sizing: arb_qty scales with available edge to take more when safe.
    Cooldown prevents refiring the same arb every tick when legs are filling.
    """

    BOX_PAIRS  = [(950, 1000), (1000, 1050), (950, 1050)]
    MIN_EDGE   = 2
    COOLDOWN   = 4.0

    def __init__(self, client: "MyXchangeClient"):
        self.client      = client
        self.b_tracker   = BPriceTracker(client)
        self._last: dict = {}   # keyed by arb_id → last fire time

    def _bba(self, sym: str):
        return self.client.get_best_bid_ask(sym)

    def _cold(self, key: str) -> bool:
        now = asyncio.get_running_loop().time()
        if now - self._last.get(key, 0) < self.COOLDOWN:
            return False
        self._last[key] = now
        return True

    def _arb_qty(self, edge: float) -> int:
        """Scale order size with edge: larger edge → more units."""
        if edge < self.MIN_EDGE:
            return 0
        if edge < 5:
            return 1
        if edge < 10:
            return 3
        return 5

    async def _pcp_arb(self, strike: int) -> Optional[str]:
        c_bid, c_ask = self._bba(f"B_C_{strike}")
        p_bid, p_ask = self._bba(f"B_P_{strike}")
        s_bid, s_ask = self._bba("B")

        if None in (c_bid, c_ask, p_bid, p_ask):
            return "opts-no-book"
        if None in (s_bid, s_ask):
            return "B-no-book"

        s_impl = self.b_tracker.get_implied_price()

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
            return None

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
            return None

        best_edge = max(edge1, edge2)
        return f"no-edge({best_edge:.0f})"

    async def _box_arb(self, k1: int, k2: int) -> Optional[str]:
        c1b, c1a = self._bba(f"B_C_{k1}")
        c2b, c2a = self._bba(f"B_C_{k2}")
        p1b, p1a = self._bba(f"B_P_{k1}")
        p2b, p2a = self._bba(f"B_P_{k2}")
        if None in (c1b, c1a, c2b, c2a, p1b, p1a, p2b, p2a):
            return "no-book"

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
            return None

        sell_rcpt = c1b - c2a + p2b - p1a
        sell_edge = sell_rcpt - theo
        qty_s     = self._arb_qty(sell_edge)
        if qty_s > 0 and self._cold(f"box_sell_{k1}_{k2}"):
            log(f"[OPT] BOX SELL ({k1},{k2}) edge={sell_edge:.0f} qty={qty_s}")
            await self.client.safe_place_order(f"B_C_{k1}", qty_s, Side.SELL, c1b)
            await self.client.safe_place_order(f"B_C_{k2}", qty_s, Side.BUY,  c2a)
            await self.client.safe_place_order(f"B_P_{k1}", qty_s, Side.BUY,  p1a)
            await self.client.safe_place_order(f"B_P_{k2}", qty_s, Side.SELL, p2b)
            return None

        best_edge = max(buy_edge, sell_edge)
        return f"no-edge({best_edge:.0f})"

    async def run(self) -> None:
        now = asyncio.get_running_loop().time()
        if now - self._last.get("_run", 0) < 1.5:
            return
        self._last["_run"] = now

        # KEY FIX: do NOT cancel resting arb orders before re-checking.
        # Resting box/PCP orders are profitable when they fill — we want them
        # in the book.  Cancelling and immediately re-placing creates a race
        # where the exchange still counts the (being-cancelled) orders against
        # the outstanding volume limit, blocking the new placements.
        #
        # We only skip placing a new arb if we already have enough pending
        # orders for that leg (checked inside _pcp_arb / _box_arb via _cold).

        skipped = []
        for k in (950, 1000, 1050):
            reason = await self._pcp_arb(k)
            if reason:
                skipped.append(f"PCP-{k}:{reason}")
        for k1, k2 in self.BOX_PAIRS:
            reason = await self._box_arb(k1, k2)
            if reason:
                skipped.append(f"BOX-{k1}/{k2}:{reason}")

        if skipped:
            log(f"[OPT] skipped — {', '.join(skipped)}")

    def reset(self) -> None:
        self._last.clear()


# ═══════════════════════════════════════════════════════════════════════
# Strategy: Federal Reserve Prediction Market
# ═══════════════════════════════════════════════════════════════════════

class FedStrategy:
    """
    Trade R_HIKE, R_HOLD, R_CUT based on our probability estimates.

    Signal sources:
      - Structured news: CPI print (actual vs forecast)
      - Unstructured news: FEDSPEAK keyword parsing
      - Book mid: market consensus (updated continuously)

    Sizing: half-Kelly proportional to divergence from market.
    This replaces the old fixed qty=1 which left significant edge untouched.

    Half-Kelly for prediction markets:
        edge = our_p - market_p
        kelly_fraction = edge / (1 - market_p)   [for buy leg]
        size = int(0.5 * kelly_fraction * max_pos)
    """

    # Update these after seeing the practice round's news message types
    FEDSPEAK_MSG_TYPES: set[str] = {"FEDSPEAK", "fedspeak", "FED_SPEAK"}

    DOVISH  = ["easing", "cooling", "dovish", "cut", "lower rates", "easing inflation",
               "slowing growth", "policy easing", "expectations of policy easing"]
    HAWKISH = ["restrictive", "hawkish", "hike", "inflation risks", "stay restrictive",
               "tightening", "higher for longer", "restrictive for longer"]
    NEUTRAL = ["wide range of views", "weighing", "uncertainties", "not in a hurry",
               "either direction", "balanced", "mixed", "no clear", "options open"]

    CONTRACTS  = ("R_HIKE", "R_HOLD", "R_CUT")
    MAX_POS    = 25
    EDGE_MIN   = 0.04   # minimum divergence to trade
    CPI_SENS   = 80     # probability shift per unit CPI surprise
    SPEAK_SHIFT= 0.03   # shift per fedspeak headline

    def __init__(self, client: "MyXchangeClient"):
        self.client      = client
        self._book_reads = 0
        self._mkt_probs  = {c: None for c in self.CONTRACTS}

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
                # Seed our fair values from market during warmup
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
            # Inline with expectations → shift toward hold
            self.client.fair_values["R_HIKE"] = max(0.04, self.client.fair_values["R_HIKE"] - 0.05)
            self.client.fair_values["R_CUT"]  = max(0.04, self.client.fair_values["R_CUT"]  - 0.05)
            self.client.fair_values["R_HOLD"] += 0.10
        elif surprise > 0:   # hawkish
            mv = min(shift, self.client.fair_values["R_CUT"])
            self.client.fair_values["R_CUT"]  -= mv
            self.client.fair_values["R_HIKE"] += mv
        else:                # dovish
            mv = min(shift, self.client.fair_values["R_HIKE"])
            self.client.fair_values["R_HIKE"] -= mv
            self.client.fair_values["R_CUT"]  += mv
        self._normalize()
        log(f"[FED] post-CPI: H={self.client.fair_values['R_HIKE']:.3f} "
            f"N={self.client.fair_values['R_HOLD']:.3f} "
            f"C={self.client.fair_values['R_CUT']:.3f}", "fed.log")

    def on_fedspeak(self, content: str) -> None:
        t      = content.lower()
        dovish = any(kw in t for kw in self.DOVISH)
        hawkish= any(kw in t for kw in self.HAWKISH)
        neutral= any(kw in t for kw in self.NEUTRAL)

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
        """Half-Kelly position size for buying (direction='buy') or selling (direction='sell')."""
        if direction == "buy":
            if our_p <= mkt_p or mkt_p >= 1.0:
                return 0
            kf = (our_p - mkt_p) / (1.0 - mkt_p)
        else:
            if our_p >= mkt_p or mkt_p <= 0.0:
                return 0
            kf = (mkt_p - our_p) / mkt_p
        half_k  = 0.5 * kf
        target  = int(half_k * self.MAX_POS)
        if direction == "buy":
            return max(1, min(target, self.MAX_POS - pos))
        else:
            return max(1, min(target, self.MAX_POS + pos))

    async def trade(self) -> None:
        self._read_book_probs()
        if self._book_reads < 3:
            return

        for c in self.CONTRACTS:
            await self.client.cancel_quotes_for(c)

        for c in self.CONTRACTS:
            mkt_p  = self._mkt_probs.get(c)
            our_p  = self.client.fair_values.get(c)
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
                    log(f"[FED] BUY  {c} {sz}u | our={our_p:.3f} mkt={mkt_p:.3f} div={div:+.3f}", "fed.log")
                    await self.client.safe_place_order(c, sz, Side.BUY, a)
            elif div < -self.EDGE_MIN:
                sz = self._kelly_size(our_p, mkt_p, pos, "sell")
                if sz > 0:
                    log(f"[FED] SELL {c} {sz}u | our={our_p:.3f} mkt={mkt_p:.3f} div={div:+.3f}", "fed.log")
                    await self.client.safe_place_order(c, sz, Side.SELL, b)

    def reset(self) -> None:
        # Keep probability estimates across rounds — they carry over
        self._book_reads = 0


# ═══════════════════════════════════════════════════════════════════════
# Strategy: ETF Arbitrage
# ═══════════════════════════════════════════════════════════════════════

class ETFStrategy:
    """
    ETF arb: the ETF should trade at NAV = A + B + C.
    Swap cost = ETF_SWAP_COST per unit each direction.

    Two modes:

    ONE-SIDED (lower risk, preferred):
      The case packet hints the ETF is more likely to be the mispriced leg.
      If ETF > NAV + threshold: SELL the ETF directly (don't buy components)
      If ETF < NAV - threshold: BUY the ETF directly (don't sell components)
      This captures the mispricing without execution risk on 3 legs.

    FULL-SWAP (higher return, more risk):
      Only attempted when the edge is very large (> FULL_SWAP_THRESHOLD).
      Creates/redeems via the swap and trades the other side.
      Includes a timeout: if component legs don't all fill within 10s, abort.
    """

    ONE_SIDED_THRESHOLD  = 6    # edge above swap cost to do one-sided trade
    FULL_SWAP_THRESHOLD  = 12   # edge above swap cost to attempt full swap
    ONE_SIDED_QTY        = 8
    FULL_SWAP_MAX_QTY    = 5
    LEG_TIMEOUT_SECS     = 10.0

    def __init__(self, client: "MyXchangeClient"):
        self.client = client

        self.pending_create   = False
        self.pending_redeem   = False
        self.filled_comps     = {"A": 0, "B": 0, "C": 0}
        self.filled_etf       = 0
        self._target_qty      = 0
        self._pending_since   = None

    def _reset(self) -> None:
        self.pending_create = False
        self.pending_redeem = False
        self.filled_comps   = {"A": 0, "B": 0, "C": 0}
        self.filled_etf     = 0
        self._target_qty    = 0
        self._pending_since = None

    async def _abort(self) -> None:
        log("[ETF] ABORT — timing out partial arb, flattening legs")
        for s in ["A", "B", "C", "ETF"]:
            await self.client.cancel_quotes_for(s)
        self._reset()

    def _nav(self) -> Optional[float]:
        """Compute NAV using FV for A and C (more accurate than mid alone)."""
        fv_a  = self.client.fair_values.get("A")
        fv_c  = self.client.fair_values.get("C")
        _, _, mid_a = self.client.get_bba_mid("A")
        _, _, mid_b = self.client.get_bba_mid("B")
        _, _, mid_c = self.client.get_bba_mid("C")
        if mid_a is None or mid_b is None or mid_c is None:
            return None
        a = fv_a if (fv_a and fv_a > 0) else mid_a
        c = fv_c if (fv_c and fv_c > 0) else mid_c
        return a + mid_b + c

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

        diff = etf_mid - nav
        log(f"[ETF] mid={etf_mid:.1f} nav={nav:.1f} diff={diff:+.1f}")

        pos_etf = self.client.positions.get("ETF", 0)
        MAX_POS = 40

        # ── ONE-SIDED TRADES (always attempt if threshold met) ──
        # ETF overpriced: just sell the ETF
        if diff > ETF_SWAP_COST + self.ONE_SIDED_THRESHOLD and pos_etf > -MAX_POS:
            qty = min(self.ONE_SIDED_QTY, MAX_POS + pos_etf)
            if etf_bid is not None and qty > 0:
                await self.client.safe_place_order("ETF", qty, Side.SELL, etf_bid)
                log(f"[ETF] ONE-SIDED SELL {qty} ETF @ {etf_bid} | diff={diff:+.1f}")

        # ETF underpriced: just buy the ETF
        elif diff < -(ETF_SWAP_COST + self.ONE_SIDED_THRESHOLD) and pos_etf < MAX_POS:
            qty = min(self.ONE_SIDED_QTY, MAX_POS - pos_etf)
            if etf_ask is not None and qty > 0:
                await self.client.safe_place_order("ETF", qty, Side.BUY, etf_ask)
                log(f"[ETF] ONE-SIDED BUY  {qty} ETF @ {etf_ask} | diff={diff:+.1f}")

        # ── FULL-SWAP ARBS (only on large edge) ──
        if diff > ETF_SWAP_COST + self.FULL_SWAP_THRESHOLD:
            # Create: buy A+B+C, swap to ETF, sell ETF
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
            # Redeem: buy ETF, swap to components, sell components
            pos_etf = self.client.positions.get("ETF", 0)
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
# Strategy: Meta Market  (fill in on competition day)
# ═══════════════════════════════════════════════════════════════════════

class MetaStrategy:
    """
    Placeholder for the second prediction market revealed on competition day.

    Competition day checklist:
    1. Note the exact symbols from the exchange (printed on first position_snapshot)
    2. Register with self.client._ensure_symbol(sym) if not in DEFAULT_SYMBOLS
    3. Identify what the settlement condition is
    4. Find the news message_type for meta events from news.log during practice round
    5. Implement on_news() to update self.client.fair_values[sym]
    6. trade() handles the rest

    Petition news (asset, new_signatures, cumulative) may relate to this market.
    """

    META_SYMBOLS:    list[str] = []
    META_MSG_TYPES:  set[str]  = set()

    def __init__(self, client: "MyXchangeClient"):
        self.client = client
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

        # Shared model state
        self.fair_values: dict = {
            "A": None, "C": None, "ETF": None,
            "R_HIKE": 0.33, "R_HOLD": 0.33, "R_CUT": 0.34,
        }
        self.eps: dict = {"A": None, "C": None}
        self.my_quote_ids: set = set()

        # Risk limits (update from Ed post on competition day)
        _syms = self.ALL_TRADABLE
        self.max_order_size      = {s: 40  for s in _syms}
        self.max_open_orders     = {s: 50  for s in _syms}
        self.max_outstanding_vol = {s: 120 for s in _syms}
        self.max_abs_position    = {s: 200 for s in _syms}
        # Tighter position cap on C — reduces PnL swings from large directional exposure.
        # C has a noise term in its formula; holding 200 units amplifies every fluctuation.
        # We capture the same edge with a smaller, more consistent position.
        self.max_abs_position["C"] = 80

        # Stale order cleanup: track when each order was placed
        # Orders resting unfilled for > STALE_ORDER_SECS get cancelled
        self._order_placed_time: dict[str, float] = {}
        self._STALE_ORDER_SECS = 20.0   # cancel unfilled orders older than this
        self._last_stale_check: float = 0.0
        self._STALE_CHECK_INTERVAL = 5.0  # run cleanup every 5s

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
        self._round_start_wall:  float = 0.0
        self._flatten_triggered: bool  = False
        self._pending_round_reset: bool = False

    # ── Limit-aware order placement ───────────────────────────────

    async def safe_place_order(self, symbol: str, qty: int,
                               side: Side, price: int) -> Optional[str]:
        if qty <= 0:
            return None
        # Auto-register unknown symbols (e.g., meta market)
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

        result = await self.place_order(symbol, qty, side, price)
        if result is not None:
            self._order_placed_time[result] = time.time()
        return result

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

    async def cancel_stale_orders(self) -> None:
        """
        Cancel limit orders that have been resting unfilled for > STALE_ORDER_SECS.

        This prevents end-of-day rejection cascades where hundreds of unfilled
        orders from earlier in the day have consumed all available order slots.

        We skip orders that have had at least one partial fill (actively filling).
        We skip market orders (info[2] == True).
        """
        now = time.time()
        if now - self._last_stale_check < self._STALE_CHECK_INTERVAL:
            return
        self._last_stale_check = now

        cutoff = now - self._STALE_ORDER_SECS
        stale = [
            oid for oid, info in self.open_orders.items()
            if (info
                and not info[2]                          # not a market order
                and self._order_placed_time.get(oid, now) < cutoff  # old enough
                and info[1] == info[0].limit.qty         # zero fills (remaining == original)
                )
        ]
        if stale:
            log(f"[CLEANUP] cancelling {len(stale)} stale orders "
                f"(unfilled >{self._STALE_ORDER_SECS:.0f}s)")
            for oid in stale:
                self._order_placed_time.pop(oid, None)
                await self.cancel_order(oid)

    # ── Round management ──────────────────────────────────────────

    def handle_position_snapshot(self, msg) -> None:
        """Override to detect round starts.  Positions reset to 0 each round."""
        super().handle_position_snapshot(msg)
        # If all positions are near zero, this is a round start
        total = sum(abs(v) for k, v in self.positions.items() if k != "cash")
        if total < 5:
            self._pending_round_reset = True

    async def _on_round_start(self) -> None:
        log("[ROUND] New round detected — resetting per-round state")
        # Clear fills/open orders (prior round orders won't be honored)
        self.fill_tracker.clear()
        self.open_orders.clear()
        self.my_quote_ids.clear()
        self._order_placed_time.clear()
        self._last_stale_check = 0.0

        # Reset strategy per-round state
        self.strat_a.reset()
        self.strat_c.reset()       # keeps BETA/GAMMA
        self.strat_options.reset()
        self.strat_etf.reset()
        self.strat_meta.reset()
        self.strat_fed.reset()     # keeps fed prob estimates

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

        # Reset stale timer on any fill — this order is actively filling
        self._order_placed_time[order_id] = time.time()

        # ETF full-swap fill tracking
        if self.strat_etf.pending_create and sym in ("A", "B", "C"):
            self.strat_etf.filled_comps[sym] += qty
            if all(self.strat_etf.filled_comps[s] >= self.strat_etf._target_qty
                   for s in ("A", "B", "C")):
                await self.place_swap_order("toETF", self.strat_etf._target_qty)
                eb, _, _ = self.get_bba_mid("ETF")
                if eb:
                    await self.safe_place_order("ETF", self.strat_etf._target_qty, Side.SELL, eb)
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
        """Observe trade prints for smart money flow tracking."""
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
            self.strat_c.recalc_fv()

    async def bot_handle_swap_response(self, swap: str, qty: int, success: bool) -> None:
        if success:
            print(f"[SWAP] {swap} x{qty} OK")
        else:
            print(f"[SWAP] {swap} x{qty} FAILED")
            if swap in ("toETF", "fromETF"):
                await self.strat_etf._abort()

    async def bot_handle_news(self, news_release: dict) -> None:
        tick      = news_release["tick"]
        kind      = news_release["kind"]
        data      = news_release["new_data"]

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
                self.strat_fed.on_cpi(data["forecast"], data["actual"])
                self.strat_c.recalc_fv()

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
            elif msg_type in MetaStrategy.META_MSG_TYPES:
                self.strat_meta.on_news(msg_type, content, tick)
            else:
                # Unknown type — log loudly so we can classify during practice round
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
            lines.append(f"  {s:14s} pos={pos:+d}  avg={avg:.0f}  mid={mid:.0f}  uPnL={upnl:+.0f}")

        fv_a = self.fair_values.get("A"); fv_a_s = f"{fv_a:.0f}" if fv_a else "N/A"
        fv_c = self.fair_values.get("C"); fv_c_s = f"{fv_c:.0f}" if fv_c else "N/A"
        lines += [
            "─" * 50,
            f"  uPnL={total_upnl:+.0f}  cash={self.positions.get('cash', 0)}",
            f"  FV_A={fv_a_s}  FV_C={fv_c_s}  "
            f"β={self.strat_c.beta:.5f}  γ={self.strat_c.gamma:.2f}",
            f"  fed: H={self.fair_values['R_HIKE']:.3f} "
            f"N={self.fair_values['R_HOLD']:.3f} "
            f"C={self.fair_values['R_CUT']:.3f}",
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
                    # Recompute A FV if we have an EPS
                    if self.eps.get("A"):
                        self.fair_values["A"] = StockAStrategy.calc_fv(self.eps["A"])
                        print(f"[CMD] A FV updated to {self.fair_values['A']:.0f}")
                except (IndexError, ValueError):
                    print("[CMD] Usage: scale <number>")
            elif cmd.startswith("beta "):
                try:
                    self.strat_c.beta = float(cmd.split()[1])
                    self.strat_c.recalc_fv()
                    print(f"[CMD] C beta = {self.strat_c.beta}")
                except (IndexError, ValueError):
                    pass
            elif cmd.startswith("gamma "):
                try:
                    self.strat_c.gamma = float(cmd.split()[1])
                    self.strat_c.recalc_fv()
                    print(f"[CMD] C gamma = {self.strat_c.gamma}")
                except (IndexError, ValueError):
                    pass
            elif cmd.startswith("alpha "):
                # Set FlowTracker alpha: "alpha 0" to disable, "alpha 0.02" to enable lightly
                try:
                    FlowTracker.ALPHA = float(cmd.split()[1])
                    print(f"[CMD] FlowTracker.ALPHA = {FlowTracker.ALPHA}")
                except (IndexError, ValueError):
                    pass
            elif cmd.startswith("crate "):
                # Set C snipe rate limit: "crate 0.3" = max 1 snipe per 0.3s
                try:
                    self.strat_c._SNIPE_RATE_SECS = float(cmd.split()[1])
                    print(f"[CMD] C snipe rate = {self.strat_c._SNIPE_RATE_SECS}s")
                except (IndexError, ValueError):
                    pass
            elif cmd.startswith("cbudget "):
                # Set C snipe pending budget: "cbudget 10"
                try:
                    self.strat_c._SNIPE_MAX_PENDING = int(cmd.split()[1])
                    print(f"[CMD] C snipe budget = {self.strat_c._SNIPE_MAX_PENDING}")
                except (IndexError, ValueError):
                    pass
            elif cmd.startswith("meta "):
                # Register meta symbols: "meta M_YES M_NO"
                syms = cmd.split()[1:]
                MetaStrategy.META_SYMBOLS = syms
                for s in syms:
                    self._ensure_symbol(s)
                    self.fair_values[s] = 0.5
                print(f"[CMD] Meta symbols registered: {syms}")

    # ── Main trading loop ─────────────────────────────────────────

    async def trade(self) -> None:
        # Clear logs and close any stale file handles so they reopen fresh
        global _log_handles
        for handle in _log_handles.values():
            try:
                handle.close()
            except Exception:
                pass
        _log_handles = {}
        for f in ["pnl.log", "fills.log", "fed.log", "news.log", "trades.log", "meta.log", "bot.log"]:
            open(f, "w").close()

        await asyncio.sleep(5)
        self._round_start_wall = time.time()
        asyncio.create_task(self._listen_commands())

        last_status = time.time()
        last_arb    = time.time()

        while True:
            now = time.time()

            # ── Round reset (async-safe deferred from position_snapshot) ──
            if self._pending_round_reset:
                self._pending_round_reset = False
                await self._on_round_start()
                continue

            # ── Auto-flatten before round end ──
            if (not self._flatten_triggered
                    and self._round_start_wall > 0
                    and now - self._round_start_wall >= ROUND_DURATION_SECS - FLATTEN_LEAD_SECS):
                log(f"[AUTO-FLATTEN] T-{FLATTEN_LEAD_SECS}s — flattening all positions", "pnl.log")
                self._flatten_triggered = True
                await self.flatten_all()

            if self._flatten_triggered:
                await asyncio.sleep(0.5)
                continue

            # ── Pre-earnings pre-cancel ──
            await self.strat_a.check_pre_cancel()

            # ── Core strategies ──
            await self.strat_a.update_quotes()
            await self.strat_c.update_quotes()
            await self.strat_c.record_obs()
            await self.strat_fed.trade()
            await self.strat_meta.trade()

            # ── Arb strategies (slightly less frequent) ──
            if now - last_arb >= 0.5:
                last_arb = now
                await self.strat_etf.check_arb()
                await self.strat_options.run()

            # ── Stale order cleanup (every 5s) ──
            await self.cancel_stale_orders()

            # ── Status report ──
            if now - last_status >= 10:
                last_status = now
                self.print_status()

            await asyncio.sleep(0.1)

    async def start(self) -> None:
        def _exc_handler(task: asyncio.Task) -> None:
            if not task.cancelled() and task.exception():
                import traceback
                traceback.print_exception(type(task.exception()),
                                          task.exception(),
                                          task.exception().__traceback__)
        t = asyncio.create_task(self.trade())
        t.add_done_callback(_exc_handler)
        await self.connect()


# ═══════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════

async def main() -> None:
    # Tee stdout → bot.log so the monitor sees print() calls too
    import sys as _sys
    class _Tee:
        def __init__(self, stream, path):
            self._s = stream
            self._f = open(path, "a", buffering=1)
        def write(self, data):
            self._s.write(data)
            self._f.write(data)
        def flush(self):
            self._s.flush()
            self._f.flush()
        def fileno(self):          # needed for asyncio internals
            return self._s.fileno()
        def isatty(self):
            return False
    open(_BOT_LOG, "w").close()   # clear bot.log at startup
    _sys.stdout = _Tee(_sys.stdout, _BOT_LOG)

    SERVER = "practice.uchicago.exchange:3333"
    bot    = MyXchangeClient(SERVER, "chicago6", "bolt-nova-rocket")
    await bot.start()


if __name__ == "__main__":
    asyncio.run(main())
