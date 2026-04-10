from typing import Optional

from utcxchangelib import XChangeClient, Side
import asyncio
import math


# ── Constants (fill in on competition day) ──────────────────────────
PE_A = 10
PE0_C = 14.0
EPS0 = 2.00
Y0 = 0.045
B0_OVER_N = 40
D = 7.5
C = 55.0
LAMBDA = 0.65
RF_RATE = 0
ETF_COST = 5

GAMMA = None #are we going to get these or will we have to figure them out?
BETA = None

OPTION_STRIKES = [950, 1000, 1050]


# ── Strategy modules ────────────────────────────────────────────────

class StockAStrategy:
    """Small-cap stock with constant P/E. Edge: fast reaction to earnings news.

    Two modes:
    1. POST-EARNINGS (first ~2s after news): aggressively sweep stale orders
       at every price level better than FV. This is our primary edge.
    2. PASSIVE MM (rest of the time): quote bid/ask around FV with inventory
       skew to earn spread while staying flat.
    """

    def __init__(self, client):
        self.client = client

        # ── Exchange hard limits for A ──
        self.MAX_ORDER_SIZE = 40
        self.MAX_OUTSTANDING_VOL = 120
        self.MAX_OPEN_ORDERS = 50
        self.MAX_ABS_POS = 200

        # ── Passive MM params ──
        self.base_spread = 4
        self.skew_per_unit = 0.15
        self.quote_size = 10
        self.max_position = 100  # soft limit — start unwinding beyond this

        # ── Aggressive params ──
        self.edge_threshold = 2
        self.snipe_size = 20
        self.post_earnings_ticks = 10

        # ── Stop loss params ──

        # ── State ──
        self.earnings_countdown = 0
        self.prev_fv = None
        self.pending_cancels = set()
        self.last_book_snipe_time = 0.0
        self.my_outstanding_vol = 0
        self.my_open_order_count = 0
        # Track last placed quotes to avoid pointless cancel+replace churn
        self.last_bid = None   # (price, size)
        self.last_ask = None   # (price, size)

    # ── Limit tracking: use ACTUAL exchange state, no optimistic assumptions ──

    def _outstanding_volume_actual(self):
        """What the exchange considers our outstanding volume for A.
        This counts ALL open orders including pending cancels,
        because the exchange hasn't processed those cancels yet."""
        total = 0
        for info in self.client.open_orders.values():
            if info and info[0].symbol == "A" and not info[2]:
                total += info[1]
        return total

    def _open_order_count_actual(self):
        """What the exchange considers our open order count for A."""
        count = 0
        for info in self.client.open_orders.values():
            if info and info[0].symbol == "A" and not info[2]:
                count += 1
        return count

    async def _place_order_a(self, qty, side, price):
        """THE ONLY WAY to place an order for A. Hard-checks all limits."""
        if qty <= 0:
            return None

        qty = min(qty, self.MAX_ORDER_SIZE)

        # Hard limit: outstanding volume
        cur_vol = self._outstanding_volume_actual()
        if cur_vol + qty > self.MAX_OUTSTANDING_VOL:
            qty = self.MAX_OUTSTANDING_VOL - cur_vol
            if qty <= 0:
                return None

        # Hard limit: open order count
        if self._open_order_count_actual() + 1 > self.MAX_OPEN_ORDERS:
            return None

        # Hard limit: absolute position
        pos = self.client.positions["A"]
        if side == Side.BUY and pos + qty > self.MAX_ABS_POS:
            qty = self.MAX_ABS_POS - pos
        elif side == Side.SELL and pos - qty < -self.MAX_ABS_POS:
            qty = self.MAX_ABS_POS + pos
        if qty <= 0:
            return None

        oid = await self.client.place_order("A", qty, side, price)
        return oid

    def calc_fair_value(self, eps):
        # NOTE: EPS arrives as a float in dollars (e.g. 1.023), but exchange
        # prices are integer cents (e.g. 1023). Multiply by 100 to match units.
        # Verify: print("[A] raw EPS", eps) and compare to market mid on first run.
        return eps * PE_A * 100

    # ── Cancel helper ──

    async def _cancel_a_quotes(self):
        """Cancel ALL open orders for A — quotes, snipes, and stop-loss orders.
        Using all open_orders (not just my_quote_ids) prevents stale orders accumulating."""
        to_cancel = [
            oid for oid, info in self.client.open_orders.items()
            if info and info[0].symbol == "A" and oid not in self.pending_cancels
        ]
        for oid in to_cancel:
            self.client.my_quote_ids.discard(oid)
            self.pending_cancels.add(oid)
            await self.client.cancel_order(oid)

    def on_cancel_confirmed(self, order_id):
        self.pending_cancels.discard(order_id)

    def on_order_rejected(self, order_id):
        self.pending_cancels.discard(order_id)

    # ── Stop loss removed ──
    # The stop loss was creating a buy-sell loop with the passive MM:
    # MM sells at FV+2, market fills it (short), stop loss buys at market,
    # MM sells again → net loss every cycle. Position risk is managed
    # purely through spread widening and inventory skew instead.

    # ── Aggressive: sweep stale orders after earnings ──

    async def _sweep_stale_orders(self):
        """Walk the book and pick off every resting order better than FV.

        Sweep only reduces position toward 0 — never overshoots to the other side.
        If already flat or on the wrong side, skip that sweep direction.
        """
        fv = self.client.fair_values.get("A")
        if fv is None:
            return
        book = self.client.order_books["A"]
        pos = self.client.positions["A"]

        # Buy side: only sweep if short (buying reduces position toward 0)
        if pos < 0:
            stale_asks = sorted(
                [(px, qty) for px, qty in book.asks.items() if qty > 0 and px < fv - 1],
                key=lambda x: x[0]
            )
            for px, qty in stale_asks:
                if pos >= 0:
                    break  # don't overshoot to long
                buy_qty = min(qty, self.snipe_size, abs(pos))
                oid = await self._place_order_a(buy_qty, Side.BUY, px)
                if oid is None:
                    break
                pos += buy_qty
                print(f"[A] SNIPE BUY {buy_qty} @ {px} | edge={fv - px:.0f} | pos→{pos}")

        # Sell side: only sweep if long (selling reduces position toward 0)
        if pos > 0:
            stale_bids = sorted(
                [(px, qty) for px, qty in book.bids.items() if qty > 0 and px > fv + 1],
                key=lambda x: -x[0]
            )
            for px, qty in stale_bids:
                if pos <= 0:
                    break  # don't overshoot to short
                sell_qty = min(qty, self.snipe_size, pos)
                oid = await self._place_order_a(sell_qty, Side.SELL, px)
                if oid is None:
                    break
                pos -= sell_qty
                print(f"[A] SNIPE SELL {sell_qty} @ {px} | edge={px - fv:.0f} | pos→{pos}")

    async def on_book_update(self):
        """On book update, check for mispriced orders. Rate-limited to avoid spam."""
        fv = self.client.fair_values.get("A")
        if fv is None:
            return

        now = asyncio.get_event_loop().time()

        # During post-earnings window, sweep but rate-limit to 5/sec
        if self.earnings_countdown > 0:
            if now - self.last_book_snipe_time < 0.2:
                return
            self.last_book_snipe_time = now
            await self._sweep_stale_orders()
            return

        # Outside earnings: rate-limit to 2/sec max
        if now - self.last_book_snipe_time < 0.5:
            return
        self.last_book_snipe_time = now

        best_bid, best_ask = self.client.get_best_bid_ask("A")

        if best_ask is not None and best_ask < fv - self.edge_threshold:
            oid = await self._place_order_a(self.snipe_size, Side.BUY, best_ask)
            if oid:
                print(f"[A] BUY @ {best_ask} | signal: ask {best_ask} < FV {fv:.0f} - {self.edge_threshold}")
        elif best_bid is not None and best_bid > fv + self.edge_threshold:
            oid = await self._place_order_a(self.snipe_size, Side.SELL, best_bid)
            if oid:
                print(f"[A] SELL @ {best_bid} | signal: bid {best_bid} > FV {fv:.0f} + {self.edge_threshold}")

    # ── Passive MM: quote around FV with inventory skew ──

    async def update_quotes(self):
        """Quote bid/ask around FV. Skew toward reducing position."""
        fv = self.client.fair_values.get("A")
        if fv is None:
            # Before first earnings: use market mid as FV estimate
            best_bid, best_ask = self.client.get_best_bid_ask("A")
            if best_bid is not None and best_ask is not None:
                fv = (best_bid + best_ask) / 2
                self.client.fair_values["A"] = fv
                print(f"[A] no earnings yet, using market mid as FV={fv:.1f}")
            else:
                return

        pos = self.client.positions["A"]
        cash = self.client.positions.get("cash", 0)

        # Decay the post-earnings countdown
        if self.earnings_countdown > 0:
            self.earnings_countdown -= 1

        best_bid, best_ask = self.client.get_best_bid_ask("A")

        # ── Dynamic spread: widen when position is large ──
        pos_frac = abs(pos) / max(self.max_position, 1)
        spread = self.base_spread + int(pos_frac * 3)

        # ── Inventory skew: shift mid toward reducing position ──
        skew = self.skew_per_unit * pos

        bid = round(fv - spread / 2 - skew)
        ask = round(fv + spread / 2 - skew)
        if bid >= ask:
            ask = bid + 1

        # ── At position limits: force unwind at market price ──
        # When pinned at MAX_ABS_POS, FV-based quotes may be far from market.
        # Override to market price so we can actually get out.
        if pos >= self.MAX_ABS_POS and best_bid is not None:
            ask = best_bid  # sell at best bid to guarantee fill
        if pos <= -self.MAX_ABS_POS and best_ask is not None:
            bid = best_ask  # buy at best ask to guarantee fill

        # ── Don't place a quote that would be immediately filled at a loss ──
        # Skip only when NOT at position limits (limits override above handles that case).
        if abs(pos) < self.MAX_ABS_POS:
            if best_ask is not None and bid >= best_ask:
                bid = 0
            if best_bid is not None and ask <= best_bid:
                ask = 0

        # ── Size: reduce on the side that would increase risk ──
        bid_size = self.quote_size if pos < self.max_position else max(2, self.quote_size // 3)
        ask_size = self.quote_size if pos > -self.max_position else max(2, self.quote_size // 3)

        # ── Skip cancel+replace if quotes haven't changed — avoids flooding exchange ──
        # Compare intended quotes (not whether they were placed) so a blocked ask
        # doesn't cause an infinite cancel+replace loop.
        if (bid, bid_size) == self.last_bid and (ask, ask_size) == self.last_ask:
            return

        # Record intended quotes immediately — even if orders get blocked below,
        # the intention hasn't changed so we won't loop.
        self.last_bid = (bid, bid_size)
        self.last_ask = (ask, ask_size)

        # ── Cancel existing quotes then place new ones ──
        await self._cancel_a_quotes()

        if bid > 0:
            bid_id = await self._place_order_a(bid_size, Side.BUY, bid)
            if bid_id:
                self.client.my_quote_ids.add(bid_id)
            else:
                bid_size = 0
        else:
            bid_size = 0

        if ask > 0:
            ask_id = await self._place_order_a(ask_size, Side.SELL, ask)
            if ask_id:
                self.client.my_quote_ids.add(ask_id)
            else:
                ask_size = 0
        else:
            ask_size = 0

        outvol = self._outstanding_volume_actual()
        print(f"[A] MM: fv={fv:.1f} pos={pos} cash={cash} spread={spread} skew={skew:.1f} "
              f"bid={bid}x{bid_size} ask={ask}x{ask_size} outvol={outvol}")

    # ── Earnings handler ──

    async def on_earnings(self, eps):
        """New EPS → update FV, cancel stale quotes, immediately sweep the book."""
        self.prev_fv = self.client.fair_values.get("A")
        self.client.eps["A"] = eps
        self.client.fair_values["A"] = self.calc_fair_value(eps)
        fv = self.client.fair_values["A"]

        delta = fv - self.prev_fv if self.prev_fv else 0
        print(f"[A] *** EARNINGS *** EPS={eps} → FV={fv} (Δ={delta:+.1f}) pos={self.client.positions['A']} cash={self.client.positions.get('cash', 0)}")

        # Enter aggressive mode
        self.earnings_countdown = self.post_earnings_ticks

        # Force quotes to be re-placed after earnings even if prices are numerically the same
        self.last_bid = None
        self.last_ask = None

        # Cancel our existing quotes immediately so we don't get picked off
        await self._cancel_a_quotes()

        # Sweep the book for stale orders
        await self._sweep_stale_orders()


class StockCStrategy:
    """Large-cap insurance company. Price depends on earnings + bond portfolio + fed rates."""

    def __init__(self, client):
        self.client = client

    def calc_fair_value(self, eps):
        expected_rate_change = 25 * self.client.fair_values["R_HIKE"] - 25 * self.client.fair_values["R_CUT"]
        delta_y = BETA * expected_rate_change
        pe = PE0_C * math.exp(-GAMMA * delta_y)
        delta_b_over_b0 = -D * delta_y + 0.5 * C * delta_y ** 2
        return eps * pe + LAMBDA * B0_OVER_N * delta_b_over_b0

    async def on_book_update(self):
        pass

    async def update_quotes(self):
        pass

    async def on_earnings(self, eps):
        pass

    def recalc_fair_value(self):
        """Recalculate C fair value after fed prob changes."""
        pass

    # TODO: calculate beta and gamma


class OptionsStrategy:
    """B options arbitrage. European calls/puts at strikes 950, 1000, 1050.

    With RF_RATE = 0, formulas simplify to:
    - Put-call parity: C - P = S - K
    - Box spread: (long C K1, short C K2, short P K1, long P K2) = K2 - K1

    PCP arb legs:
      If C - P > S - K:  sell call, buy put, buy stock
      If C - P < S - K:  buy call, sell put, sell stock

    Box arb legs:
      Buy box  (cost < K2-K1): buy C K1, sell C K2, sell P K1, buy P K2
      Sell box (receipt > K2-K1): sell C K1, buy C K2, buy P K1, sell P K2
    """

    BOX_PAIRS = [(950, 1000), (1000, 1050)]

    def __init__(self, client):
        self.client = client
        self.arb_qty = 1
        self.min_edge = 3
        # Per-arb cooldown: keyed by arb identifier, value is last-fired time.
        # Prevents re-entering the same arb every tick when legs are getting rejected.
        self._last_arb_time = {}
        self.arb_cooldown = 5.0  # seconds between re-entries of same arb

    def _bba(self, symbol):
        return self.client.get_best_bid_ask(symbol)

    def _cooled_down(self, key):
        """Return True if enough time has passed to re-enter this arb."""
        now = asyncio.get_event_loop().time()
        if now - self._last_arb_time.get(key, 0) < self.arb_cooldown:
            return False
        self._last_arb_time[key] = now
        return True

    async def check_pcp_arb(self, strike):
        """Put-call parity: C - P = S - K (r=0)."""
        c_bid, c_ask = self._bba(f"B_C_{strike}")
        p_bid, p_ask = self._bba(f"B_P_{strike}")
        s_bid, s_ask = self._bba("B")

        if None in (c_bid, c_ask, p_bid, p_ask, s_bid, s_ask):
            return

        buy_call_edge = p_bid + s_bid - c_ask - strike
        if buy_call_edge > self.min_edge and self._cooled_down(f"pcp_buy_{strike}"):
            print(f"[OPT] PCP K={strike}: buy call, sell put, sell B | edge={buy_call_edge}")
            await self.client.place_order(f"B_C_{strike}", self.arb_qty, Side.BUY, c_ask)
            await self.client.place_order(f"B_P_{strike}", self.arb_qty, Side.SELL, p_bid)
            await self.client.place_order("B", self.arb_qty, Side.SELL, s_bid)
            return

        sell_call_edge = c_bid - p_ask - s_ask + strike
        if sell_call_edge > self.min_edge and self._cooled_down(f"pcp_sell_{strike}"):
            print(f"[OPT] PCP K={strike}: sell call, buy put, buy B | edge={sell_call_edge}")
            await self.client.place_order(f"B_C_{strike}", self.arb_qty, Side.SELL, c_bid)
            await self.client.place_order(f"B_P_{strike}", self.arb_qty, Side.BUY, p_ask)
            await self.client.place_order("B", self.arb_qty, Side.BUY, s_ask)

    async def check_box_spread(self, strike_low, strike_high):
        """Box spread always worth K2 - K1 at expiry (r=0)."""
        k1, k2 = strike_low, strike_high
        c1_bid, c1_ask = self._bba(f"B_C_{k1}")
        c2_bid, c2_ask = self._bba(f"B_C_{k2}")
        p1_bid, p1_ask = self._bba(f"B_P_{k1}")
        p2_bid, p2_ask = self._bba(f"B_P_{k2}")

        if None in (c1_bid, c1_ask, c2_bid, c2_ask, p1_bid, p1_ask, p2_bid, p2_ask):
            return

        theoretical = k2 - k1

        buy_cost = c1_ask - c2_bid + p2_ask - p1_bid
        buy_edge = theoretical - buy_cost
        if buy_edge > self.min_edge and self._cooled_down(f"box_buy_{k1}_{k2}"):
            print(f"[OPT] BOX BUY ({k1},{k2}): cost={buy_cost} theoretical={theoretical} edge={buy_edge}")
            await self.client.place_order(f"B_C_{k1}", self.arb_qty, Side.BUY, c1_ask)
            await self.client.place_order(f"B_C_{k2}", self.arb_qty, Side.SELL, c2_bid)
            await self.client.place_order(f"B_P_{k1}", self.arb_qty, Side.SELL, p1_bid)
            await self.client.place_order(f"B_P_{k2}", self.arb_qty, Side.BUY, p2_ask)
            return

        sell_receipt = c1_bid - c2_ask + p2_bid - p1_ask
        sell_edge = sell_receipt - theoretical
        if sell_edge > self.min_edge and self._cooled_down(f"box_sell_{k1}_{k2}"):
            print(f"[OPT] BOX SELL ({k1},{k2}): receipt={sell_receipt} theoretical={theoretical} edge={sell_edge}")
            await self.client.place_order(f"B_C_{k1}", self.arb_qty, Side.SELL, c1_bid)
            await self.client.place_order(f"B_C_{k2}", self.arb_qty, Side.BUY, c2_ask)
            await self.client.place_order(f"B_P_{k1}", self.arb_qty, Side.BUY, p1_ask)
            await self.client.place_order(f"B_P_{k2}", self.arb_qty, Side.SELL, p2_bid)

    async def on_book_update(self, _symbol):
        # Intentionally a no-op — run() handles periodic sweeps.
        # on_book_update was causing double-firing and flooding the exchange.
        pass

    async def run(self):
        """Sweep all arbs every 2 seconds."""
        now = asyncio.get_event_loop().time()
        if now - self._last_arb_time.get("_run", 0) < 2.0:
            return
        self._last_arb_time["_run"] = now
        for strike in OPTION_STRIKES:
            await self.check_pcp_arb(strike)
        for k1, k2 in self.BOX_PAIRS:
            await self.check_box_spread(k1, k2)


class FedStrategy:
    """Federal Reserve prediction market trading + fed prob management.

    - Read R_HIKE/R_HOLD/R_CUT book mids to get market-implied probs
    - CPI news shifts our probs: actual > forecast = hawkish (hike), vice versa = dovish (cut)
    - FEDSPEAK: parse keywords for dovish/hawkish signals
    - Trade R contracts when our probs diverge from market
    """

    DOVISH_KEYWORDS = [
        "easing", "cooling", "dovish", "cut", "lower rates",
        "easing inflation", "slowing growth", "policy easing",
        "expectations of policy easing",
    ]
    HAWKISH_KEYWORDS = [
        "restrictive", "hawkish", "hike", "inflation risks",
        "stay restrictive", "tightening", "higher for longer",
        "restrictive for longer",
    ]

    def __init__(self, client):
        self.client = client
        self.cpi_sensitivity = 100  # tweak — surprises are ~0.0003-0.0007, so need large multiplier
        self.fedspeak_shift = 0.03  # how much each headline shifts probs (tweak)
        self.fed_probs = {"R_HIKE": None, "R_HOLD": None, "R_CUT": None}
        self.edge_threshold = 0.05 # edge before trading (tweak)
        self.fed_qty = 1 # tweak (do we want to minimize exposure?)
        self.book_reads = 0  # warmup counter — don't trade until we've read the book

    def update_probs_from_book(self):
        """Read R_HIKE/R_HOLD/R_CUT book mids to update fed_probs."""
        updated = 0
        for contract in ["R_HIKE", "R_HOLD", "R_CUT"]:
            best_bid, best_ask = self.client.get_best_bid_ask(contract)
            if best_bid is not None and best_ask is not None:
                mid = (best_bid + best_ask) / 2
                self.fed_probs[contract] = mid / 1000
                # Seed our fair value estimate from the book so we start close to market
                self.client.fair_values[contract] = mid / 1000
                updated += 1
        if updated == 3:
            self.book_reads += 1

    def on_cpi_news(self, forecast, actual):
        """Shift fair value estimation of fed probs based on CPI surprise."""
        surprise = actual - forecast
        print(f"[FED] CPI surprise={surprise:+.6f} ({'hawkish' if surprise > 0 else 'dovish'})")
        shift = self.cpi_sensitivity * abs(surprise)
        if surprise > 0:
            actual_shift = min(shift, self.client.fair_values["R_CUT"])
            self.client.fair_values["R_CUT"] -= actual_shift
            self.client.fair_values["R_HIKE"] += actual_shift
        else:
            actual_shift = min(shift, self.client.fair_values["R_HIKE"])
            self.client.fair_values["R_HIKE"] -= actual_shift
            self.client.fair_values["R_CUT"] += actual_shift
        print(f"[FED] probs after CPI: R_HIKE={self.client.fair_values["R_HIKE"]}, \
            R_HOLD={self.client.fair_values["R_HOLD"]}, R_CUT={self.client.fair_values["R_CUT"]}")

    def on_fedspeak(self, content):
        """Parse unstructured FEDSPEAK for dovish/hawkish signals."""
        text = content.lower()
        dovish = any(kw in text for kw in self.DOVISH_KEYWORDS)
        hawkish = any(kw in text for kw in self.HAWKISH_KEYWORDS)

        if dovish and not hawkish:
            shift = min(self.fedspeak_shift, self.client.fair_values["R_HIKE"])
            self.client.fair_values["R_HIKE"] -= shift
            self.client.fair_values["R_CUT"] += shift
            print(f"[FED] DOVISH: '{content}' → probs: R_HIKE={self.client.fair_values["R_HIKE"]}, \
                R_HOLD={self.client.fair_values["R_HOLD"]}, R_CUT={self.client.fair_values["R_CUT"]}")
        elif hawkish and not dovish:
            shift = min(self.fedspeak_shift, self.client.fair_values["R_CUT"])
            self.client.fair_values["R_CUT"] -= shift
            self.client.fair_values["R_HIKE"] += shift
            print(f"[FED] HAWKISH: '{content}' → probs: R_HIKE={self.client.fair_values["R_HIKE"]}, \
                R_HOLD={self.client.fair_values["R_HOLD"]}, R_CUT={self.client.fair_values["R_CUT"]}")
        else:
            print(f"[FED] NEUTRAL: '{content}'")

    async def trade_prediction_market(self):
        """Trade R contracts when our probs diverge from market probs.

        - If our prob > market prob, buy that contract
        - If our prob < market prob, sell that contract
        """
        # Don't trade until we've read the book at least 3 times — avoids
        # trading against 33/33/34 priors when the market already has strong views
        if self.book_reads < 3:
            return
        for contract in ["R_HIKE", "R_HOLD", "R_CUT"]:
            market_prob = self.fed_probs[contract]
            estimated_fv_prob = self.client.fair_values[contract]
            if market_prob is None or estimated_fv_prob is None:
                continue
            divergence = estimated_fv_prob - market_prob
            best_bid, best_ask = self.client.get_best_bid_ask(contract)
            if best_bid is None or best_ask is None:
                continue
            
            if divergence > self.edge_threshold:
                # we think this outcome is more likely than market does -> buy
                print(f"[FED] {contract} underpriced: estimation={estimated_fv_prob:.3f} market={market_prob:.3f} -> BUY")
                await self.client.place_order(contract, self.fed_qty, Side.BUY, best_ask)
            elif divergence < -self.edge_threshold:
                # we think this outcome is less likely than market does -> sell
                print(f"[FED] {contract} overpriced: estimation={estimated_fv_prob:.3f} market={market_prob:.3f} -> SELL")
                await self.client.place_order(contract, self.fed_qty, Side.SELL, best_bid)
    
    # TODO: how to change hold probability?
    # limit order size


class ETFStrategy:
    """ETF arbitrage: swap between ETF and components A + B + C.

    - 1 ETF = 1A + 1B + 1C (swap costs ETF_COST per unit)
    - If ETF price > NAV + cost: buy components, swap to ETF, sell ETF
    - If ETF price < NAV - cost: buy ETF, redeem, sell components
    """

    def __init__(self, client):
        self.client = client
        self.etf_qty = 1  # tweak - create function to calculate based on position

    async def check_arb(self):
        if self.client.fair_values["A"] is None or self.client.fair_values["C"] is None:
            return

        etf_bid, etf_ask, etf_mid = self.client.get_bba_mid("ETF")
        if etf_mid is None:
            return
        bid_a, ask_a, mid_a = self.client.get_bba_mid("A")
        bid_b, ask_b, mid_b = self.client.get_bba_mid("B")
        bid_c, ask_c, mid_c = self.client.get_bba_mid("C")
        if mid_a is None or mid_b is None or mid_c is None:
            return

        nav = self.client.fair_values["A"] + mid_b + self.client.fair_values["C"]
        diff = etf_mid - nav
        print(f"[ETF] etf_mid={etf_mid:.1f} nav={nav:.1f} diff={diff:.1f}")

        if diff > ETF_COST:
            # ETF overpriced: buy components at ask, swap to ETF, sell ETF at bid
            print(f"[ETF] overpriced by {diff:.1f}, creating ETF")
            await asyncio.gather(
                self.client.place_order("A", self.etf_qty, Side.BUY, ask_a),
                self.client.place_order("B", self.etf_qty, Side.BUY, ask_b),
                self.client.place_order("C", self.etf_qty, Side.BUY, ask_c),
            )
            await self.client.place_swap_order("toETF", self.etf_qty)
            await self.client.place_order("ETF", self.etf_qty, Side.SELL, etf_bid)
        elif diff < -ETF_COST:
            # ETF underpriced: buy ETF at ask, redeem, sell components at bid
            print(f"[ETF] underpriced by {abs(diff):.1f}, redeeming ETF")
            await self.client.place_order("ETF", self.etf_qty, Side.BUY, etf_ask)
            await self.client.place_swap_order("fromETF", self.etf_qty)
            await asyncio.gather(
                self.client.place_order("A", self.etf_qty, Side.SELL, bid_a),
                self.client.place_order("B", self.etf_qty, Side.SELL, bid_b),
                self.client.place_order("C", self.etf_qty, Side.SELL, bid_c),
            )
    
    # TODO: make etf_qty rely on current position
    # (want to minimize exposure/risk)


# ── Main client ─────────────────────────────────────────────────────

class MyXchangeClient(XChangeClient):
    def __init__(self, host: str, username: str, password: str):
        super().__init__(host, username, password)

        # Shared state
        self.fair_values = {"A": None, "C": None, "ETF": None, "R_HIKE": 0.33, "R_HOLD": 0.33, "R_CUT": 0.34} #tweak starting probs
        self.eps = {"A": None, "C": None}
        self.my_quote_ids = set()

        # Risk limits
        ALL_SYMBOLS = ["A", "B", "B_C_950", "B_P_950", "B_C_1000", "B_P_1000",
                       "B_C_1050", "B_P_1050", "C", "ETF", "R_CUT", "R_HOLD", "R_HIKE"]
        self.max_order_size = {s: 40 for s in ALL_SYMBOLS}
        self.max_open_orders = {s: 50 for s in ALL_SYMBOLS}
        self.max_outstanding_vol = {s: 120 for s in ALL_SYMBOLS}
        self.max_abs_position = {s: 200 for s in ALL_SYMBOLS}

        # Strategy modules
        self.strat_a = StockAStrategy(self)
        self.strat_c = StockCStrategy(self)
        self.strat_options = OptionsStrategy(self)
        self.strat_fed = FedStrategy(self)
        self.strat_etf = ETFStrategy(self)

    # ── Helpers ──────────────────────────────────────────────────────

    def get_best_bid_ask(self, symbol):
        """Return (best_bid, best_ask) or (None, None) if book is empty."""
        book = self.order_books[symbol]
        bids = [k for k, v in book.bids.items() if v > 0]
        asks = [k for k, v in book.asks.items() if v > 0]
        best_bid = max(bids) if bids else None
        best_ask = min(asks) if asks else None
        return best_bid, best_ask

    def get_bba_mid(self, symbol):
        """Return (best_bid, best_ask, mid) or (None, None, None)."""
        best_bid, best_ask = self.get_best_bid_ask(symbol)
        if best_bid is None or best_ask is None:
            return None, None, None
        return best_bid, best_ask, (best_bid + best_ask) / 2

    async def cancel_quotes_for(self, symbol):
        """Cancel all our outstanding quotes for a given symbol."""
        # First clean stale IDs that are no longer in open_orders
        self.my_quote_ids = {oid for oid in self.my_quote_ids if oid in self.open_orders}
        to_cancel = [oid for oid in self.my_quote_ids
                     if self.open_orders[oid][0].symbol == symbol]
        for oid in to_cancel:
            self.my_quote_ids.discard(oid)
        # Cancel sequentially to avoid overwhelming gRPC
        for oid in to_cancel:
            await self.cancel_order(oid)

    # ── Handlers ─────────────────────────────────────────────────────

    async def bot_handle_cancel_response(self, order_id: str, success: bool, error: Optional[str] = None) -> None:
        self.strat_a.on_cancel_confirmed(order_id)
        if success:
            self.my_quote_ids.discard(order_id)
        else:
            print(f"[CANCEL] Failed to cancel order {order_id}: {error}")

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int):
        self.strat_a.pending_cancels.discard(order_id)
        print(f"[FILL] order {order_id}: {qty} @ {price}")

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
        self.strat_a.on_order_rejected(order_id)
        self.my_quote_ids.discard(order_id)
        print(f"[REJECTED] order {order_id}: {reason}")

    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int):
        pass

    async def bot_handle_book_update(self, symbol: str) -> None:
        if symbol == "A":
            await self.strat_a.on_book_update()
        elif symbol == "C":
            await self.strat_c.on_book_update()
        elif symbol in ["R_HIKE", "R_HOLD", "R_CUT"]:
            self.strat_fed.update_probs_from_book()
            self.strat_c.recalc_fair_value()
        elif symbol.startswith("B_"):
            await self.strat_options.on_book_update(symbol)

    async def bot_handle_swap_response(self, swap: str, qty: int, success: bool):
        if success:
            print(f"[SWAP] {swap} x{qty} succeeded")
        else:
            print(f"[SWAP] {swap} x{qty} failed")

    async def bot_handle_news(self, news_release: dict):
        tick = news_release["tick"]
        news_type = news_release["kind"]
        news_data = news_release["new_data"]

        if news_type == "structured":
            subtype = news_data["structured_subtype"]
            if subtype == "earnings":
                asset = news_data["asset"]
                value = news_data["value"]
                if asset == "A":
                    await self.strat_a.on_earnings(value)
                elif asset == "C":
                    await self.strat_c.on_earnings(value)
            elif subtype == "cpi_print":
                self.strat_fed.on_cpi_news(news_data["forecast"], news_data["actual"])
                self.strat_c.recalc_fair_value()
        else:
            content = news_data["content"]
            self.strat_fed.on_fedspeak(content)
            self.strat_c.recalc_fair_value()

    async def bot_handle_market_resolved(self, market_id: str, winning_symbol: str, tick: int):
        print(f"Market {market_id} resolved: winner is {winning_symbol}")

    async def bot_handle_settlement_payout(self, user: str, market_id: str, amount: int, tick: int):
        print(f"Settlement payout: {amount} from {market_id}")

    # ── Main trade loop ──────────────────────────────────────────────

    async def trade(self):
        await asyncio.sleep(5)

        while True:
            await self.strat_a.update_quotes()
            await self.strat_c.update_quotes()
            await self.strat_etf.check_arb()
            await self.strat_options.run()
            await self.strat_fed.trade_prediction_market()
            await asyncio.sleep(0.1)

    async def start(self):
        def _handle_trade_exception(task):
            if not task.cancelled() and task.exception():
                print(f"Trade task exception: {task.exception()}")
        self._trade_task = asyncio.create_task(self.trade())
        self._trade_task.add_done_callback(_handle_trade_exception)
        await self.connect()


async def main():
    SERVER = 'practice.uchicago.exchange:3333'
    my_client = MyXchangeClient(SERVER, "chicago6", "bolt-nova-rocket")
    await my_client.start()


if __name__ == "__main__":
    asyncio.run(main())
