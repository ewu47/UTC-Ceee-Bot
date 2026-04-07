from typing import Optional

from utcxchangelib import XChangeClient, Side
import asyncio
import math


# ── Constants (fill in on competition day) ──────────────────────────
PE_A = 10
PE0_C = None
GAMMA = None
Y0 = None
BETA = None
B0 = None
D = None
CONVEXITY = None
N = None
LAMBDA = None
RF_RATE = 0
ETF_COST = 5

OPTION_STRIKES = [950, 1000, 1050]


# ── Strategy modules ────────────────────────────────────────────────

class StockAStrategy:
    """Small-cap stock with constant P/E. Edge: fast reaction to earnings news."""

    def __init__(self, client):
        self.client = client

    def calc_fair_value(self, eps):
        pass

    async def on_book_update(self):
        pass

    async def update_quotes(self):
        pass

    async def on_earnings(self, eps):
        pass


class StockCStrategy:
    """Large-cap insurance company. Price depends on earnings + bond portfolio + fed rates."""

    def __init__(self, client):
        self.client = client

    def calc_fair_value(self, eps):
        pass

    async def on_book_update(self):
        pass

    async def update_quotes(self):
        pass

    async def on_earnings(self, eps):
        pass

    def recalc_fair_value(self):
        """Recalculate C fair value after fed prob changes."""
        pass


class OptionsStrategy:
    """B options arbitrage. European calls/puts at strikes 950, 1000, 1050.

    Key formulas:
    - Put-call parity: C - P = S0 - K * e^(-r*T)
    - Box spread: bull call spread + bear put spread = K2 - K1
    """

    def __init__(self, client):
        self.client = client

    async def on_book_update(self, symbol):
        pass

    async def check_pcp_arb(self, strike):
        """Put-call parity arbitrage at a given strike."""
        pass

    async def check_box_spread(self, strike_low, strike_high):
        """Box spread arbitrage between two strikes."""
        pass

    async def run(self):
        """Called each tick from the trade loop."""
        pass


class FedStrategy:
    """Federal Reserve prediction market trading + fed prob management.

    - Read R_HIKE/R_HOLD/R_CUT book mids to get market-implied probs
    - CPI news shifts our probs: actual > forecast = hawkish (hike), vice versa = dovish (cut)
    - Trade R contracts when our probs diverge from market
    """

    def __init__(self, client):
        self.client = client
        self.cpi_sensitivity = 0.1  # tweak

    def update_probs_from_book(self):
        """Read R_HIKE/R_HOLD/R_CUT book mids to update fed_probs."""
        for contract, key in [("R_HIKE", "hike"), ("R_HOLD", "hold"), ("R_CUT", "cut")]:
            best_bid, best_ask = self.client.get_best_bid_ask(contract)
            if best_bid is not None and best_ask is not None:
                mid = (best_bid + best_ask) / 2
                self.client.fed_probs[key] = mid / 1000

    def on_cpi_news(self, forecast, actual):
        """Shift fed probs based on CPI surprise."""
        surprise = actual - forecast
        print(f"[FED] CPI surprise={surprise:+.6f} ({'hawkish' if surprise > 0 else 'dovish'})")
        shift = self.cpi_sensitivity * abs(surprise)
        if surprise > 0:
            actual_shift = min(shift, self.client.fed_probs["cut"])
            self.client.fed_probs["cut"] -= actual_shift
            self.client.fed_probs["hike"] += actual_shift
        else:
            actual_shift = min(shift, self.client.fed_probs["hike"])
            self.client.fed_probs["hike"] -= actual_shift
            self.client.fed_probs["cut"] += actual_shift

    async def trade_prediction_market(self):
        """Trade R contracts when our probs diverge from market probs."""
        # TODO: compare self.client.fed_probs with market-implied probs
        # If our prob > market prob, buy that contract
        # If our prob < market prob, sell that contract
        pass


class ETFStrategy:
    """ETF arbitrage: swap between ETF and components A + B + C.

    - 1 ETF = 1A + 1B + 1C (swap costs ETF_COST per unit)
    - If ETF price > NAV + cost: buy components, swap to ETF, sell ETF
    - If ETF price < NAV - cost: buy ETF, redeem, sell components
    """

    def __init__(self, client):
        self.client = client
        self.num_etf = 1  # tweak

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

        nav = mid_a + mid_b + mid_c
        diff = etf_mid - nav
        print(f"[ETF] etf_mid={etf_mid:.1f} nav={nav:.1f} diff={diff:.1f}")

        if diff > ETF_COST:
            # ETF overpriced: buy components at ask, swap to ETF, sell ETF at bid
            print(f"[ETF] overpriced by {diff:.1f}, creating ETF")
            await asyncio.gather(
                self.client.place_order("A", self.num_etf, Side.BUY, ask_a),
                self.client.place_order("B", self.num_etf, Side.BUY, ask_b),
                self.client.place_order("C", self.num_etf, Side.BUY, ask_c),
            )
            await self.client.place_swap_order("toETF", self.num_etf)
            await self.client.place_order("ETF", self.num_etf, Side.SELL, etf_bid)
        elif diff < -ETF_COST:
            # ETF underpriced: buy ETF at ask, redeem, sell components at bid
            print(f"[ETF] underpriced by {abs(diff):.1f}, redeeming ETF")
            await self.client.place_order("ETF", self.num_etf, Side.BUY, etf_ask)
            await self.client.place_swap_order("fromETF", self.num_etf)
            await asyncio.gather(
                self.client.place_order("A", self.num_etf, Side.SELL, bid_a),
                self.client.place_order("B", self.num_etf, Side.SELL, bid_b),
                self.client.place_order("C", self.num_etf, Side.SELL, bid_c),
            )


# ── Main client ─────────────────────────────────────────────────────

class MyXchangeClient(XChangeClient):
    def __init__(self, host: str, username: str, password: str):
        super().__init__(host, username, password)

        # Shared state
        self.eps = {"A": None, "C": None}
        self.fair_values = {"A": None, "C": None, "ETF": None}
        self.fed_probs = {"hike": 0.39, "hold": 0.42, "cut": 0.19}  # tweak starting probs
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
        to_cancel = [oid for oid in list(self.my_quote_ids)
                     if oid in self.open_orders and self.open_orders[oid][0].symbol == symbol]
        if to_cancel:
            await asyncio.gather(*(self.cancel_order(oid) for oid in to_cancel))

    # ── Handlers ─────────────────────────────────────────────────────

    async def bot_handle_cancel_response(self, order_id: str, success: bool, error: Optional[str] = None) -> None:
        if success:
            self.my_quote_ids.discard(order_id)
        else:
            print(f"[CANCEL] Failed to cancel order {order_id}: {error}")

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int):
        print(f"[FILL] order {order_id}: {qty} @ {price}")

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
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
            print(f"[NEWS] unstructured: {content}")
            # TODO: parse unstructured news for fed sentiment

    async def bot_handle_market_resolved(self, market_id: str, winning_symbol: str, tick: int):
        print(f"Market {market_id} resolved: winner is {winning_symbol}")

    async def bot_handle_settlement_payout(self, user: str, market_id: str, amount: int, tick: int):
        print(f"Settlement payout: {amount} from {market_id}")

    # ── Main trade loop ──────────────────────────────────────────────

    async def trade(self):
        await asyncio.sleep(5)

        while True:
            await asyncio.gather(
                self.strat_a.update_quotes(),
                self.strat_c.update_quotes(),
                self.strat_etf.check_arb(),
                self.strat_options.run(),
                self.strat_fed.trade_prediction_market(),
            )
            await asyncio.sleep(0.2)

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
