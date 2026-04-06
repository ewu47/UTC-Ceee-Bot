from typing import Optional

from utcxchangelib import XChangeClient, Side
import asyncio
import math

PE_A = 10
#hopefully will be given to us??
PE0_C = None
GAMMA = None
Y0 = None
BETA = None
B0 = None
D = None
CONVEXITY = None
N = None
LAMBDA = None

class MyXchangeClient(XChangeClient):
    


    def __init__(self, host: str, username: str, password: str):
        super().__init__(host, username, password)
        self.eps = {"A": None, "C": None}
        self.fair_values = {"A": None, "C": None, "ETF": None}
        self.spreads = {"A": 5, "C": 10} #change
        self.skews = {"A": 0.1, "C": 0.1} #change
        # fed probabilities from the news or the book
        self.fed_probs = {"hike": 0.39, "hold": 0.42, "cut": 0.19} #tweak starting probs?
        self.rf_rate = 0
        #maximums will be given on ed
        self.max_order_size = None
        self.max_open_order = None #size of unfilled order
        self.max_outstanding_vol = None #total volume of unfilled order
        self.max_abs_position = None #sum of long+short positions
        self.my_quote_ids = set()
        
    def calc_fv_a(self, eps):
        return eps * PE_A
    
    def calc_fv_c(self, eps):
        expected_rate_change = (25 * self.fed_probs["hike"]) + (-25 * self.fed_probs["cut"])
        dy = BETA * expected_rate_change
        pe_c = PE0_C * math.exp(-GAMMA * dy)
        delta_b = B0 * (-D * dy + 0.5 * CONVEXITY * dy**2)
        return (eps * pe_c) + (LAMBDA * delta_b / N)

    async def bot_handle_cancel_response(self, order_id: str, success: bool, error: Optional[str] = None) -> None:
        if success:
            self.my_quote_ids.discard(order_id)
        else:
            print(f"[CANCEL] Failed to cancel order {order_id}: {error}")

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int):
        pass

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
        pass

    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int):
        pass

    async def bot_handle_book_update(self, symbol: str) -> None:
        if symbol == "A" and self.fair_values["A"] is not None:
            book = self.order_books["A"]
            asks = [k for k,v in book.asks.items() if v > 0]
            bids = [k for k,v in book.bids.items() if v > 0]
            if not asks or not bids:
                return

            best_ask = min(asks)
            best_bid = max(bids)
            EDGE = 2  # require 2 ticks of edge before trading

            if best_ask < self.fair_values["A"] - EDGE:
                print(f"A underpriced: ask={best_ask} FV={self.fair_values['A']:.1f} → BUY")
                await self.place_order("A", 5, Side.BUY, best_ask)
            elif best_bid > self.fair_values["A"] + EDGE:
                print(f"A overpriced: bid={best_bid} FV={self.fair_values['A']:.1f} → SELL")
                await self.place_order("A", 5, Side.SELL, best_bid)
        pass

    async def bot_handle_swap_response(self, swap: str, qty: int, success: bool):
        pass
    
    async def bot_handle_news(self, news_release: dict): #note potential bug - parent class has (self, timestamp, news_release) variables
        tick = news_release["tick"] #note potential bug - no "tick" key in dictionary in parent class
        news_type = news_release["kind"]
        symbol = news_release["symbol"]  # may be None
        news_data = news_release["new_data"]

        if news_type == "structured":
            subtype = news_data["structured_subtype"]
            if subtype == "earnings":
                asset = news_data["asset"]
                print(f"asset: {asset}")
                value = news_data["value"]
                print(f"value: {value}")
                if asset == "A":
                    self.eps["A"] = value
                    self.fair_values["A"] = self.calc_fv_a(self.eps["A"])
                    print(f"[NEWS] A EPS = {value}  FV_A = {self.fair_values['A']}")
                elif asset == "C":
                    self.eps["C"] = value
                    self.fair_values["C"] = self.calc_fv_c(self.eps["C"])
                    print(f"[NEWS] C EPS = {value}  FV_C = {self.fair_values['C']}")
            elif subtype == "cpi_print":
                forecast = news_data["forecast"]
                print(f"forecast: {forecast}")
                actual = news_data["actual"]
                print(f"actual: {actual}")
                surprise = news_data["actual"] - news_data["forecast"]
                print(f"[NEWS] CPI surprise={surprise:+.6f} ({'hawkish' if surprise>0 else 'dovish'})")
        else:
            content = news_data["content"]
            message_type = news_data["type"]
            
    async def bot_handle_market_resolved(self, market_id: str, winning_symbol: str, tick: int):
        print(f"Market {market_id} resolved: winner is {winning_symbol}")

    async def bot_handle_settlement_payout(self, user: str, market_id: str, amount: int, tick: int):
        print(f"Settlement payout: {amount} from {market_id}")



    async def trade(self):
        """This is a simple example bot that places orders and prints updates."""
        await asyncio.sleep(5) #why do we want to wait 5 seconds?

        while True:
            await self.update_quotes("A")
            await self.update_quotes("C")
            await asyncio.sleep(0.2) #length of a tick

        # You can also look at order books like this
        for security, book in self.order_books.items():
            if book.bids or book.asks:
                sorted_bids = sorted((k,v) for k,v in book.bids.items() if v != 0)
                sorted_asks = sorted((k,v) for k,v in book.asks.items() if v != 0)
                print(f"Bids for {security}:\n{sorted_bids}")
                print(f"Asks for {security}:\n{sorted_asks}")

    async def update_quotes(self, symbol):
        fv = self.fair_values.get(symbol)
        if fv is None:
            return       
        spread = self.spreads[symbol]
        skew_per_unit = self.skews[symbol]
        position = self.positions[symbol]
        skew = skew_per_unit * position
        bid = round(fv - spread / 2 - skew)
        ask = round(fv + spread / 2 - skew)
        to_cancel = [oid for oid in list(self.my_quote_ids) if oid in self.open_orders and self.open_orders[oid][0].symbol == symbol]
        for oid in to_cancel:
            await self.cancel_order(oid)
        if position < self.max_abs_position:
            bid_id = await self.place_order(symbol, 5, Side.BUY, bid)
            self.my_quote_ids.add(bid_id)
        if position > -self.max_abs_position:
            ask_id = await self.place_order(symbol, 5, Side.SELL, ask)
            self.my_quote_ids.add(ask_id)
        print(f"[QUOTES] {symbol} fv={fv:.1f} pos={position} bid={bid} ask={ask}")

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