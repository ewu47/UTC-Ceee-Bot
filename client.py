from typing import Optional

from utcxchangelib import XChangeClient, Side
import asyncio


class MyXchangeClient(XChangeClient):
    
    

    def __init__(self, host: str, username: str, password: str):
        super().__init__(host, username, password)
        self.eps_a = None
        self.fv_a = None
        self.eps_c = None
        # fed probabilities from the news or the book
        self.q_hike = 0.39
        self.q_hold = 0.42
        self.q_cut = 0.19  #tweak ^^^
        
    async def calc_fv_a(self, eps):
        return eps * 10
    
    async def calc_fv_c(self, eps, pe, b, n, lmbda, noise):
        return (eps * pe) + (lmbda * (b/n)) + noise

    async def bot_handle_cancel_response(self, order_id: str, success: bool, error: Optional[str] = None) -> None:
        pass

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int):
        pass

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
        pass

    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int):
        pass

    async def bot_handle_book_update(self, symbol: str) -> None:
        if symbol == "A" and self.fv_a is not None:
            book = self.order_books["A"]
            asks = [k for k,v in book.asks.items() if v > 0]
            bids = [k for k,v in book.bids.items() if v > 0]
            if not asks or not bids:
                return

            best_ask = min(asks)
            best_bid = max(bids)
            EDGE = 2  # require 2 ticks of edge before trading

            if best_ask < self.fv_a - EDGE:
                print(f"A underpriced: ask={best_ask} FV={self.fv_a:.1f} → BUY")
                await self.place_order("A", 5, Side.BUY, best_ask)
            elif best_bid > self.fv_a + EDGE:
                print(f"A overpriced: bid={best_bid} FV={self.fv_a:.1f} → SELL")
                await self.place_order("A", 5, Side.SELL, best_bid)
        pass

    async def bot_handle_swap_response(self, swap: str, qty: int, success: bool):
        pass
    
    async def bot_handle_news(self, news_release: dict):
        tick = news_release["tick"]
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
                    self.eps_a = value
                    self.fv_a = self.calc_fv_a(self.eps_a)
                    print(f"[NEWS] A EPS = {value}  FV_A = {self.fv_a}")
                elif asset == "C":
                    self.eps_c = value
                    self.fv_c = self.calc_fv_c(self.eps_c)
                    print(f"[NEWS] C EPS = {value}  FV_C = {self.fv_C}")
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
        await asyncio.sleep(5)

        # Place an order
        await self.place_order("A", 10, Side.BUY, 100)

        # You can also look at order books like this
        for security, book in self.order_books.items():
            if book.bids or book.asks:
                sorted_bids = sorted((k,v) for k,v in book.bids.items() if v != 0)
                sorted_asks = sorted((k,v) for k,v in book.asks.items() if v != 0)
                print(f"Bids for {security}:\n{sorted_bids}")
                print(f"Asks for {security}:\n{sorted_asks}")

    async def start(self):
        def _handle_trade_exception(task):
            if not task.cancelled() and task.exception():
                print(f"Trade task exception: {task.exception()}")
        self._trade_task = asyncio.create_task(self.trade())
        self._trade_task.add_done_callback(_handle_trade_exception)
        await self.connect()


async def main():
    SERVER = '34.197.188.76:3333'
    my_client = MyXchangeClient(SERVER, "chicago6", "bolt-nova-rocket")
    await my_client.start()


if __name__ == "__main__":
    asyncio.run(main())