# UChicago Trading Competition 2026 — Case 1

Algorithmic market-making bot for the UTC 2026 Case 1.

---

## Setup

```bash
pip install git+https://github.com/UChicagoFM/utcxchangelib.git
python final_bot.py
```

Update credentials at the bottom of `final_bot.py` before running:

```python
SERVER = "practice.uchicago.exchange:3333"
bot    = MyXchangeClient(SERVER, "your_username", "your_password")
```

---

## What It Trades

| Symbol | Description |
|--------|-------------|
| `A` | Small-cap equity — earnings-driven, constant P/E |
| `B` | Semiconductor — no direct news, price inferred from options via PCP |
| `C` | Insurance company — EPS + Fed rate sensitivity |
| `ETF` | Basket of 1A + 1B + 1C, swappable at 5-tick cost |
| `B_C/P_950/1000/1050` | European calls and puts on B |
| `R_HIKE / R_HOLD / R_CUT` | Fed prediction markets, settle at 1000 or 0 |

---

## Strategy Summary

**Stock A** is the primary alpha source. Every EPS release (20 per round, at seconds 22 and 88 of each 90-second day), the bot sweeps all stale book orders using the new fair value `EPS × PE_A × EPS_SCALE_A`. Between earnings it market-makes passively with inventory skew. `EPS_SCALE_A` self-calibrates from market observations.

**Stock C** uses a one-parameter empirical model: `C_FV = anchor + sensitivity × ΔERC`, where sensitivity is calibrated from the first significant CPI print. After calibration, the bot takes a directional position on every CPI before C reprices.

**Fed markets** are traded using half-Kelly sizing based on probability estimates updated by CPI surprises and fedspeak headlines. Positions are capped at ±25 per contract. A settlement trade fires once per round in the final 90 seconds when conviction is high.

**ETF** is traded against its NAV (A + B + C) when it drifts by more than 11 ticks. One-sided trades only — the ETF is the mispriced leg.

**Options** are checked for PCP and box-spread arbitrage every 1.5 seconds. In practice these rarely trigger due to bid-ask friction; the main value of options is extracting an implied B price for the ETF NAV calculation.

---

## Runtime Commands

While the bot is running, type into stdin:

| Command | Effect |
|---------|--------|
| `s` | Print current positions, fair values, PnL estimate |
| `f` | Manually flatten all positions |
| `scale <n>` | Override `EPS_SCALE_A` (e.g. `scale 100`) |
| `sens <n>` | Override C sensitivity (e.g. `sens -2.5`) |
| `meta SYM1 SYM2` | Register meta market symbols (revealed on competition day) |

---

## Key Files

| File | Description |
|------|-------------|
| `final_bot.py` | The bot |
| `dictionary.html` | Full reference — every parameter, constant, state variable, and log message |
| `slideshow.html` | Development story and architecture walkthrough (16 slides) |

---

## Important Notes

- **C sensitivity sign:** Should always be negative. If the log shows `[C] CALIBRATED sensitivity=+X`, type `sens -X` immediately.
- **EPS_SCALE_A:** If A fair value looks wrong after first earnings, check `implied_scale` in the log and type `scale <value>` to correct it.
- **Meta market:** Symbols are revealed on competition day. Register them live with `meta SYM1 SYM2` and update `FedStrategy.FEDSPEAK_MSG_TYPES` if the news type string is different from expected.
- **Logs:** `fills.log`, `fed.log`, `news.log`, `pnl.log`, `meta.log` are written to the working directory. `bot.log` gets everything.
