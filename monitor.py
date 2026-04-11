"""
monitor.py — problems-only view.

Shows only lines that indicate something is wrong or costing PnL.
Normal trading activity (snipes, quotes, fills) is suppressed entirely.
"""

import os, re, time
from collections import defaultdict

R = "\033[91m"; Y = "\033[93m"; M = "\033[95m"; W = "\033[97m"; RST = "\033[0m"
def col(s, c): return f"{c}{s}{RST}"

BOT_LOG = "bot.log"

# Each rule: (regex, colour, label, description)
# Only lines that indicate a problem, miscalibration, or missed opportunity.
RULES = [
    # ── Wrong model / miscalibration ─────────────────────────────
    (r"implied_scale=(?!1\.0)(?!0\.9)(?!1\.1)\d",  # scale far from 1.0
     R, "A-SCALE-WRONG",
     "EPS scale is off — type 'scale X' in bot terminal"),

    (r"\[C\].*gap=.*\([-+]?[2-9]\d\.|\([-+]?[1-9]\d{2,}",  # C FV >20% off market
     Y, "C-FV-GAP",
     "C fair value is >20% from market mid — check beta/gamma"),

    (r"WARNING",
     R, "WARNING", ""),

    # ── Order infrastructure problems ────────────────────────────
    (r"\[REJECTED\]",
     R, "REJECTED",
     "Order rejected by exchange — check position/volume limits"),

    (r"\[SWAP\].*FAIL",
     R, "SWAP-FAIL",
     "ETF swap failed — pending arb state may be stuck"),

    (r"\[ETF\] ABORT",
     R, "ETF-ABORT",
     "ETF arb aborted after timeout — partial legs may be open"),

    (r"\[CLEANUP\] cancelling [1-9]\d ",   # 10+ stale orders at once
     Y, "MANY-STALE",
     "10+ stale orders at once — order accumulation is high"),

    # ── Options not trading ──────────────────────────────────────
    (r"\[OPT\] skipped.*no-book",
     Y, "OPT-NO-BOOK",
     "Options/B book is empty — no arb possible right now"),

    # ── News we can't interpret ──────────────────────────────────
    (r"UNKNOWN TYPE",
     R, "UNKNOWN-NEWS",
     "Unrecognised news type — update FEDSPEAK_MSG_TYPES or META_MSG_TYPES"),

    (r"PETITION.*cumul=(?:[5-9]\d\d|[1-9]\d{3,})",  # petition cumulative >= 500
     M, "PETITION-HIGH",
     "Petition signature count is high — may affect meta market"),

    # ── Round / session ──────────────────────────────────────────
    (r"COMMITTED",
     W, "C-COMMITTED",
     "C model committed — beta/gamma now locked in"),

    (r"\[AUTO-FLATTEN\]",
     W, "FLATTENING",
     "Round ending — auto-flatten triggered"),
]

compiled = [(re.compile(p, re.I), c, l, d) for p, c, l, d in RULES]

# Throttle repeated identical labels: show first occurrence, then 1 per 30s
_last_seen: dict[str, float] = defaultdict(float)
_counts:    dict[str, int]   = defaultdict(int)
REPEAT_INTERVAL = 30.0

def process(line: str) -> None:
    line = line.rstrip()
    if not line:
        return
    for pat, colour, label, desc in compiled:
        if pat.search(line):
            _counts[label] += 1
            now = time.time()
            if now - _last_seen[label] < REPEAT_INTERVAL:
                return   # suppress repeat within window
            _last_seen[label] = now
            tag  = col(f"[{label}]", colour)
            note = col(f"  ↳ {desc}", colour) if desc else ""
            print(f"{tag} {line}", flush=True)
            if note:
                print(note, flush=True)
            break
    # All other lines silently dropped

def run() -> None:
    print(col("═"*55, W))
    print(col("  UTC 2026 — Problems Monitor", W))
    print(col("  Silent = everything is working fine", W))
    print(col("═"*55 + "\n", W))

    f = pos = None
    checked = time.time()
    last_alive = time.time()

    while True:
        if f is None:
            if os.path.exists(BOT_LOG):
                f = open(BOT_LOG, "r"); pos = 0
            else:
                time.sleep(0.2); continue

        f.seek(pos)
        chunk = f.read()
        if chunk:
            pos = f.tell()
            last_alive = time.time()
            for line in chunk.splitlines():
                process(line)

        now = time.time()
        # Detect log truncation (new run)
        if now - checked >= 1.0:
            checked = now
            try:
                if os.path.getsize(BOT_LOG) < pos:
                    print(col("\n  [new run started — counters reset]\n", W), flush=True)
                    f.close(); f = None; pos = 0; _counts.clear(); _last_seen.clear()
            except FileNotFoundError:
                f = None; pos = 0

        # Heartbeat: if no output for 60s, confirm bot is alive
        if now - last_alive > 60:
            last_alive = now
            n_rejected = _counts.get("REJECTED", 0)
            print(col(f"  [still watching — {n_rejected} rejections so far]", W), flush=True)

        time.sleep(0.05)

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print(col("\nStopped.", W))
        if any(_counts.values()):
            print(col("Session totals:", W))
            for label, n in sorted(_counts.items()):
                c = R if label in ("REJECTED","WARNING","SWAP-FAIL") else Y
                print(f"  {col(label, c)}: {n}")
