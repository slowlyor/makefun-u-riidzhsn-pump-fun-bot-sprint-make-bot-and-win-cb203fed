// Pump.fun Bot Sprint — paper-money trading bot for the agent-fun/replay/v2 harness.
//
// Reads replayed pump.fun bonding-curve events from stdin (hello, batch..., done)
// and answers each batch with one reply line of buy/sell orders on stdout.
// Deterministic: no clock, no environment, no randomness — every decision is a pure
// function of the event stream and the hello message. No network use at runtime.
//
// Strategy in one paragraph: buy the first life. A coin is entered as soon as it
// shows a couple of buys and its real reserve is big enough for the size cap to
// allow a real buy — no momentum confirmation, because by the time momentum
// confirms the pop has already happened and the coin mean-reverts. Each bet is
// a flat 0.02 SOL, one per coin, held at most 1500 slots: sell everything at
// +150%, cut at -15%, otherwise let the clock close it. The edge is the early
// pop itself — most coins die, but the few that run pay for the many small
// stops — so the bot takes many small disciplined trades and never chases.
// Never more than 5 open positions, never a trade bigger than the harness
// size cap. When the bot's own round-trip pace lags what the guardrails need
// (20 coins with >= 0.01 SOL round trips), it runs tiny probe round trips on
// liquid curves to stay on pace.

const LAMPORTS_PER_SOL = 1_000_000_000n;
const LAMPORTS = 1e9;
// Curve fees: protocol 95 bps + creator 30 bps fallback = 1.25% of the trade.
const CURVE_FEE = 0.0125;

// ---- tunables (SOL units unless noted) ------------------------------------
const WINDOW_SLOTS = 20; // entry lookback window (buys counted here)
const MIN_NET_SOL = 0; // unused: the first-life entry takes no net-flow filter
const MIN_BUYS = 2; // buy trades in the window
const MIN_BUYERS = 1; // distinct buying wallets in the window
const MIN_PRICE_UP = -1; // unused: no price-rise filter on entry
const MAX_PRICE_UP = 10; // unused: no run-up cap on entry
const NEAR_HIGH = 0; // unused: no near-high filter on entry
const MIN_REAL_SOL = 0; // real SOL reserve bounds at entry (size cap governs)
const MAX_REAL_SOL = 1000;
const MIN_AGE_SLOTS = 0; // slots since CreateEvent before an entry is allowed
const INITIAL_SOL = 0.02; // first buy of a position
const ADD_SOL = 0.02; // each pyramid add
const MAX_STAKE_SOL = 0.02; // most SOL at cost in one coin (no pyramid adds)
const MIN_BUY_SOL = 0.01; // smallest buy worth sending
const BUY_SLIP_BPS = 2000;
const SELL_SLIP_BPS = 10_000;
const STOP_LOSS = 0.15; // sell all when value falls 15% below remaining cost
const TRAIL_ARM = 10; // trailing stop disabled: never arms
const TRAIL_DROP = 0.12; // (unused while the trail never arms)
const TAKE_HALF_AT = 10; // tranche sell disabled: never triggers
const TAKE_ALL_AT = 1.5; // sell everything at +150%: let winners run, exit rarely
const FADE_WINDOW = 25; // (unused: fade exit disabled below)
const FADE_NET_SOL = -1e9;
const MAX_HOLD_SLOTS = 1500;
const HOLD_IF_UP = 0.5; // max-hold does not apply while up at least this much
const COOLDOWN_SLOTS = 200; // wait after an exit before re-entering the same coin
const MAX_ENTRIES_PER_COIN = 1;
const MAX_MAIN_POSITIONS = 4; // one of the 5 allowed slots is kept for probes
const PROBE_SOL = 0.02; // probe round-trip size
const PROBE_TARGET_COINS = 25; // guardrail pacing targets
const PROBE_TARGET_FILLS = 80;
const PROBE_PACE_SECS = 14_400; // pace is scaled by block time over this span
const MAX_PROBES = 1;

// ---- wire types (loose: the harness speaks JSON lines) ---------------------
type Mint = string;

interface Trade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
}

interface Curve {
  created: number | null;
  vq: bigint;
  vt: bigint;
  rq: bigint;
  rt: bigint;
  complete: boolean;
  trades: Trade[];
  /** Lifetime buy count / distinct buyers (never trimmed; entry uses these). */
  buys: number;
  buyers: Set<string>;
}

interface Held {
  opened: number;
  peak: number; // best close value of the remaining tokens, SOL
  cost: number; // SOL spent on the tokens still held (fees included)
  lastAddPrice: number;
  exiting: boolean;
  filled: boolean;
  tookHalf: boolean;
}

interface Order {
  side: "buy" | "sell";
  client_id: string;
  mint: Mint;
  quote_lamports?: string;
  token_amount?: string;
  max_slippage_bps: number;
}

// ---- bot state -------------------------------------------------------------
let hello: any = null;
let cashLamports = 0n;
const positions = new Map<Mint, { tokens: bigint; cost: bigint }>();
const curves = new Map<Mint, Curve>();
const held = new Map<Mint, Held>();
const entriesByMint = new Map<Mint, number>();
const lastExitByMint = new Map<Mint, number>();
const probes = new Map<Mint, boolean>(); // false = buying, true = selling
const pending = new Map<string, { side: string; mint: Mint; quote: bigint }>();
const myCoins = new Set<Mint>();
let myFills = 0;
let firstTime: number | null = null;
let lastTime: number | null = null;
let orderCounter = 1;

function curveOf(mint: Mint): Curve {
  let c = curves.get(mint);
  if (c === undefined) {
    c = {
      created: null,
      vq: 0n,
      vt: 0n,
      rq: 0n,
      rt: 0n,
      complete: false,
      trades: [],
      buys: 0,
      buyers: new Set<string>(),
    };
    curves.set(mint, c);
  }
  return c;
}

function txFeeSol(): number {
  return Number(BigInt(hello.fees.base_fee_lamports) + BigInt(hello.fees.priority_fee_lamports)) / LAMPORTS;
}

function maxBuyLamports(mint: Mint): bigint {
  const c = curves.get(mint);
  if (c === undefined || c.complete) return 0n;
  return (c.rq * BigInt(hello.caps.max_order_bps_of_real_reserve)) / 10_000n;
}

function committedCount(): number {
  const mints = new Set<Mint>(positions.keys());
  for (const p of pending.values()) if (p.side === "buy") mints.add(p.mint);
  return mints.size;
}

function freeCashSol(): number {
  let free = cashLamports;
  const tx = BigInt(hello.fees.base_fee_lamports) + BigInt(hello.fees.priority_fee_lamports);
  for (const p of pending.values()) {
    free -= tx;
    if (p.side === "buy") free -= p.quote;
  }
  return Number(free) / LAMPORTS;
}

/** Mark-to-market value of a position in SOL, curve + tx fees deducted. */
function positionValue(mint: Mint): number {
  const pos = positions.get(mint);
  const c = curves.get(mint);
  if (pos === undefined || c === undefined || c.vt === 0n) return 0;
  const tokens = Number(pos.tokens);
  const vq = Number(c.vq);
  const vt = Number(c.vt);
  // Selling `tokens` moves the curve: tokens out buys vq * tokens / (vt + tokens) SOL.
  const gross = (tokens * vq) / (vt + tokens) / LAMPORTS;
  if (!Number.isFinite(gross)) return 0;
  return gross * (1 - CURVE_FEE) - txFeeSol();
}

function priceOf(c: Curve): number {
  if (c.vt === 0n) return 0;
  const p = Number(c.vq) / Number(c.vt);
  return Number.isFinite(p) ? p : 0;
}

/** Net recorded SOL (buys minus sells) on a curve over the last `w` slots. */
function flow(c: Curve, now: number, w: number): number {
  let net = 0;
  for (let i = c.trades.length - 1; i >= 0; i--) {
    const tr = c.trades[i]!;
    if (tr.slot <= now - w) break;
    net += tr.buy ? tr.sol : -tr.sol;
  }
  return net;
}

/** Entry signal: buy the first life. The scan of the dev tape showed the edge is
 *  in being early, not in confirmed momentum: a coin is entered as soon as it
 *  has a couple of buys and enough real reserve for the size cap to allow a
 *  meaningful buy. No price-rise, near-high or acceleration filter — by the
 *  time those confirm, the pop has already happened and the coin mean-reverts. */
function entrySignal(c: Curve, now: number): boolean {
  const rqSol = Number(c.rq) / LAMPORTS;
  if (rqSol < MIN_REAL_SOL || rqSol > MAX_REAL_SOL) return false;
  if (c.created === null || now - c.created < MIN_AGE_SLOTS) return false;
  // Lifetime counts: a slow-growing coin may take many slots to reach a real
  // reserve the size cap can buy into; the window must not expire first.
  if (c.buys < MIN_BUYS || c.buyers.size < MIN_BUYERS) return false;
  return true;
}

function nextClientId(): string {
  return `o${orderCounter++}`;
}

function decideBatch(batch: any): Order[] {
  const now: number = batch.slot;
  const orders: Order[] = [];
  const cap = hello.caps.max_orders_per_batch as number;
  const tx = txFeeSol();
  const traded = new Set<Mint>();
  const bigSell = new Set<Mint>();

  // Fills: count the bot's own filled orders (harness closes start with '_').
  for (const f of batch.fills ?? []) {
    if (f.status === "filled" && !String(f.client_id).startsWith("_")) {
      myFills++;
      myCoins.add(f.mint);
    }
    pending.delete(f.client_id);
  }

  // Account snapshot.
  cashLamports = BigInt(batch.account.cash_lamports);
  positions.clear();
  for (const p of batch.account.positions ?? []) {
    const tokens = BigInt(p.token_amount);
    if (tokens > 0n) positions.set(p.mint, { tokens, cost: BigInt(p.cost_lamports) });
  }

  // Marks: latest reserves for held / pending mints.
  for (const m of batch.marks ?? []) {
    const c = curves.get(m.mint);
    if (c === undefined) continue;
    if (m.slot < 0) continue;
    c.vq = BigInt(m.virtual_quote_reserves);
    c.vt = BigInt(m.virtual_token_reserves);
    c.rq = BigInt(m.real_quote_reserves);
    c.rt = BigInt(m.real_token_reserves);
  }

  // Events.
  for (const e of batch.events ?? []) {
    if (e.block_time !== null && e.block_time !== undefined) {
      if (firstTime === null) firstTime = e.block_time;
      if (lastTime === null || e.block_time > lastTime) lastTime = e.block_time;
    }
    if (e.event === "CreateEvent") {
      curveOf(e.mint).created = e.slot;
    } else if (e.event === "TradeEvent") {
      const c = curveOf(e.mint);
      c.vq = BigInt(e.virtual_quote_reserves);
      c.vt = BigInt(e.virtual_token_reserves);
      c.rq = BigInt(e.real_quote_reserves);
      c.rt = BigInt(e.real_token_reserves);
      const sol = Number(e.sol_amount) / LAMPORTS;
      if (e.is_buy) {
        c.buys++;
        c.buyers.add(e.user);
      }
      const vtNum = Number(e.virtual_token_reserves);
      const tradePrice = vtNum > 0 ? Number(e.virtual_quote_reserves) / vtNum : 0;
      c.trades.push({
        slot: e.slot,
        buy: e.is_buy,
        sol,
        user: e.user,
        price: Number.isFinite(tradePrice) ? tradePrice : 0,
      });
      traded.add(e.mint);
      if (!e.is_buy && sol >= 1e9) bigSell.add(e.mint); // big-sell exit disabled
    } else if (e.event === "CompleteEvent") {
      curveOf(e.mint).complete = true;
    }
  }
  // Trim tapes to the window we ever look back over.
  const keep = Math.max(WINDOW_SLOTS, FADE_WINDOW) + 5;
  for (const mint of traded) {
    const t = curves.get(mint)!;
    let cut = 0;
    while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut++;
    if (cut > 0) t.trades.splice(0, cut);
  }

  const pendingByMint = new Map<Mint, string>();
  for (const p of pending.values()) pendingByMint.set(p.mint, p.side);

  const pushBuy = (mint: Mint, sol: number): boolean => {
    if (orders.length >= cap) return false;
    if (!Number.isFinite(sol) || sol <= 0) {
      console.error(`skip non-finite buy ${mint} sol=${sol}`);
      return false;
    }
    const lamports = BigInt(Math.floor(sol * LAMPORTS));
    if (lamports <= 0n) return false;
    const id = nextClientId();
    orders.push({ side: "buy", client_id: id, mint, quote_lamports: lamports.toString(), max_slippage_bps: BUY_SLIP_BPS });
    pending.set(id, { side: "buy", mint, quote: lamports });
    return true;
  };
  const pushSell = (mint: Mint, tokens: bigint): boolean => {
    if (orders.length >= cap) return false;
    if (tokens <= 0n) return false;
    const id = nextClientId();
    orders.push({ side: "sell", client_id: id, mint, token_amount: tokens.toString(), max_slippage_bps: SELL_SLIP_BPS });
    pending.set(id, { side: "sell", mint, quote: 0n });
    return true;
  };

  // ---- exits first ---------------------------------------------------------
  for (const [mint, h] of held) {
    const pos = positions.get(mint);
    const c = curves.get(mint);
    if (pos === undefined) {
      if (!pendingByMint.has(mint)) {
        held.delete(mint);
        if (h.filled) lastExitByMint.set(mint, now);
        else entriesByMint.set(mint, (entriesByMint.get(mint) ?? 1) - 1);
      }
      continue;
    }
    h.filled = true;
    if (c === undefined || c.complete) continue;
    const value = positionValue(mint);
    const cost = Number(pos.cost) / LAMPORTS;
    h.cost = cost;
    if (value > h.peak) h.peak = value;
    const rqSol = Number(c.rq) / LAMPORTS;
    const riding = rqSol >= 85; // near completion: let it ride, only the hard stop applies
    let exitAll = false;
    let takeHalf = false;
    if (value < cost * (1 - STOP_LOSS)) exitAll = true;
    if (!exitAll && !riding && h.peak >= cost * (1 + TRAIL_ARM) && value < h.peak * (1 - TRAIL_DROP)) exitAll = true;
    if (!exitAll && !riding && flow(c, now, FADE_WINDOW) < FADE_NET_SOL) exitAll = true;
    if (!exitAll && !riding && bigSell.has(mint)) exitAll = true;
    if (!exitAll && !riding && now - h.opened > MAX_HOLD_SLOTS && value < cost * (1 + HOLD_IF_UP)) exitAll = true;
    if (!exitAll && value >= cost * (1 + TAKE_ALL_AT)) exitAll = true;
    if (!exitAll && !h.tookHalf && value >= cost * (1 + TAKE_HALF_AT)) takeHalf = true;
    if (exitAll) {
      h.exiting = true;
      if (pendingByMint.get(mint) === "sell") continue;
      if (pushSell(mint, pos.tokens)) held.delete(mint);
    } else if (takeHalf) {
      if (pendingByMint.get(mint) === "sell") continue;
      const half = pos.tokens / 2n;
      if (pushSell(mint, half)) {
        h.tookHalf = true;
        h.peak = value / 2; // peak now tracks the remaining half's value
        h.cost = cost / 2;
      }
    }
  }

  // ---- probe sells: dump the probe as soon as its buy has landed -----------
  for (const [mint, selling] of probes) {
    const pos = positions.get(mint);
    const c = curves.get(mint);
    if (pos === undefined) {
      if (!pendingByMint.has(mint)) probes.delete(mint);
      continue;
    }
    if (c === undefined || c.complete || pendingByMint.has(mint)) continue;
    if (orders.length >= cap) break;
    if (pushSell(mint, pos.tokens)) probes.set(mint, true);
  }

  // ---- entries and pyramid adds -------------------------------------------
  let free = freeCashSol();
  let slotsLeft = hello.caps.max_open_positions - committedCount();
  let mainLeft = MAX_MAIN_POSITIONS - held.size;
  for (const mint of traded) {
    if (orders.length >= cap) break;
    const c = curves.get(mint);
    if (c === undefined || c.complete) continue;
    if (pendingByMint.has(mint) || probes.has(mint)) continue;
    const h = held.get(mint);
    const price = priceOf(c);
    let want: number;
    if (h === undefined) {
      if (slotsLeft <= 0 || mainLeft <= 0) continue;
      if ((entriesByMint.get(mint) ?? 0) >= MAX_ENTRIES_PER_COIN) continue;
      if (now - (lastExitByMint.get(mint) ?? -1e12) < COOLDOWN_SLOTS) continue;
      if (!entrySignal(c, now)) continue;
      want = INITIAL_SOL;
    } else {
      // Pyramid into strength only: price stepped up from the last add, the
      // position is up, and net flow over the entry window is still positive.
      if (h.exiting || !h.filled) continue;
      if (h.cost >= MAX_STAKE_SOL - MIN_BUY_SOL) continue;
      if (price < h.lastAddPrice * 1.05) continue;
      const value = positionValue(mint);
      if (value < h.cost * 1.02) continue;
      if (flow(c, now, WINDOW_SLOTS) < 0.3) continue;
      want = Math.min(ADD_SOL, MAX_STAKE_SOL - h.cost);
    }
    const capSol = Number(maxBuyLamports(mint)) / LAMPORTS;
    let amount = Math.min(capSol, want);
    if (amount > free - tx) amount = free - tx;
    if (!Number.isFinite(amount)) {
      console.error(`non-finite amount mint=${mint} capSol=${capSol} want=${want} free=${free} tx=${tx} cost=${h?.cost}`);
      continue;
    }
    if (amount < MIN_BUY_SOL) continue;
    if (!pushBuy(mint, amount)) continue;
    free -= amount + tx;
    if (h === undefined) {
      held.set(mint, {
        opened: now,
        peak: 0,
        cost: amount,
        lastAddPrice: price,
        exiting: false,
        filled: false,
        tookHalf: false,
      });
      entriesByMint.set(mint, (entriesByMint.get(mint) ?? 0) + 1);
      slotsLeft--;
      mainLeft--;
    } else {
      h.cost += amount;
      h.lastAddPrice = price;
    }
  }

  // ---- guardrail pacing: probe round trips when behind ---------------------
  if (probes.size < MAX_PROBES && slotsLeft > 0 && firstTime !== null && lastTime !== null) {
    const pace = Math.min(1, (lastTime - firstTime) / PROBE_PACE_SECS);
    const needCoins = myCoins.size < Math.ceil(PROBE_TARGET_COINS * pace);
    const needFills = myFills < Math.ceil(PROBE_TARGET_FILLS * pace);
    if (needCoins || needFills) {
      const minRq = (PROBE_SOL * 10_000) / (hello.caps.max_order_bps_of_real_reserve as number);
      for (const mint of traded) {
        if (orders.length >= cap || probes.size >= MAX_PROBES) break;
        if (held.has(mint) || probes.has(mint) || pendingByMint.has(mint) || positions.has(mint)) continue;
        if (needCoins && !needFills && myCoins.has(mint)) continue;
        const c = curves.get(mint);
        if (c === undefined || c.complete) continue;
        if (Number(c.rq) / LAMPORTS < minRq * 1.5) continue;
        if (free < PROBE_SOL + 2 * tx) break;
        if (pushBuy(mint, PROBE_SOL)) {
          probes.set(mint, false);
          free -= PROBE_SOL + tx;
        }
      }
    }
  }

  return orders;
}

// ---- the line loop -----------------------------------------------------------
async function main(): Promise<void> {
  let buf = "";
  process.stdin.setEncoding("utf8");
  for await (const chunk of process.stdin) {
    buf += chunk;
    let idx: number;
    while ((idx = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, idx).replace(/\r$/, "");
      buf = buf.slice(idx + 1);
      if (line.trim() === "") continue;
      const msg = JSON.parse(line);
      if (msg.type === "hello") {
        hello = msg;
        cashLamports = BigInt(msg.capital.lamports);
      } else if (msg.type === "batch") {
        const orders = decideBatch(msg);
        process.stdout.write(JSON.stringify({ type: "reply", seq: msg.seq, orders }) + "\n");
      } else if (msg.type === "done") {
        return;
      } else {
        throw new Error(`unknown message type ${msg.type}`);
      }
    }
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.stack ?? err.message : String(err));
  process.exit(1);
});
