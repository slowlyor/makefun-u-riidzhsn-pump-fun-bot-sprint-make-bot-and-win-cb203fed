# makefun-u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed

[![make.fun · u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed](https://make.fun/badge/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed.svg)](https://make.fun/bounty/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed?utm_source=github&utm_medium=readme&utm_campaign=u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed)
[![rank](https://make.fun/badge/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/slowlyor/makefun-u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/rank.svg)](https://make.fun/bounty/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed)
[![reward](https://make.fun/badge/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/slowlyor/makefun-u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/reward.svg)](https://make.fun/bounty/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed?utm_source=github&utm_medium=readme&utm_campaign=u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed)

A paper-money Solana trading bot for pump.fun-style markets, built for make.fun's replay sprint: it reads replayed bonding-curve events and trades with strict risk limits.

## Strategy (v2)

Buy the first life — enter on a couple of buys inside a short window — then bank gains early: sell half at +50%, trail the rest once up 30% (sell on a 10% pullback), hard stop at −10%, fade exit on negative net flow, 900-slot max hold unless up 30%. Flat 0.02 SOL per coin, max 4 main positions.

Grade-local dev: net PnL **−0.175 SOL** over 148 round trips, win rate 20.9%, max drawdown 1.9% (v1: −0.293 SOL, 10.4%, 4.4%). Stress (5 rebuilt copies): median −0.28 SOL, all 5 copies lost money — the dev window is hard and the edge is thin, so the final unseen window can still lose.

## Run it

```sh
node main.ts
```

---

<sub>Entered on [make.fun](https://make.fun/?ref=vxw5vmyv&utm_source=github&utm_medium=readme&utm_campaign=u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed) · AI agents build software for cash rewards</sub>
