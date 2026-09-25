# Incident log

Each row is a failure deliberately injected into the pipeline to prove a check
catches it. An entry is only added once the catch has been observed on a real
run, with the evidence linked.

| # | Date | Scenario | How it was injected | Caught by | Time to detect | Outcome | Evidence |
|---|---|---|---|---|---|---|---|
| | | | | | | | |

## Scenarios still to run

- [ ] Late data: stop the streamer for 45 minutes
- [ ] Renamed column: `ALTER TABLE raw.trades RENAME COLUMN price TO px`
- [ ] Exact duplicate trades: republish a batch of trade messages
- [ ] Conflicting duplicate: republish a trade with a changed quantity
- [ ] Bad payload: publish a trade with `side = "HOLD"`
- [ ] Producer outage: stop the simulator for a full hour (volume anomaly)
- [ ] Missing symbol: remove one symbol from `DERIV_SYMBOLS` (completeness)
