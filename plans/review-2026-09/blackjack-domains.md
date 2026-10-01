# Blackjack example — approved domain design

Supersedes §2 of `blackjack-plan.md` where they differ. Key goal: the example
exercises every substantial angzarr capability. Framework decisions in force:
saga/PM commands are deferred with no expected version (dedupe by provenance);
no 2PC; no basis_seq; no destination_sequences; compensation = Notification to
HandleCompensation via a durable coordinator outbox; RETENTION_DEFAULT behaves
like TRANSIENT; *_UNSPECIFIED = 0 enums.

## 1. `player` — wallet (APPROVED)

Owns all money off the table. A hold earmarks part of the bankroll; it never
moves money.

| Command | Sender / mode | Behaviour | Capability |
|---|---|---|---|
| RegisterPlayer{display_name, email} | client | create wallet (tags 1/2 kept) | aggregate basics, PLAYER_ALREADY_EXISTS |
| UpdateProfile{display_name} | client | rename | COMMUTATIVE partner (disjoint fields) |
| DepositFunds{amount} | client, MERGE_COMMUTATIVE | bankroll += amount | stale deposit merges vs UpdateProfile, retryable reject vs concurrent deposit; SIMPLE sync |
| WithdrawFunds{amount} | client, MERGE_MANUAL | bankroll −= amount, never held funds | stale withdrawal → DLQ for human review |
| RequestTopUp{table_root, amount, request_id} | client | hold + TopUpRequested | idempotent client request ids |
| HoldFunds / CaptureFunds / ReleaseHold{hold_id …} | buy-in PM, SYNC_MODE_DECISION | hold lifecycle | deferred PM commands, provenance dedupe, DECISION |
| ImportPlayer{display_name, email} | admin migration, SYNC_MODE_ISOLATED | identity only, no balance, no reactions | ISOLATED |

Facts (from saga-table-player, external_id dedupe, through handle_fact which
validates and flags but cannot reject): TopUpSettled{hold_id, amount} spends a
hold; CashOutCredited{cashout_id, amount} credits the bankroll.

Compensation: AddChips rejected by the table → outbox-delivered Notification →
player compensation handler emits TopUpRefused (hold released).

Events: PlayerRegistered, PlayerImported, ProfileUpdated, FundsDeposited
(+ legacy FundsDepositedV1 for the upcaster), FundsWithdrawn, FundsHeld,
FundsCaptured, HoldReleased, TopUpRequested, TopUpRefused, TopUpSettled,
CashOutCredited.

Invariant L1: bankroll ≥ Σholds ≥ 0; each hold > 0;
bankroll = deposited − withdrawn − to_tables + from_tables.

Player additions (approved with table, for CascadeErrorMode coverage):
EnrollLoyalty{} → LoyaltyEnrolled; RecordRoundResult{table_root, round, net}
(from saga-table-player-history) → RoundResultRecorded, compensated by
RoundResultRetracted; AwardLoyaltyPoints{table_root, round, points} (from
saga-table-player-loyalty) → LoyaltyPointsAwarded, rejected LOYALTY_NOT_ENROLLED.

## 2. `table` — seats, shoe, round, settlement, house (APPROVED)

Owns all chips on the table. The round lifecycle is in-aggregate
(multi-event commands; the last action emits DealerPlayed + RoundSettled).

| Command | Sender / mode | Behaviour | Capability |
|---|---|---|---|
| CreateTable{limits, seats, decks, shoe_seed} | client | TableCreated + ShoeShuffled | multi-event |
| RequestSeat{player_root, seat, amount, request_id} | client, CASCADE | SeatHeld → buy-in PM | CASCADE returns when seated |
| ConfirmSeat / ReleaseSeat{buy_in_id} | buy-in PM, DECISION | PlayerSeated / SeatReleased | PM-driven commands |
| AddChips{player_root, hold_id, amount} | saga-player-table | ChipsAdded; WAGER_IN_PLAY mid-round | saga rejection → source compensation |
| LeaveTable{seat} | client, CASCADE | PlayerCashedOut (deterministic cashout_id) | cash-out fact |
| PlaceBet{seat, amount} | client, MERGE_AGGREGATE_HANDLES | BetPlaced; handler rejects same-seat double bet | AGGREGATE_HANDLES |
| DealRound | client | [ShoeShuffled] + RoundDealt [+ DealerPlayed + RoundSettled] | multi-event, recorded shoe |
| Hit / Stand / DoubleDown{seat} | client, explicit sequence, MERGE_STRICT | turn events; last action settles | concurrent hits: exactly one lands |

Events: TableCreated, ShoeShuffled, SeatHeld, SeatReleased, PlayerSeated,
ChipsAdded, PlayerCashedOut, BetPlaced, RoundDealt, CardDealt, HandStood,
HandDoubled, DealerPlayed, RoundSettled.

Snapshots: periodic DEFAULT; snapshot at each ShoeShuffled marked PERSIST.
Editions: what-if replay of a round. No speculative execution on table (shoe leak).

Cascade error modes: RoundSettled (final action sent with CASCADE) fans out to
saga-table-player-history (RecordRoundResult; COMPENSATE → RoundResultRetracted)
and saga-table-player-loyalty (AwardLoyaltyPoints; LOYALTY_NOT_ENROLLED is the
deterministic failure). No money in this fan-out.

Invariant L2: Σstacks + Σwagers + house_result = chips_in − chips_out after every event.

## 3. `pmg-buy-in` — buy-in process manager (APPROVED)

Only PM: secures a seat (table) and funds (player), confirming or releasing
each based on the other. State keyed by correlation_id (core-derived root):
buy_in_id, player, table, seat, amount, phase AwaitingHold → AwaitingSeat →
AwaitingCapture → Completed | Failed.

All emitted commands: angzarr_deferred, SYNC_MODE_DECISION, correlation_id
left empty for core, type URLs from generated code only.

| Trigger | Phase | PM event | Sends |
|---|---|---|---|
| table SeatHeld | new → AwaitingHold | BuyInStarted | player HoldFunds |
| player FundsHeld | AwaitingHold → AwaitingSeat | BuyInFundsHeld | table ConfirmSeat |
| table PlayerSeated | AwaitingSeat → AwaitingCapture | BuyInSeated | player CaptureFunds |
| player FundsCaptured | AwaitingCapture → Completed | BuyInCompleted | — |
| HoldFunds rejected | AwaitingHold → Failed | BuyInFailed | table ReleaseSeat |
| ConfirmSeat rejected | AwaitingSeat → Failed | BuyInFailed | player ReleaseHold |

Rejections arrive as outbox-delivered Notifications to the PM's compensation
handlers. Every handler guards buy_in_id == state.buy_in_id and phase. No
business decisions in the PM. L3: each PlayerSeated(B, X) ⇔ one FundsCaptured(B, X).
PM scope confirmed: buy-in only. Departure is saga + fact (the table's decision
is final; outbox + cashout_id dedupe make it durable).

## 4. Sagas (APPROVED)

Stateless translators; target roots come from source-event fields, never
lookups; commands deferred; facts where the destination cannot refuse.

| Saga | Listens to | Emits | Kind | Shows |
|---|---|---|---|---|
| saga-player-table | player TopUpRequested | table AddChips{player_root, hold_id, amount} | command | rejection (WAGER_IN_PLAY) → outbox compensation → player TopUpRefused |
| saga-table-player-settlement | table ChipsAdded, PlayerCashedOut | player TopUpSettled{hold_id}, CashOutCredited{cashout_id} | facts (external_id = hold_id / cashout_id) | fact injection, external-id dedupe, handle_fact validation |
| saga-table-player-history | table RoundSettled | player RecordRoundResult per seat | command | CASCADE fan-out reaction 1; COMPENSATE → RoundResultRetracted |
| saga-table-player-loyalty | table RoundSettled | player AwardLoyaltyPoints per seat | command | fan-out reaction 2; LOYALTY_NOT_ENROLLED drives the error modes |

L3 (rest): each ChipsAdded(H, X) ⇔ one TopUpSettled(H, X); each
PlayerCashedOut(K, Y) ⇔ one CashOutCredited(K, Y). RoundSettled feeds two
sagas + the projector (one event, several consumers).

## 5. `projector-player-table-ledger` + upcaster (APPROVED)

Subscribes to player + table (multi-domain). Read model keyed by (root,
sequence) for idempotent replay: per player (bankroll, held, loyalty points,
recent round results); per table (stacks, wagers, house_result, chips_in,
chips_out); global (deposits, withdrawals, in_flight between PlayerSeated →
FundsCaptured and ChipsAdded → TopUpSettled).

LedgerQueryService: GetPlayerBalance (found flag); GetLedger (totals +
balanced, true when in_flight == 0 and L4 holds:
Σbankroll + Σ(stacks + wagers) + Σhouse_result = deposits − withdrawals).
Live view via the core event-stream service filtered by correlation_id.
SIMPLE deposit returns after this projector applied it.

Upcaster (served by the player aggregate binary): FundsDepositedV1{amount_chips}
→ FundsDeposited{amount}; test seeds a legacy event and asserts replay.

Invariant tiers: L1/L2 unit property tests; L3 in-process session scenario +
cluster correlation query; L4 cluster: projector `balanced` AND an independent
recompute from EventQuery.
