Feature: Policy refusals - a refused command becomes one fact for the policy's own source domain
  A policy's target, an aggregate or a process manager alike, refuses a
  policy's command the way it refuses any command: a gRPC business status
  carrying a TYPED refusal object of its own domain (one of the command's
  refuses types), under ANGZARR_REASON_COMMAND_REFUSED, and it records
  nothing (decisions 14.37 and 14.95: typed refusals only). There is ONE
  refusal path (decision 46 #5): the policy's coordinator calls
  PolicyService.Translate with the triggering event, the refused command as
  it was delivered (a ReactionDelivery) and the target's refusal (its
  status and typed object, read off the target's status). The policy's one
  refusal callback (decision 14.89) answers one fact of the policy's own
  source domain, payload only (TranslateResponse). The coordinator
  addresses it to the trigger's exact stream and stamps its identity, the
  refused command's id, as a ReactionDelivery in the fact arm. There is no
  pass-through, no standard CommandRefused fact, no source-side
  on_refused handler and no policy that declines to translate. Only a
  command takes this path: a fact a policy emits is never refused
  (policy_facts.feature; decision 14.69). See docs/design/policies.md §3,
  §4 and §6.

  Background:
    Given a "wallet" aggregate whose HoldFunds lists the refusal InsufficientFunds, declared FAILED_PRECONDITION, and refuses with it when funds are short
    And a process manager "BuyIn" owning domain "buy-in"
    And a policy "BuyInToWallet" translating from "buy-in" to "wallet" that builds HoldFunds from BuyInStarted
    And "BuyInToWallet"'s one refusal callback answers the "buy-in" fact HoldFundsDeclined for an InsufficientFunds refusal of HoldFunds

  # C-0613 (a refused command passed through as CommandRefused to the
  # source's generated on_refused_<Command> handler) is retired: there is
  # no pass-through, no CommandRefused and no source-side refusal dispatch;
  # the one path is Translate (decision 46 #5, 14.78). C-0614 and C-0833
  # state the one path.

  @policy_refusal
  @C-0614
  Scenario: A refused command is translated into a domestic fact by the policy's one callback
    Given wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1" under correlation "0192a6b0-0000-7000-8000-000000000021"
    Then wallet "p-1" refuses HoldFunds FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED, refusal_type InsufficientFunds, and records nothing
    And "BuyInToWallet"'s Translate is called once, with a trigger book holding only the BuyInStarted event, the HoldFunds ReactionDelivery as it was delivered, and a refusal of status REFUSAL_STATUS_FAILED_PRECONDITION whose typed object is that InsufficientFunds
    And Translate answers the HoldFundsDeclined payload alone
    And a HoldFundsDeclined fact is delivered to "buy-in" root "0192a6b0-0000-7000-8000-000000000021" as a ReactionDelivery in its fact arm
    And "BuyIn" handles it as an ordinary fact, with no reference to the wallet's protos

  @policy_refusal
  @C-0833
  Scenario: A refused policy command has one path, Translate, and nothing else
    # Decision 46 #5: one refusal path. The coordinator builds no refusal
    # fact of its own, the source dispatches nothing by the refused
    # command's type, and the policy cannot decline.
    Given wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then exactly one fact is delivered to "buy-in" for the refusal, and its payload is the one Translate answered
    And the coordinator builds no fact of its own from the refusal
    And no handler of "BuyIn" is chosen by the refused command's type HoldFunds

  @policy_refusal
  @C-0615
  Scenario: The refusal fact's id is the refused command's id
    Given wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1" under correlation "0192a6b0-0000-7000-8000-000000000022"
    Then the refusal fact's ReactionDelivery command_id is the refused HoldFunds' command_id
    And the fact origin "buy-in" derives has external_id that command_id's canonical text
    And the ReactionDelivery's causation names the BuyInStarted event
    And its correlation_id is "0192a6b0-0000-7000-8000-000000000022"
    And its component is "BuyInToWallet"
    And wallet "p-1"'s stream holds no event with that command_id

  @policy_refusal
  @C-0616
  Scenario: The refusal fact replaces the command's outbox entry in one transaction
    Given wallet "p-1" has too little available to hold 500
    And the policy's coordinator crashes after Translate answers and before its transaction commits
    When the coordinator restarts
    Then HoldFunds is delivered again and refused again
    And "BuyIn" records the refusal fact once

  @policy_refusal
  @C-0617
  Scenario Outline: Only a business rejection reaches Translate, and the status decides which is one
    # Decision 14.37 (owner rulings, 2026-10-29): the gRPC status decides
    # the class. FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND,
    # ALREADY_EXISTS, OUT_OF_RANGE and PERMISSION_DENIED, carrying a typed
    # refusal, are business refusals, never retried; ABORTED, a handler's
    # own or a typed refusal declaring it (decision 14.111), is an
    # immediate-return business error, whose policy step the runtime
    # reloads and re-runs under the capped backoff; INTERNAL, UNKNOWN and
    # DATA_LOSS are fatal bugs, dead-lettered at once; the rest are
    # retryable bugs, retried with backoff and jitter up to a configured
    # cap, then dead-lettered (docs/design/command-handling.md §3).
    Given wallet "p-1" answers HoldFunds with <status>
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then "BuyInToWallet"'s Translate <translate>
    And <outcome>

    Examples:
      | status                                                        | translate                  | outcome                                                                                                      |
      | UNAVAILABLE                                                   | is not called              | HoldFunds is retried with backoff up to its configured cap, then dead-lettered and alerted                   |
      | ABORTED                                                       | is not called              | the policy step is reloaded and re-run with backoff up to its configured cap, then dead-lettered and alerted |
      | ABORTED with a typed refusal the command lists                | is not called              | the policy step is reloaded and re-run with backoff up to its configured cap, then dead-lettered and alerted |
      | INVALID_ARGUMENT with a typed refusal the command lists       | is called with the refusal | HoldFunds is neither retried nor dead-lettered                                                               |
      | INTERNAL                                                      | is not called              | HoldFunds is dead-lettered and an alert is raised                                                            |

  @policy_refusal
  @C-0618
  Scenario: A hand-written policy with no refusal callback answers a refusal as a bug
    # Only a policy written by hand can lack the callback: a generated one
    # has it, required (decisions 14.76 and 14.89; reason-keys.md §8.10).
    Given "BuyInToWallet" is written by hand and its router has no refusal callback
    And wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then "BuyInToWallet"'s Translate is refused INTERNAL with reason ANGZARR_REASON_NO_REFUSAL_HANDLER, naming command_type HoldFunds
    And the policy step is dead-lettered and alerted at the policy
    And nothing is recorded in "buy-in" or "wallet"

  @policy_refusal
  @C-0619
  Scenario: A failing refusal callback is dead-lettered and alerted
    Given "BuyInToWallet"'s refusal callback fails with INTERNAL
    And wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then the policy step is dead-lettered and an alert is raised
    And nothing is recorded in "buy-in"

  @policy_refusal
  @C-0834
  Scenario: A Translate answer with no payload is refused by the source's router
    # docs/design/policies.md §6: a fact with no type URL is delivered like
    # any other, and the source's router refuses it.
    Given "BuyInToWallet"'s Translate answers a fact with no payload
    And wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then the refusal fact is refused INTERNAL by "buy-in"'s router with reason ANGZARR_REASON_NO_FACT_HANDLER
    And it is dead-lettered and alerted at "buy-in"
    And nothing is recorded in "buy-in"

  # C-0621 (a refusal fact the callback addressed to another stream,
  # refused as malformed) is retired: Translate answers a payload alone
  # (TranslateResponse.fact), so a callback cannot address the fact; the
  # coordinator delivers it to the trigger's exact stream (C-0614, C-0615;
  # decisions 14.42, 14.78).

  @policy_refusal
  @C-0622
  Scenario: The mechanism is the same when the policy's target is a process manager
    Given a process manager "Fulfillment" owning domain "fulfillment" whose RecordStockReserved lists the refusal AlreadyCompleted and refuses with it once completed
    And a policy "InventoryToFulfillment" translating from "inventory" to "fulfillment" whose refusal callback answers the "inventory" fact ReservationUnwanted for AlreadyCompleted
    When "Fulfillment" refuses the RecordStockReserved built from StockReserved of item "sku-1"
    Then "Fulfillment" records nothing
    And "InventoryToFulfillment"'s Translate is called with the AlreadyCompleted refusal
    And a ReservationUnwanted fact is delivered to "inventory" item "sku-1"

  @policy_refusal
  @C-0624
  Scenario: A command handler's typed refusal object reaches the policy's Translate, packed, with its status
    Given wallet "p-1" has 120 available and is asked to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then wallet "p-1"'s HoldFunds is refused FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED, refusal_type InsufficientFunds, and the InsufficientFunds object with available 120 and requested 500 in the status details
    And "BuyInToWallet"'s Translate receives a refusal whose status is REFUSAL_STATUS_FAILED_PRECONDITION and whose typed object unpacks to InsufficientFunds with available 120 and requested 500
    And the refusal carries no text

  @policy_refusal
  @error_keys
  @C-0674
  Scenario: A target command that lists no refusal cannot refuse, so nothing reaches Translate
    # Decision 14.95: typed refusals only. A command whose refuses names
    # nothing cannot refuse; a domain's own key travels only as ABORTED or
    # a bug, never to Translate. The handler's rejection under its own
    # untyped key is a bug, ANGZARR_REASON_UNHANDLED_HANDLER_ERROR.
    Given the "wallet" aggregate's CloseHold lists no refusal
    And a policy "BuyInToWallet" that builds CloseHold from BuyInCancelled
    And wallet "p-1"'s CloseHold handler fails with its own key ACCOUNT_CLOSED and arg wallet "p-1"
    When "BuyIn" records BuyInCancelled for wallet "p-1"
    Then CloseHold is answered INTERNAL with reason ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, cause ACCOUNT_CLOSED and cause.wallet "p-1"
    And "BuyInToWallet"'s Translate is not called
    And the CloseHold delivery is dead-lettered and alerted

  @policy_refusal
  @C-0835
  Scenario: A reaction refused as not forked in its edition never reaches Translate
    # docs/design/edition-meta-events.md §2: the framework's key, not a
    # typed refusal, so it is a failed delivery (decision 14.107).
    Given "buy-in" root "0192a6b0-0000-7000-8000-000000000024" is forked in edition "what-if" and wallet "p-1" is not
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1" in edition "what-if" under correlation "0192a6b0-0000-7000-8000-000000000024"
    Then HoldFunds is refused FAILED_PRECONDITION with reason ANGZARR_REASON_EDITION_NOT_FORKED, stream wallet "p-1" in edition "what-if" and edition "what-if"
    And "BuyInToWallet"'s Translate is not called
    And the HoldFunds delivery is dead-lettered with that reason and alerted
    And nothing is recorded in "buy-in" or "wallet"

  # C-0685 (a translated fact that set its own cover, handling, page or
  # command_id refused under its retired refusal-fact keys) is
  # retired: TranslateResponse holds the fact's payload alone, so none of
  # those faults can be represented and the keys are gone (decision 14.78).
