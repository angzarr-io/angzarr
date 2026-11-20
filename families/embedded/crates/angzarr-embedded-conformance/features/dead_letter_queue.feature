Feature: Dead letter queue routing
  Messages the framework cannot process and must not drop are published as
  AngzarrDeadLetter to the per-domain topic "angzarr.dlq.{domain}", where
  domain is the domain of the dead letter's payload's cover. A dead letter
  is its payload and its one statement of why (decisions 14.78, 14.101 and
  14.107; docs/design/dead-letters.md):

  - the payload, one of: rejected_command (a client's command or fact, its
    ids assigned), rejected_reaction (a policy's output or refusal fact, as
    delivered; decision 14.79), rejected_events (the events a policy or
    projector failed on), or failed_edition_removal (the stream's Cover);
  - why, exactly one of: refusal (a typed refusal of an asynchronous
    command, packed; its key implied, ANGZARR_REASON_COMMAND_REFUSED, its
    type the Any's type URL, its args its fields), reason (every other
    failure: a Diagnostic, a key and its args, never rendered text;
    decision 14.37), or transport (a transport's own why, packed, its key
    implied by its type: today the distributed transport's
    PayloadRetrievalFailed, key DISTRIBUTED_REASON_PAYLOAD_UNAVAILABLE);
  - retry_count (the retries after the first attempt), occurred_at, and
    the producing component's name and kind (source_component,
    source_component_kind: COMPONENT_KIND_AGGREGATE, _POLICY,
    _PROCESS_MANAGER or _PROJECTOR).

  Whether a failure was transient is read from the reason key's declared
  status, never stored. The distributed transport's DeadLetterEnvelope
  wraps the entry with the captured stack trace only (decision 14.107: no
  metadata, and a claim check's retrieval failure is the transport arm).

  Sources of dead letters:
  - asynchronous commands refused, by the coordinator's expected-sequence
    check or by business code with a typed refusal (the refusal arm);
  - work with no caller waiting, and facts, at the race re-run cap
    (ANGZARR_REASON_RETRY_CAP_EXHAUSTED);
  - retryable bugs at the retry cap, under their own key as last answered;
  - policy and projector handler failures, once retries are exhausted or
    at once for a fatal bug;
  - claim-check payloads that cannot be retrieved, at the retry cap (the
    transport arm).

  Compensation is no longer part of the contract (RM-2), so its sources are
  gone: C-0448 (a compensation handler's RevocationResponse sending the
  rejected command to the dead letter queue) is retired.

  @C-0443
  Scenario: A queued command with a stale expected sequence is dead-lettered with the coordinator's refusal
    Given an "order" aggregate "order-1" at next_sequence 3
    When a ChangeShippingAddress command with expected sequence 1 and correlation_id "0192f3a4-5b6c-7d8e-9f01-23456789ab07" is sent asynchronously
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's command is addressed to domain "order" root "order-1" with correlation_id "0192f3a4-5b6c-7d8e-9f01-23456789ab07"
    And the dead letter's why is the refusal arm, an ExpectedSequenceMismatch with expected 1 and head 3
    And the dead letter's source_component_kind is COMPONENT_KIND_AGGREGATE
    And the dead letter has occurred_at set

  @C-0738
  Scenario: A command whose business process cannot be reached is dead-lettered at the retry cap with the unavailable key
    # Decision 14.49: a retryable bug at its cap records its own key, as
    # last answered (14.48, amended), never ANGZARR_REASON_RETRY_CAP_EXHAUSTED.
    Given an "order" aggregate "order-1" at next_sequence 3
    And the retry cap is reached after 3 attempts
    And the business process handling "order" cannot be reached, its connection refused
    When an AddItem command with no expected sequence is sent asynchronously and the coordinator processes it
    Then the coordinator attempts the delivery 3 times, backing off between attempts
    And a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's why is the reason arm, code ANGZARR_REASON_HANDLER_UNAVAILABLE, arg stream "order/order-1", arg domain "order" and arg cause
    And the dead letter's retry_count is 2
    And an alert is raised
    And no events are persisted

  @C-0744
  Scenario: A command whose event store keeps failing is dead-lettered at the retry cap with the store key
    # Decision 14.51: a retryable failure carries a key, so the dead letter
    # at the cap records it, as last answered, never
    # ANGZARR_REASON_RETRY_CAP_EXHAUSTED (14.48, amended).
    Given an "order" aggregate "order-1" at next_sequence 3
    And the retry cap is reached after 3 attempts
    And the "order" event store fails every write, its connection dropped
    When an AddItem command with no expected sequence is sent asynchronously and the coordinator processes it
    Then the coordinator attempts the command 3 times, backing off between attempts
    And a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's why is the reason arm, code ANGZARR_REASON_STORE_UNAVAILABLE and arg cause
    And the dead letter's retry_count is 2
    And an alert is raised
    And no events are persisted

  # C-0739 (a decision past the router's step limit dead-lettered under the
  # step-limit key) is retired: Emit has no step cap and no key
  # for one; a step that always continues is a handler bug with no framework
  # guard (decision 14.104; docs/design/command-handling.md §9).

  @C-0444
  Scenario: A policy that keeps failing is dead-lettered with the events it could not process
    # Decision 14.95: a handler's own key travels as the cause of
    # ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, under the retryable status it
    # picks (C-0678); at the cap the dead letter records it as last answered.
    Given a policy "OrderFulfillment" subscribed to "order" whose handler fails UNAVAILABLE with reason ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, its cause INVENTORY_UNAVAILABLE
    And the policy's retry cap is 3 attempts
    When an OrderCreated event for "order-1" is delivered to the policy
    Then the policy handler is attempted 3 times
    And a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the OrderCreated event as rejected_events
    And the dead letter's why is the reason arm, code ANGZARR_REASON_UNHANDLED_HANDLER_ERROR and arg cause INVENTORY_UNAVAILABLE
    And the dead letter's retry_count is 2
    And the dead letter's source_component is "OrderFulfillment" with source_component_kind COMPONENT_KIND_POLICY

  @C-0445
  Scenario: A projector that keeps failing is dead-lettered
    Given a projector "OrderSummary" subscribed to "order" whose handler fails UNAVAILABLE with reason ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, its cause VIEW_STORE_UNAVAILABLE
    When an OrderCreated event for "order-1" is delivered to the projector and its retries are exhausted
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the OrderCreated event as rejected_events
    And the dead letter's source_component is "OrderSummary" with source_component_kind COMPONENT_KIND_PROJECTOR

  @C-0446
  Scenario: A fatal handler failure is dead-lettered without retries
    # Decision 14.37: a bug under INTERNAL is fatal, never retried.
    Given a policy "OrderFulfillment" subscribed to "order" whose handler fails INTERNAL with reason ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, its cause UNKNOWN_EVENT_VERSION
    When an OrderCreated event for "order-1" is delivered to the policy
    Then the policy handler is attempted once
    And a dead letter is published to "angzarr.dlq.order"
    And the dead letter's why is the reason arm, code ANGZARR_REASON_UNHANDLED_HANDLER_ERROR and arg cause UNKNOWN_EVENT_VERSION
    And the dead letter's retry_count is 0

  @C-0447
  Scenario: An unretrievable claim-check payload is dead-lettered at the retry cap with the transport's own why
    # Decisions 14.51 and 14.107: the retrieval failure is the dead letter's
    # why, the transport arm, a PayloadRetrievalFailed whose type implies
    # DISTRIBUTED_REASON_PAYLOAD_UNAVAILABLE; the envelope adds nothing to it.
    Given the retry cap is reached after 3 attempts
    And an OrderCreated event for "order-1" whose payload is claim-checked at "s3://payloads/abc.bin"
    And the payload store cannot return "s3://payloads/abc.bin"
    When the event is delivered to the policy "OrderFulfillment"
    Then the coordinator attempts the retrieval 3 times, backing off between attempts
    And a dead letter is published to "angzarr.dlq.order"
    And the dead letter's why is the transport arm, a PayloadRetrievalFailed whose reference has uri "s3://payloads/abc.bin" and whose failure is the payload store's code and args
    And the dead letter's why has no reason arm and no refusal arm
    And the dead letter's retry_count is 2
    And the dead letter envelope carries the dead letter and its stack trace, and nothing else
    And an alert is raised

  @C-0449
  Scenario: A dead letter is not published when processing succeeds
    Given a policy "OrderFulfillment" subscribed to "order" whose handler succeeds
    When an OrderCreated event for "order-1" is delivered to the policy
    Then no dead letter is published

  @C-0673
  Scenario: A dead letter carries a key and its args, never rendered text
    # Decision 14.37: text is rendered only where it is displayed, from a
    # catalog keyed by the code.
    Given a policy "OrderFulfillment" subscribed to "order" whose handler fails INTERNAL with reason ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, its cause INVENTORY_UNAVAILABLE with arg sku "sku-1", and logs "inventory service down" for itself
    When an OrderCreated event for "order-1" is delivered to the policy
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter's why is the reason arm, code ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, arg cause INVENTORY_UNAVAILABLE and arg cause.sku "sku-1"
    And no field of the dead letter carries the text "inventory service down"

  @C-0730
  Scenario: An asynchronous command ahead of the stream is dead-lettered with the refusal arm
    # Decision 14.94: one refusal, ahead or behind; decision 14.101: the
    # refusal arm, its key implied.
    Given an "order" aggregate "order-1" at next_sequence 3
    When a ChangeShippingAddress command with expected sequence 7 is sent asynchronously
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's why is the refusal arm, an ExpectedSequenceMismatch with expected 7 and head 3
    And the refusal's type URL names io.angzarr.v1.ExpectedSequenceMismatch
    And the dead letter's why has no reason arm

  @C-0731
  Scenario: An asynchronous command behind the stream is dead-lettered with the same refusal
    Given an "order" aggregate "order-1" at next_sequence 3
    When a ChangeShippingAddress command with expected sequence 1 is sent asynchronously
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command, its Handling as sent with expected sequence 1
    And the dead letter's why is the refusal arm, an ExpectedSequenceMismatch with expected 1 and head 3
    And the dead letter's why has no reason arm

  @C-0732
  Scenario: An asynchronous command dead-lettered at the race re-run cap carries the cap key and its retry count
    # Decision 14.48: retry_count is the retries, the attempts arg less the
    # first run; the key is for write races only.
    Given an "order" aggregate "order-1" at next_sequence 3
    And the race re-run cap is 3
    And an AddItem command with no expected sequence is sent asynchronously
    And the business process answers every run with an ItemAdded event
    And another write to the "order" stream lands after each run's load and before the command is written
    When the coordinator processes the command
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's why is the reason arm, code ANGZARR_REASON_RETRY_CAP_EXHAUSTED, arg stream "order/order-1", arg attempts "3" and arg cap "3"
    And the dead letter's retry_count is 2
    And an alert is raised

  @C-0733
  Scenario: An asynchronous command refused with a typed refusal keeps the refusal object as its why
    # Decision 14.101: the refusal arm holds the object whole, as the wire
    # carried it; its type is the Any's own type URL, its key implied.
    Given a "wallet" aggregate whose HoldFunds names the refusal InsufficientFunds, declared FAILED_PRECONDITION, and refuses with InsufficientFunds with available 200 and requested 500 when funds are short
    When a HoldFunds command for 500 on wallet "p-1" is sent asynchronously
    Then a dead letter is published to "angzarr.dlq.wallet"
    And the dead letter carries the rejected command
    And the dead letter's why is the refusal arm, the InsufficientFunds refusal object packed, with available 200 and requested 500
    And the refusal's type URL names InsufficientFunds
    And the dead letter's why has no reason arm
