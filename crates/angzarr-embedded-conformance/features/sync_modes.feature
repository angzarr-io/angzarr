Feature: Command modes - how much of a command's processing the caller waits for
  How much the caller waits for is a call-level choice, never part of the
  command: one coordinator RPC per mode (docs/design/command-handling.md
  §1). Every mode persists the decision's events before answering (or
  answers the rejection), and every mode publishes the persisted events
  downstream.

  - Handle (DECISION): accept or reject synchronously; answers a
    CommandResult { events, delivery } (delivery APPLIED or DUPLICATE;
    decision 14.105). Projectors and policies run asynchronously.
  - HandleAsync (ASYNC): validate, assign ids, enqueue and return
    google.protobuf.Empty; nothing about the decision comes back.
  - HandleSync (SIMPLE): takes a HandleSyncRequest { command, projectors },
    the registered names of the projectors to wait for (decision 14.107;
    no projector carries a synchronous mode), and answers a
    CommandSyncResult { events, delivery, projections }, one projection per
    named projector. The list is required: an empty one is
    INVALID_ARGUMENT ANGZARR_REASON_SYNC_NO_PROJECTORS, and a named
    projector not subscribed to the command's domain, an unknown name
    included, is INVALID_ARGUMENT
    ANGZARR_REASON_SYNC_PROJECTOR_NOT_SUBSCRIBED (args domain, projector),
    both before anything runs. A name listed twice is waited for once. A
    named projector that fails after the decision commits fails the call
    UNAVAILABLE ANGZARR_REASON_SYNC_PROJECTION_FAILED (args stream,
    projector, cause): the decision stays committed, there is no partial
    answer, and a retry with the same command_id deduplicates (DUPLICATE),
    answers the same events and waits on the named projectors again
    (decision 14.105).

  No mode waits for reactions: policies always run asynchronously. The
  sync_mode field (CommandRequest.sync_mode, PageHeader.sync_mode) is gone:
  the RPC is the mode. CASCADE, ISOLATED and the cascade error modes are
  deleted (decision 2.1, backlog RM-1); their scenarios (C-0430, C-0431,
  C-0432, C-0433) are retired.

  Background:
    Given an "order" aggregate that accepts CreateOrder, and whose CancelOrder names the refusal OrderUnknown, declared FAILED_PRECONDITION, refused for an unknown order
    And a projector "OrderSummary" subscribed to "order"
    And a projector "OrderAudit" subscribed to "order"
    And a projector "StockLevels" subscribed to "inventory" only
    And a policy "OrderFulfillment" translating OrderCreated into a ReserveStock command for "inventory"

  @C-0425
  Scenario: HandleAsync returns once the command is enqueued, with nothing of the decision
    When a CreateOrder command is sent through HandleAsync
    Then the answer is google.protobuf.Empty
    And the command is processed later, its OrderCreated event persisted
    And the OrderCreated event is published to the bus
    And the OrderSummary projector and the OrderFulfillment policy receive it from the bus

  @C-0426
  Scenario: Handle answers a CommandResult with the events and the delivery
    When a CreateOrder command is sent through Handle
    Then the answer is a CommandResult holding the OrderCreated event with delivery APPLIED
    And the CommandResult has no projections

  @C-0427
  Scenario: Handle surfaces acceptance synchronously and defers downstream work
    When a CreateOrder command is sent through Handle
    Then the answer is a CommandResult holding the OrderCreated event
    And the OrderSummary projector has not necessarily run when the answer is returned
    And the OrderCreated event is published to the bus

  @C-0428
  Scenario: Handle surfaces a refusal synchronously
    When a CancelOrder command for an unknown order is sent through Handle
    Then the command fails FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED and refusal_type naming OrderUnknown
    And no events are persisted

  @C-0429
  Scenario: HandleSync waits for the projectors it names, and only for them
    When a CreateOrder command is sent through HandleSync naming the projector "OrderSummary"
    Then the answer is a CommandSyncResult holding the OrderCreated event with delivery APPLIED
    And its projections hold exactly one Projection, projector "OrderSummary", covering the OrderCreated event
    And the OrderAudit projector is not waited for
    And the OrderFulfillment policy has not run when the answer is returned
    And the OrderCreated event is published to the bus

  @C-0808
  Scenario: HandleSync answers one projection per named projector, a name listed twice waited for once
    When a CreateOrder command is sent through HandleSync naming the projectors "OrderSummary", "OrderAudit" and "OrderSummary"
    Then the answer is a CommandSyncResult holding the OrderCreated event
    And its projections hold exactly one Projection for "OrderSummary" and one for "OrderAudit"
    And the OrderSummary projector projects the OrderCreated event once

  @C-0809
  Scenario: HandleSync naming no projector is refused before anything runs
    # Decision 14.107: a caller that waits for no projector calls Handle;
    # an empty list would be a second path to it.
    When a CreateOrder command is sent through HandleSync naming no projectors
    Then the call fails INVALID_ARGUMENT with reason ANGZARR_REASON_SYNC_NO_PROJECTORS and no args
    And business code is not invoked
    And no events are persisted

  @C-0810
  Scenario Outline: HandleSync naming a projector not subscribed to the command's domain is refused before anything runs
    When a CreateOrder command is sent through HandleSync naming the projectors "OrderSummary" and "<projector>"
    Then the call fails INVALID_ARGUMENT with reason ANGZARR_REASON_SYNC_PROJECTOR_NOT_SUBSCRIBED, arg domain "order" and arg projector "<projector>"
    And business code is not invoked
    And no events are persisted

    Examples:
      | projector       |
      | StockLevels     |
      | NoSuchProjector |

  # The scenarios that were here (C-0434, C-0435, C-0507, C-0508, C-0512)
  # had a process manager emit reaction commands with their own
  # PageHeader.sync_mode. A process manager sends no commands (decision
  # 14.31), so they are retired. A policy's reactions are delivered one
  # way only, through the transient outbox and Deliver, answered as Handle
  # (decisions 14.79 and 14.107).

  @error_keys
  @C-0676
  Scenario: A refusal reaches the caller as its status, key and typed object
    # Decisions 14.37 and 14.95: typed refusals only; the status message is
    # the reason key, and the coordinator adds no text of its own.
    Given the CancelOrder handler refuses an unknown order with an OrderUnknown refusal with order "o-9"
    When a CancelOrder command for order "o-9" is sent through Handle
    Then the command fails FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED, which is also its status message
    And the status's error info carries refusal_type naming OrderUnknown
    And the status details hold the OrderUnknown refusal with order "o-9"

  @error_keys
  @C-0677
  Scenario: A named projector that fails after the decision commits fails HandleSync with a retryable key
    # Decision 14.105: no partial answer; the decision stays committed.
    Given the projector "OrderSummary" fails UNAVAILABLE with reason ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, its cause VIEW_STORE_UNAVAILABLE
    When a CreateOrder command with command_id "0192f3a4-5b6c-7d8e-9f01-23456789ac77" is sent through HandleSync naming the projector "OrderSummary"
    Then the call fails UNAVAILABLE with reason ANGZARR_REASON_SYNC_PROJECTION_FAILED, arg stream naming the order stream, arg projector "OrderSummary" and arg cause ANGZARR_REASON_UNHANDLED_HANDLER_ERROR
    And the failure carries no text
    And the OrderCreated event stays persisted
    And nothing is dead-lettered

  @error_keys
  @C-0811
  Scenario: A retry after a failed synchronous projection deduplicates and waits on the projectors again
    Given a CreateOrder command with command_id "0192f3a4-5b6c-7d8e-9f01-23456789ac77" was sent through HandleSync naming the projector "OrderSummary", committed, and failed ANGZARR_REASON_SYNC_PROJECTION_FAILED
    And the projector "OrderSummary" now projects successfully
    When the caller resends the command with the same command_id through HandleSync naming the projector "OrderSummary"
    Then business code is not invoked
    And the answer is a CommandSyncResult holding the same OrderCreated event with delivery DUPLICATE
    And its projections hold the "OrderSummary" Projection covering the OrderCreated event
