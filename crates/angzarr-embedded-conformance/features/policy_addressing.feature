Feature: Who a policy's command addresses, and the correlation it carries
  A policy sends commands, and the facts its destination declares
  (decision 14.69), to aggregates directly, and to process managers
  (decision 14.36, as amended); every rule below holds for its facts as for
  its commands. A policy's answer is a list of Outputs, each the
  destination stream's root and the payload in the arm of its kind
  (io.angzarr.handler.v1.Output; decisions 14.78 and 14.91), and nothing
  else: the policy's coordinator addresses and stamps every one as a
  ReactionDelivery (decision 14.79): the destination domain (the
  descriptor's one output domain), the trigger's edition, the derived
  command_id, the trigger's correlation_id, the causation (the trigger's
  EventRef) and the policy's component name (docs/design/policies.md §2,
  §6). A policy cannot carry another correlation, address another domain,
  or set an identity or a fact origin: none of those is representable. A
  policy names its output's root for EVERY target, aggregate or process
  manager alike: nothing fills one in, and an output with no root is a
  policy bug (ANGZARR_REASON_POLICY_COMMAND_NO_ROOT). Setting a process
  manager's root to the trigger's correlation is a convention a policy may
  follow by writing it. The correlation ids below are UUIDs, as in every
  scenario (decision 14.18).

  Background:
    Given an "order" aggregate "order-1" on the main timeline
    And an "inventory" aggregate "inv-7" on the main timeline
    And a process manager "Fulfillment" owning domain "fulfillment" whose StartFulfillment command is a starting input

  @policy_addressing
  @C-0656
  Scenario: A policy's command to an aggregate lands on the root it names, with the trigger's correlation
    Given a policy "OrderToInventory" translating from "order" to "inventory" that sends ReserveStock to "inventory" "inv-7" on OrderPlaced
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000040" is recorded for order "order-1"
    Then "inventory" "inv-7" handles ReserveStock
    And the ReserveStock command carries correlation_id "0192a6b0-0000-7000-8000-000000000040"
    And the events it records carry correlation_id "0192a6b0-0000-7000-8000-000000000040"

  # C-0665 (an aggregate command naming no root) is merged into C-0657 and
  # retired: the rule is the same for every target (decision 14.36, as
  # amended).

  @policy_addressing
  @C-0657
  Scenario Outline: A policy's command naming no root is a policy bug, whatever its target
    Given a policy "<policy>" translating from "order" to "<domain>" that sends <command> with no root on OrderPlaced
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000041" is recorded for order "order-1"
    Then the policy's answer is refused INTERNAL with reason ANGZARR_REASON_POLICY_COMMAND_NO_ROOT, naming domain "<domain>" and command_type <command>
    And nothing is delivered, no root is filled in, and nothing is appended to any "<domain>" stream
    And the OrderPlaced trigger is dead-lettered with reason ANGZARR_REASON_POLICY_COMMAND_NO_ROOT as rejected_events, and an alert is raised

    Examples:
      | policy             | domain      | command          |
      | OrderToInventory   | inventory   | ReserveStock     |
      | OrderToFulfillment | fulfillment | StartFulfillment |

  @policy_addressing
  @C-0658
  Scenario: A policy's command to a process manager lands on the root the policy sets, keeping its trigger's correlation
    Given a policy "OrderToFulfillment" translating from "order" to "fulfillment" that sends StartFulfillment to "fulfillment" "0192a6b0-0000-7000-8000-0000000000aa" on OrderPlaced
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000042" is recorded for order "order-1"
    Then "Fulfillment" handles StartFulfillment on stream "fulfillment" root "0192a6b0-0000-7000-8000-0000000000aa"
    And nothing is appended to stream "fulfillment" root "0192a6b0-0000-7000-8000-000000000042"
    And the StartFulfillment command carries correlation_id "0192a6b0-0000-7000-8000-000000000042"
    And the events "Fulfillment" records carry correlation_id "0192a6b0-0000-7000-8000-000000000042"
    And nothing is dead-lettered and no alert is raised

  @policy_addressing
  @C-0659
  Scenario: One instance named by its root takes the inputs of several correlations, each keeping its own
    Given a policy "OrderToFulfillment" translating from "order" to "fulfillment" that sends StartFulfillment to "fulfillment" "0192a6b0-0000-7000-8000-0000000000aa" on OrderPlaced
    And "Fulfillment" also takes a RecordStockReserved command, which is not a starting input
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000043" is recorded for order "order-1"
    And a client sends a RecordStockReserved command to "fulfillment" root "0192a6b0-0000-7000-8000-0000000000aa" with correlation_id "0192a6b0-0000-7000-8000-000000000044"
    Then stream "fulfillment" root "0192a6b0-0000-7000-8000-0000000000aa" holds the events of both inputs
    And the StartFulfillment events carry correlation_id "0192a6b0-0000-7000-8000-000000000043"
    And the RecordStockReserved events carry correlation_id "0192a6b0-0000-7000-8000-000000000044"

  @policy_addressing
  @C-0660
  Scenario: One event fans out to an aggregate and a process manager, both commands keeping the trigger's correlation
    Given a policy "OrderToInventory" translating from "order" to "inventory" that sends ReserveStock to "inventory" "inv-7" on OrderPlaced
    And a policy "OrderToFulfillment" translating from "order" to "fulfillment" that sends StartFulfillment to "fulfillment" root the trigger's correlation on OrderPlaced
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000045" is recorded for order "order-1"
    Then "inventory" "inv-7" handles ReserveStock with correlation_id "0192a6b0-0000-7000-8000-000000000045"
    And "Fulfillment" handles StartFulfillment on stream "fulfillment" root "0192a6b0-0000-7000-8000-000000000045" with correlation_id "0192a6b0-0000-7000-8000-000000000045"
    And each command's ReactionDelivery causation is the OrderPlaced event of "order" "order-1"
    And no event either command records carries that causation: their causation is the command arm with the command's command_id

  # C-0666 (a policy's command carrying another correlation refused
  # under its retired other-correlation key) is retired: an
  # Output carries no correlation, so the coordinator always stamps the
  # trigger's (C-0656, C-0660; decision 14.78).

  # C-0583 (a command for a domain other than the policy's destination
  # refused POLICY_COMMAND_OUTSIDE_DESTINATION) keeps its id with its
  # surviving intent: an Output names no domain, so every output goes to
  # the descriptor's one output domain (decision 14.78).
  @policy_addressing
  @C-0583
  Scenario: Every output of a policy is addressed to its one destination by its coordinator
    Given a policy "OrderToInventory" translating from "order" to "inventory", whose descriptor names output domain "inventory"
    And its business code answers an OrderPlaced event with two command Outputs, ReserveStock for root "inv-7" and ReserveStock for root "inv-8"
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000048" is recorded for order "order-1"
    Then both ReactionDeliveries are addressed to domain "inventory", roots "inv-7" and "inv-8", on the main timeline
    And nothing is delivered to any other domain

  # C-0734 (a reaction that set its own identity or carried a fact origin
  # refused POLICY_COMMAND_OTHER_IDENTITY or POLICY_COMMAND_FACT_ORIGIN)
  # keeps its id with its surviving intent: an Output is a root and a
  # payload only, so the coordinator stamps every identity field (decisions
  # 14.78, 14.79; docs/design/policies.md §6).
  @policy_addressing
  @C-0734
  Scenario Outline: The coordinator stamps every field of a policy's output but its root and payload
    Given a policy "OrderToInventory" translating from "order" to "inventory" whose business code answers an OrderPlaced event with one <arm> Output ReserveStock for root "inv-7"
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000049" is recorded for order "order-1" at sequence 3
    Then the ReactionDelivery's <field> is <value>

    Examples:
      | arm     | field          | value                                                                       |
      | command | cover          | domain "inventory", root "inv-7", the trigger's edition                     |
      | command | command_id     | the reaction id derived from (the OrderPlaced EventRef, "OrderToInventory", 0) |
      | command | correlation_id | "0192a6b0-0000-7000-8000-000000000049"                                      |
      | command | causation      | the EventRef of "order" "order-1" sequence 3                                |
      | command | component      | "OrderToInventory"                                                          |
      | fact    | command_id     | the reaction id derived from (the OrderPlaced EventRef, "OrderToInventory", 0) |
      | fact    | component      | "OrderToInventory"                                                          |

  # C-0735 (a reaction command carrying a fact origin dead-lettered at its
  # destination) is retired: a ReactionDelivery has no fact origin and no
  # Handling; the destination derives a fact's origin (decision 14.79;
  # docs/design/pipeline.md §10).

  # Decision 14.49: conditions any coordinator may enforce on a policy's
  # reactions, each keyed.
  @policy_addressing
  @C-0736
  Scenario: A policy's reaction command for a domain its coordinator cannot route is a policy bug
    Given a policy "OrderToInventory" translating from "order" to "inventory", whose descriptor names output domain "inventory"
    And no component of "inventory" is registered with the policy's coordinator, and it knows no coordinator for "inventory"
    And its business code answers an OrderPlaced event with a ReserveStock command for "inventory" "inv-7"
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000050" is recorded for order "order-1"
    Then the policy's answer is refused INTERNAL with reason ANGZARR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN, naming domain "inventory" and command_type ReserveStock
    And nothing is delivered
    And the OrderPlaced trigger is dead-lettered with reason ANGZARR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN as rejected_events, retry_count 0, and an alert is raised

  @policy_addressing
  @C-0737
  Scenario: A chain of reactions deeper than the coordinator's limit stops at the limit
    # A limit a coordinator may enforce, where it follows the chain from
    # the input a caller sent (that input at depth 0). Each reaction
    # addresses another order: a policy never addresses its trigger's own
    # stream (C-0784; decision 14.71).
    Given a coordinator whose chain depth limit is 8
    And a policy "OrderToOrder" translating from "order" to "order" whose business code answers every ItemAdded event with an AddItem command for another order, a new one each time
    When an AddItem command for "order" "order-1" is handled
    Then the reaction at depth 9 is not delivered
    And the ItemAdded event that triggered it is dead-lettered with reason ANGZARR_REASON_CHAIN_TOO_DEEP and arg limit "8" as rejected_events, retry_count 0, and an alert is raised
