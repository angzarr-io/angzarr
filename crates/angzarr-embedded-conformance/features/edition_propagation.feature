# Coordinator-contract feature: edition propagation across cross-domain
# emissions. When a policy reacts to an event in edition X, its
# coordinator stamps edition X onto every output it addresses (a policy's
# commands and facts, and its refusal facts): an Output carries only a root
# and a payload, so a policy cannot name an edition (decision 14.78). A
# process manager's events land in its input's edition, like any domain's;
# a handler answers payloads only, so it cannot name a cover either.
#
# The edition is stamped, never a fork: every stream an edition holds is
# forked explicitly with ForkStream, and an input in X to a stream not
# forked in X is refused ANGZARR_REASON_EDITION_NOT_FORKED (decision
# 14.107; edition_forks.feature). So each destination below is forked in
# the edition first.
#
# A process manager is a domain (decision 14.31): its inputs are commands
# and facts (from policies, in their trigger's edition, or from outside),
# and its events land in its input's edition, like any domain's.
#
# Coverage in core: Rust unit tests in
# src/orchestration/saga/local/tests.rs (C-0138..C-0142) and
# src/orchestration/process_manager/local/tests.rs (C-0144, before decision
# 14.31).
Feature: Edition propagation across cross-domain emissions
  As a policy / process-manager author
  I want the framework to guarantee that emitted commands, facts and events
  inherit the source / trigger cover's edition
  So that timeline consistency holds across cross-domain boundaries
  without requiring me to manually stamp every outgoing cover

  # ---------------------------------------------------------------
  # Policy: source event → outgoing commands / facts
  # ---------------------------------------------------------------

  @C-0138
  Scenario: Policy propagates source edition to outgoing commands
    Given a policy "OrderFulfillment" translating from "order" to "inventory"
    And the policy handles OrderCreated by emitting a ReserveStock command
    And the source event has edition "speculative"
    And the destination "inventory" stream is forked in edition "speculative"
    When an OrderCreated event is dispatched to the policy
    Then the delivered command's cover has edition "speculative"

  @C-0139
  Scenario: Policy propagates source edition to outgoing facts
    # A policy's answer is commands and facts its destination declares
    # (decision 14.69); a policy emits no event.
    Given a policy "OrderAudit" translating from "order" to "audit"
    And the audit aggregate declares fact OrderObserved
    And the policy handles OrderCreated by emitting an OrderObserved fact
    And the source event has edition "speculative"
    And the destination "audit" stream is forked in edition "speculative"
    When an OrderCreated event is dispatched to the policy
    Then the delivered fact's cover has edition "speculative"

  # C-0140 (a policy's command naming another edition than its trigger's
  # refused) is retired: an Output holds only a root and a payload, so a
  # policy cannot name an edition; its coordinator stamps the trigger's
  # (decision 14.78; docs/design/policies.md §6).

  @C-0141
  Scenario: Policy propagates main-timeline (empty) edition unchanged
    Given a policy "OrderFulfillment" translating from "order" to "inventory"
    And the source event has no edition set
    When an OrderCreated event is dispatched to the policy
    Then the delivered command's cover has no edition set

  # C-0142 (an edition's divergences propagated) is retired: editions are
  # per-stream forks with no divergence records (decision 14.32; the fork
  # scenarios are in edition_forks.feature).

  # ---------------------------------------------------------------
  # Process Manager: input → its own events
  # ---------------------------------------------------------------

  @C-0144
  Scenario: A PM's events land in its input's edition
    Given a process manager "Fulfillment" owning domain "fulfillment"
    And the PM records an OrderTracked event on a StartFulfillment command
    And the PM's stream is forked in edition "speculative"
    And the command's cover has edition "speculative"
    When the StartFulfillment command is delivered to the PM
    Then the OrderTracked event is persisted on the PM's stream in edition "speculative"
    And the PM's stream on the main timeline is unchanged

  # C-0590 (a PM's events book naming another edition refused) is retired:
  # a handler answers payloads only (HandleResponse.events), so it cannot
  # name a cover; the coordinator wraps every event under the input's cover
  # (decision 14.78; docs/design/handler-contract.md §2 and §7).

  @C-0608
  Scenario: A policy's refusal fact carries its trigger's edition
    # The one refusal path (decision 46 #5): Translate's fact goes to the
    # trigger's exact stream, its edition included.
    Given a policy "OrderToInventory" translating from "order" to "inventory"
    And the policy translates a refusal of ReserveStock into an order fact ReservationRefused
    And the source event has edition "speculative"
    And the destination "inventory" stream is forked in edition "speculative"
    And the inventory aggregate refuses ReserveStock with a typed refusal
    When an OrderCreated event is dispatched to the policy
    Then the ReservationRefused fact is delivered to the trigger's stream with edition "speculative"

  # ---------------------------------------------------------------
  # Main timeline: unset, and spelled no other way
  # ---------------------------------------------------------------

  @C-0409
  Scenario Outline: The main timeline is spelled one way: unset
    # types.proto, Cover.edition: main is unset; an empty edition is
    # malformed, and "angzarr" names an edition like any other (decision
    # 14.66).
    Given an "order" aggregate "order-1" with 2 events on the main timeline
    When a client sends an AddItem command to "order-1" with edition <edition>
    Then <outcome>

    Examples:
      | edition   | outcome                                                                                                              |
      | unset     | an ItemAdded event is persisted at sequence 2                                                                        |
      | ""        | the command fails INVALID_ARGUMENT and nothing is persisted                                                          |
      | "angzarr" | the client is answered FAILED_PRECONDITION with reason ANGZARR_REASON_EDITION_NOT_FORKED, stream "order-1", edition "angzarr" |

  @C-0410
  Scenario: The main timeline cannot be deleted
    Given an "order" aggregate "order-1" with 2 events on the main timeline
    When DeleteEdition is requested with an empty name
    Then the request is refused INVALID_ARGUMENT because main is unnamed and cannot be deleted
    And the aggregate still has 2 events
