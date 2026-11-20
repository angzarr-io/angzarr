Feature: Fact injection

  A process manager is a domain (decision 14.31): facts reach it as they
  reach an aggregate (timeouts, webhooks). A client's fact rides a
  CommandPage's Fact arm, with its origin (decision 14.79). A policy
  delivers facts of two kinds, both as a ReactionDelivery in its fact arm,
  carrying the trigger as its causation and no origin (the destination
  derives one; docs/design/pipeline.md §10): the facts its destination
  declares, which its business code emits as it emits commands
  (policy_facts.feature; decision 14.69), and the refusal fact its one
  refusal callback translates a refused command into, which its
  coordinator addresses to the exact stream of its trigger
  (policy_refusal.feature; decision 46 #5). The receiver judges neither by
  its causation: the coordinator that built it addressed it.

  C-0184, C-0188, C-0189 and C-0493 stay retired (decision 14.31): a
  policy's facts are policy_facts.feature's.

  A fact is something that already happened outside the system, delivered
  through the command coordinator like any command, and sequenced and
  persisted like any input. The receiving component declares each fact
  type with the (io.angzarr.v1.fact) option (decision 14.91); its handler
  records the fact and may add events that flag it, but cannot refuse it.
  A fact whose handler fails, or whose business process answers it with
  any error status, is a bug: it is dead-lettered and alerted, nothing is
  persisted, and its sender is answered INTERNAL
  ANGZARR_REASON_FACT_DEAD_LETTERED, never retried (decisions 14.72 and
  14.100; there is no PARKED outcome).

  Sequences are 0-based: an aggregate with N events has next_sequence N.

  Background:
    Given the order aggregate declares ShipmentDispatched and PaymentCaptured facts, each with the (fact) option
    And the inventory aggregate declares StockHoldRequested a fact, with the (fact) option

  # ---------------------------------------------------------------------------
  # Cross-domain reality: the receiving aggregate records, never rejects
  # ---------------------------------------------------------------------------

  @C-0185
  Scenario: Fact receives the next sequence number
    Given an "order" aggregate with 3 existing events
    When a ShipmentDispatched fact is injected
    Then the fact is persisted with sequence number 3
    And subsequent events continue from sequence 4

  @C-0186
  Scenario: Fact is recorded even when a command with the same intent would be refused
    Given an order "order-2" that has been cancelled
    When a PaymentCaptured fact is injected into the "order" aggregate "order-2"
    Then the fact is persisted at the order's next sequence
    And no rejection is produced

  @C-0591
  Scenario: A fact addressed to a process manager is recorded on its stream and never refused
    Given a process manager "Fulfillment" owning domain "fulfillment" with an instance at root "0192a6b0-0000-7000-8000-000000000001"
    When a CarrierAssigned fact with external_id "carrier-C1" is sent to "fulfillment" root "0192a6b0-0000-7000-8000-000000000001"
    Then the process manager's events for it are appended to that instance's stream
    And each of those events records the fact cause: the fact's command_id and its origin
    And the sender is answered APPLIED

  @C-0609
  Scenario: A refusal fact delivered for its trigger's own stream is recorded there
    # A policy's refusal fact, for the exact stream of the policy's
    # triggering event (decision 14.31), as a ReactionDelivery fact arm
    # (decision 14.79).
    Given an "order" aggregate "order-1" that declares DeliveryRefused a fact
    When a policy's coordinator delivers a DeliveryRefused ReactionDelivery fact to "order" root "order-1" whose causation names an event of "order" aggregate "order-1"
    Then the fact is recorded at the order's next sequence
    And its events record the fact cause with the delivery's command_id, and none records the delivery's causation

  # C-0610 (a CommandRefused addressed to another stream than its trigger's
  # refused by the receiver) is retired: there is no CommandRefused, and a
  # refusal fact is addressed by the coordinator from Translate's payload
  # alone, so it cannot name another stream (decisions 46 #5 and 14.78).

  @C-0782
  Scenario: A policy's fact whose causation is in another domain is recorded by the stream it is addressed to
    # A policy's fact for its destination (decision 14.69): its causation is
    # its trigger, in its source domain, and its command_id the reaction id
    # its policy's coordinator stamped, taken as given.
    Given an "order" aggregate "order-1" that declares ShipmentDispatched a fact
    When a policy's coordinator delivers a ShipmentDispatched ReactionDelivery fact to "order" root "order-1" whose causation names an event of the "shipping" domain, with the command_id it stamped
    Then the fact is recorded at the order's next sequence
    And its events record the fact cause with that command_id and an origin whose external_id is that command_id's canonical text

  # ---------------------------------------------------------------------------
  # Error handling
  # ---------------------------------------------------------------------------

  @C-0492
  Scenario: A fact of a type the aggregate does not declare is refused and nothing is persisted
    Given an "order" aggregate with 3 existing events
    When an OrderRefunded fact with external_id "refund-R1" is injected into the "order" aggregate
    Then "order"'s router answers the fact INTERNAL with reason ANGZARR_REASON_NO_FACT_HANDLER, naming its type_url
    And the fact is dead-lettered with reason ANGZARR_REASON_NO_FACT_HANDLER as rejected_command, and an alert is raised
    And the sender is answered INTERNAL with reason ANGZARR_REASON_FACT_DEAD_LETTERED, cause ANGZARR_REASON_NO_FACT_HANDLER
    And the "order" aggregate still has 3 events
    And external_id "refund-R1" is not recorded as processed

  @C-0786
  Scenario: A fact its business process answers with an error status is dead-lettered, and its sender answered INTERNAL
    # Owner's rulings (decisions 14.72 and 14.100): a fact is never refused,
    # so any error status its business process answers is that process's
    # bug; the fact is dead-lettered like any input that cannot be
    # delivered, and its sender answered non-retryable.
    Given an "order" aggregate with 3 existing events
    And the "order" business process answers every ShipmentDispatched fact FAILED_PRECONDITION
    When a ShipmentDispatched fact is injected
    Then the fact is dead-lettered as rejected_command, and an alert is raised
    And the sender is answered INTERNAL with reason ANGZARR_REASON_FACT_DEAD_LETTERED, naming the stream and its cause
    And the "order" aggregate still has 3 events

  @C-0190
  Scenario: Duplicate fact with same external_id is idempotent
    Given a fact with external_id "shipment-S1-dispatched"
    When the same fact is injected twice
    Then only one event is stored in the aggregate
    And the second injection is answered with delivery DUPLICATE and the stored events
