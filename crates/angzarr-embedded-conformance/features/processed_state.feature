Feature: Processed state - what a projector or a policy has processed outlives a purge
  The driver of a projector, or of a policy, keeps the record of what its
  handler has processed: every stream it has seen, and where it is on each
  (decision 14.68). A projector's run is recorded only after the
  projector's affirmative answer (decision 14.97;
  projector_delivery.feature). The record is the driver's own: a purge never
  touches it. A purge deletes a stream's events, its snapshot, its command
  ids and its dead letters, and KEEPS the stream: it appends ONE tombstone
  event at the stream's next sequence, so sequence numbers never restart, a
  later write continues after it, and the stream stays listed. A stream is
  dense from sequence 0 or from its latest tombstone; one whose last event
  is a tombstone is destroyed. Readers receive the tombstone as an ordinary
  event: a projector is handed it in a book like any other (no gone book),
  an aggregate's state is the default again after it, and a policy, which is
  handed exactly one trigger event per call (ReactRequest; decision 46,
  Q3), is not called for it. A repeat purge deletes the events written after
  the last tombstone and appends a new tombstone only if it deleted any;
  earlier tombstones stay. A purge made while no driver runs is found at
  the next one, as the tombstone every driver reads next (decision 14.68's
  gone book and forgetting are superseded).

  @C-0771
  Scenario: A projector's record of a stream outlives the stream's purge
    Given a projector "OrderSummary" of domain "order" that has processed order "order-1" to sequence 3
    When order "order-1" is purged
    Then the record of "OrderSummary" still holds order "order-1" at sequence 3

  @C-0772
  Scenario: A stream purged while no catch-up ran is handed its tombstone at the next one
    Given a projector "OrderSummary" of domain "order" that has processed order "order-1" to sequence 3
    And order "order-1" is purged while "OrderSummary" is not running
    When "OrderSummary" catches up
    Then "OrderSummary" is handed the tombstone of order "order-1" at sequence 3
    And the record of "OrderSummary" holds order "order-1" at sequence 4

  @C-0773
  Scenario: A policy's driver is not called for a purged source stream's tombstone
    # React takes one trigger event (decision 46, Q3); a tombstone is no
    # event of the domain, so the policy is not called for it.
    Given a policy "OrderFulfillment" translating from "order" to "inventory" that has processed order "order-1" to sequence 2
    And order "order-1" is purged while "OrderFulfillment" is not running
    When "OrderFulfillment" catches up
    Then "OrderFulfillment"'s React is not called for order "order-1"
    And order "order-1" is destroyed: its last event is a tombstone at sequence 2

  @C-0774
  Scenario: A stream the record holds and the store still lists is not gone
    Given a projector "OrderSummary" of domain "order" that has processed order "order-1" to sequence 3
    When "OrderSummary" catches up
    Then "OrderSummary" is handed nothing of order "order-1"
    And the record of "OrderSummary" still holds order "order-1" at sequence 3

  @C-0880
  Scenario: A purge keeps its stream and a later write continues after the tombstone
    Given order "order-1" holds events at sequences 0 to 2
    When order "order-1" is purged
    Then order "order-1" is destroyed: its last event is a tombstone at sequence 3
    And order "order-1" is still listed
    When an event is appended to order "order-1"
    Then order "order-1" holds an event at sequence 4 after the tombstone

  @C-0881
  Scenario: A repeat purge keeps the first tombstone and adds one only after events it deleted
    Given order "order-1" holds events at sequences 0 to 2
    And order "order-1" is purged
    When order "order-1" is purged
    Then order "order-1" holds one tombstone, at sequence 3
    When an event is appended to order "order-1"
    And order "order-1" is purged
    Then order "order-1" holds tombstones at sequences 3 and 5

  @C-0882
  Scenario: An edition's purge tombstones the edition and keeps its fork
    # A reader of the edition then reads main's events below the fork (they
    # are main's), the fork, and the edition's tombstone: the edition's own
    # history is closed, a fold restarts from the default state there.
    Given order "order-1" holds events at sequences 0 to 1
    And order "order-1" is forked into edition "what-if" with one event of its own
    When the edition "what-if" of order "order-1" is purged
    Then the edition "what-if" of order "order-1" ends with a tombstone at sequence 4
    And order "order-1" in main still holds events at sequences 0 to 1
