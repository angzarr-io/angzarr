# docs:start:state_building_contract
Feature: State Building - Aggregate State Reconstruction
  Aggregates reconstruct their state by replaying events. This feature
  tests the state building contract including snapshot integration,
  event application, and event dispatch behavior.

  State building is the foundation of event sourcing - without correct
  state reconstruction, commands cannot make valid decisions.

  The coordinator assembles the EventBook: the snapshot it chose as the
  starting state, and only the pages after it (decision 14.40). The
  business process folds every page it is given onto the starting state;
  it never filters pages by the snapshot's sequence.
# docs:end:state_building_contract

  # ==========================================================================
  # Basic State Building
  # ==========================================================================

  @C-0281
  Scenario: Build state from empty event history
    Given an aggregate type with default state
    And an empty EventBook
    When I build state from the EventBook
    Then the state should be the default state
    And no events should have been applied

  @C-0282
  Scenario: Build state from single event
    Given an aggregate type with default state
    And an EventBook with 1 event of type "OrderCreated"
    When I build state from the EventBook
    Then the state should reflect the OrderCreated event
    And the state should have order_id set

  @C-0283
  Scenario: Build state from multiple events
    Given an aggregate type with default state
    And an EventBook with events:
      | sequence | type        |
      | 0        | OrderCreated|
      | 1        | ItemAdded   |
      | 2        | ItemAdded   |
    When I build state from the EventBook
    Then the state should reflect all 3 events
    And the built state should have 2 items

  @C-0284
  Scenario: Events are applied in sequence order
    Given an EventBook with events in order: A, B, C
    When I build state from the EventBook
    Then events should be applied as A, then B, then C
    And final state should reflect the correct order

  # ==========================================================================
  # Snapshot Integration
  # ==========================================================================

  @C-0285
  Scenario: Build state from snapshot only
    Given an EventBook with a snapshot at sequence 5
    And no events in the EventBook
    When I build state from the EventBook
    Then the state should equal the snapshot state
    And no events should be applied

  @C-0286
  Scenario: Build state from snapshot plus events
    Given an EventBook with:
      | snapshot_sequence | 5                |
      | events            | seq 6, 7, 8, 9   |
    When I build state from the EventBook
    Then the state should start from snapshot
    And only events 6, 7, 8, 9 should be applied

  @C-0287
  Scenario: The coordinator delivers no event the snapshot covers
    Given a stream with events at seq 3, 4, 6 and 7 and a usable snapshot at sequence 5
    When the coordinator delivers a command to the stream
    Then the delivered EventBook carries the snapshot at sequence 5
    And it carries only the events at seq 6 and 7
    And business code folds both of them onto the snapshot state

  # ==========================================================================
  # Event Application
  # ==========================================================================

  @C-0288
  Scenario: Unknown event types are skipped
    Given an EventBook with an event of unknown type
    When I build state from the EventBook
    Then the unknown event should be skipped
    And no error should occur
    And other events should still be applied

  @C-0289
  Scenario: Event application modifies state
    Given initial state with field value 0
    And an event that increments field by 10
    When I apply the event to state
    Then the field should equal 10

  @C-0290
  Scenario: Cumulative event application
    Given initial state with field value 0
    And events that increment by 5, 3, and 2
    When I apply all events to state
    Then the field should equal 10

  # ==========================================================================
  # Type-Erased Event Envelopes
  # ==========================================================================

  @C-0291
  Scenario: Build state handles type-erased event envelopes
    Given events stored in a type-erased envelope
    When I build state from the EventBook
    Then the envelope should be unwrapped
    And the typed event should be applied

  @C-0292
  Scenario: Type identifier determines event type
    Given an event whose envelope identifies type "orders.ItemAdded"
    When I apply the event
    Then the ItemAdded handler should be invoked
    And the type identifier should resolve to that handler

  # ==========================================================================
  # Error Handling
  # ==========================================================================

  @C-0293
  Scenario: Malformed event payload causes error
    Given an event with corrupted payload bytes
    When I attempt to build state
    Then an error should be raised
    And the error's reason is ANGZARR_REASON_PERSISTED_EVENT_CORRUPT, with the event's type_url in its args

  @C-0294
  Scenario: Missing required field in event
    Given an event missing a required field
    When I attempt to build state
    Then an error should be raised
    And the error's args name the missing field, and it carries no text

  # ==========================================================================
  # Next Sequence Calculation
  # ==========================================================================

  @C-0295
  Scenario: Next sequence from empty aggregate
    Given an EventBook with no events and no snapshot
    When I get next_sequence
    Then next_sequence should be 0

  @C-0296
  Scenario: Next sequence from events
    Given an EventBook with events up to sequence 4
    When I get next_sequence
    Then next_sequence should be 5

  @C-0297
  Scenario: Next sequence from snapshot only
    Given an EventBook with snapshot at sequence 10 and no events
    When I get next_sequence
    Then next_sequence should be 11

  @C-0298
  Scenario: Next sequence from snapshot plus events
    Given an EventBook with snapshot at 5 and events up to 8
    When I get next_sequence
    Then next_sequence should be 9

  # ==========================================================================
  # Immutability
  # ==========================================================================

  @C-0299
  Scenario: State building does not modify EventBook
    Given an EventBook
    When I build state from the EventBook
    Then the EventBook should be unchanged
    And the EventBook events should still be present

  @C-0300
  Scenario: State building returns new state object
    Given an existing state object
    When I build state from events
    Then a new state object should be returned
    And the original state should be unchanged

  # ==========================================================================
  # State Building Contract
  # ==========================================================================

  @C-0301
  Scenario: State building accepts a starting state and event sequence
    Given a starting state and a sequence of type-erased events
    When state is built
    Then each event should be unwrapped from its envelope
    And the event application step should run for each event
    And the resulting state should be returned

  @C-0302
  Scenario: Event application dispatches by type
    Given a state and a type-erased event
    When the event is applied
    Then the envelope should be unwrapped
    And the handler registered for that event type should be invoked
    And the produced state should reflect the event

  # ==========================================================================
  # When the input was accepted (decision 14.37)
  # ==========================================================================

  @recorded_at
  @C-0675
  Scenario Outline: The coordinator stamps when it accepted the input on the delivery
    Given the coordinator's clock reads "2026-10-28T12:00:00Z"
    When <input> is accepted for an "order" aggregate
    # One call for commands and facts (decision 14.98): the request is a
    # HandleRequest, its recorded_at stamped before the handler runs.
    Then the HandleRequest delivered to business code has recorded_at "2026-10-28T12:00:00Z"

    Examples:
      | input                      |
      | an AddItem command         |
      | an ItemDelivered fact      |
