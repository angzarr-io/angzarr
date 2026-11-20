Feature: Upcasting - readers and handlers only ever see current shapes
  Stored events keep the shape they were written in. The coordinator that
  owns a stream brings them up to date on every way out of the store: when
  it loads the stream for its own component, when it answers a query, and
  wherever stored events reach a subscriber, whether published live or read
  from stored history (catching up, filling a gap, rebuilding). Live
  publishing carries only newly appended events, already in the current
  shape, so old shapes reach subscribers through stored history.
  Policies, process managers and projectors reading another component's
  stream never see an old shape and have no upcasters of their own.

  The same coordinator brings an incoming command's or fact's payload up to
  date before it judges the input by its type and before it delivers it,
  so the business process never sees an old shape (decision 14.39).

  It does all of this by asking the component's upcaster sidecar: a
  process of its own, beside the business process, whose upcasters know
  the old shapes. The business process never upcasts.

  Background:
    Given an "order" aggregate whose upcaster sidecar upcasts OrderPlacedV1 to OrderPlaced
    And order "order-1" has a stored OrderPlacedV1 event at sequence 0

  @upcasting
  @C-0584
  Scenario: Loading a stream for a command delivers current shapes
    When the coordinator delivers an AddItem command for order "order-1"
    Then business code receives an OrderPlaced event at sequence 0

  @upcasting
  @C-0585
  Scenario: A query returns current shapes
    When a client queries the events of order "order-1" through the "order" coordinator
    Then the result carries an OrderPlaced event at sequence 0

  @upcasting
  @C-0586
  Scenario Outline: A subscriber reading stored history receives current shapes
    Given a projector "OrderSummary" subscribed to "order"
    When the projector <reads> order "order-1"'s stored history from sequence 0
    Then the projector receives an OrderPlaced event at sequence 0
    And the projector is never given an OrderPlacedV1 event

    Examples:
      | reads                     |
      | catches up on             |
      | fills a gap in            |
      | is rebuilt from           |

  @upcasting
  @C-0587
  Scenario: Upcasting never rewrites the store
    When the coordinator delivers an AddItem command for order "order-1"
    Then order "order-1"'s stored event at sequence 0 is still an OrderPlacedV1

  # ===========================================================================
  # Commands and facts (decision 14.39; 12.11's discouragement stands)
  # ===========================================================================

  @upcasting @input_upcasting
  @C-0537
  Scenario: An old command type is upcast and decided as the current type
    Given the "order" upcaster sidecar upcasts AddItemV1 to AddItem
    When a client sends AddItemV1 "apple" to order "order-1"
    Then business code is handed an AddItem for "apple"
    And business code is never handed an AddItemV1
    And the command is accepted

  @upcasting @input_upcasting
  @C-0538
  Scenario: An old fact type is upcast and ingested as the current type
    Given the "order" upcaster sidecar upcasts ItemDeliveredV1 to ItemDelivered
    When a client sends the fact ItemDeliveredV1 "apple" to order "order-1"
    Then business code is handed an ItemDelivered fact for "apple"
    And the fact is accepted

  @upcasting @input_upcasting
  @C-0539
  Scenario: A type that upcasts to something no handler claims is refused naming both types
    Given the "order" upcaster sidecar upcasts AddLine to AddLineItem, which business code does not handle
    When a client sends AddLine "apple" to order "order-1"
    Then the client is refused INTERNAL with reason ANGZARR_REASON_UNKNOWN_COMMAND_TYPE, which is also its status message
    And the refusal's error info carries type_url "/orders.v1.AddLine"
    And the refusal's error info carries upcast_type_url "/orders.v1.AddLineItem"
    And nothing is recorded on order "order-1"

  @upcasting @input_upcasting
  @C-0540
  Scenario: A type with no handler and no upcaster is refused naming the type as sent
    When a client sends RemoveEverything "apple" to order "order-1"
    Then the client is refused INTERNAL with reason ANGZARR_REASON_UNKNOWN_COMMAND_TYPE, which is also its status message
    And the refusal's error info carries type_url "/orders.v1.RemoveEverything"
    And the refusal's error info carries upcast_type_url "/orders.v1.RemoveEverything"
    And nothing is recorded on order "order-1"

  @upcasting @input_upcasting @process_manager
  @C-0680
  Scenario: A process manager's start rule judges the upcast type
    Given a "Fulfillment" process manager whose starting input is StartFulfillment
    And the "Fulfillment" upcaster sidecar upcasts StartFulfillmentV1 to StartFulfillment
    When a client sends StartFulfillmentV1 for a root with no instance
    Then the coordinator starts an instance
    And business code is handed a StartFulfillment

  # Decision 14.39; the key: decision 14.51, a retryable failure carries
  # one.
  @upcasting @sidecar_down
  @C-0681
  Scenario: A sidecar that is down stalls what needs it
    Given the "order" upcaster sidecar is configured but unreachable
    When a client sends AddItem "apple" to order "order-1" and waits for the answer
    Then the client is answered UNAVAILABLE with reason ANGZARR_REASON_UPCASTER_UNAVAILABLE, arg domain "order" and arg cause
    And the "order" coordinator reports NOT_SERVING
    And nothing is recorded on order "order-1"
    And business code is not invoked

  # ===========================================================================
  # The sidecar's answer is keyed (decisions 14.96 and 14.97)
  # ===========================================================================

  @upcasting @upcast_keying
  @C-0819
  Scenario: A stream's pages are answered keyed by sequence, never by position
    Given order "order-1" also has stored OrderNotedV1 events at sequences 1 and 2
    And the "order" upcaster sidecar upcasts OrderNotedV1 to OrderNoted
    And the sidecar answers the pages of order "order-1" keyed by sequence, in the order 2, 0, 1
    When the coordinator delivers an AddItem command for order "order-1"
    Then business code receives an OrderPlaced event at sequence 0, then OrderNoted events at sequences 1 and 2
    And each event keeps the sequence and EventMeta it was stored with

  @upcasting @upcast_keying
  @C-0820
  Scenario: A command's payload is sent alone and answered keyed no_sequence
    Given the "order" upcaster sidecar upcasts AddItemV1 to AddItem
    When a client sends AddItemV1 "apple" to order "order-1"
    Then the sidecar is asked to upcast the command's payload as a lone payload, with no page and no sequence
    And it answers one payload keyed no_sequence
    And business code is handed an AddItem for "apple"

  @upcasting @upcast_keying
  @C-0821
  Scenario Outline: An answer whose keys are not exactly the requested pages' is a rule bug
    # docs/design/upcasting.md §5 and §6: a handler bug, with no key of
    # its own (guide §6: none was added).
    Given order "order-1" also has a stored OrderNotedV1 event at sequence 1
    And the "order" upcaster sidecar answers the pages of order "order-1" <answer>
    When a client sends AddItem "apple" to order "order-1" and waits for the answer
    Then the client is answered with an error status
    And business code is not invoked
    And nothing is recorded on order "order-1"

    Examples:
      | answer                                  |
      | with no entry for sequence 1            |
      | with an entry for sequence 7            |
      | with two entries for sequence 0         |
