Feature: Subscriptions - catch-up from a resume point, then live
  EventStreamService.Subscribe takes one target (SubscribeRequest.target):
  one stream (StreamFrom) or every stream of one correlation
  (CorrelationFrom). The server first delivers every page the target holds
  from the subscriber's resume point (catch-up), then every page committed
  after, as it commits (live), with no gap and no duplicate between the two
  (decision 14.105). A resume point is inclusive, as SequenceRange.lower: 0
  is a stream's start, and a reconnect resumes at the last sequence received
  + 1 (decision 14.106). Order is per stream only: a stream's pages arrive in
  sequence order, and pages of different streams in no defined order; there
  is no cross-stream order and no store-wide position, so a correlation's
  resume point is one cursor per stream
  (docs/design/queries-and-subscriptions.md §5 and §6).

  EventQueryService.ListStreams lists every stream, one Cover each.

  Background:
    Given order "order-1" has events at sequences 0 to 4 on the main timeline

  # ---------------------------------------------------------------
  # One stream (StreamFrom)
  # ---------------------------------------------------------------

  @subscribe
  @C-0822
  Scenario: A stream subscription from 0 catches up from the start, then goes live
    When a client subscribes to order "order-1" from_sequence 0
    And order "order-1" then commits events at sequences 5 and 6
    Then the client receives the pages at sequences 0 to 4, then 5 and 6, in sequence order
    And every book it receives has the cover of order "order-1"
    And no page is received twice

  @subscribe
  @C-0823
  Scenario: from_sequence is inclusive
    When a client subscribes to order "order-1" from_sequence 3
    Then the client receives the pages at sequences 3 and 4 first
    And it receives no page below sequence 3

  @subscribe
  @C-0824
  Scenario: A reconnect resumes at the last sequence received plus one, losing and repeating nothing
    Given a client subscribed to order "order-1" from_sequence 0 and received the pages at sequences 0 to 4
    And the subscription dropped
    And order "order-1" committed events at sequences 5 and 6 while it was down
    When the client subscribes to order "order-1" from_sequence 5
    And order "order-1" then commits the event at sequence 7
    Then the client receives the pages at sequences 5, 6 and 7, in sequence order
    And it receives no page at or below sequence 4

  @subscribe
  @C-0825
  Scenario: A page committed between a query and a subscription is caught up
    Given a client queried the events of order "order-1" and received sequences 0 to 4
    And order "order-1" committed the event at sequence 5 before the client subscribed
    When the client subscribes to order "order-1" from_sequence 5
    Then the client receives the page at sequence 5
    And it then receives live pages from sequence 6 on, with no gap

  @subscribe
  @C-0826
  Scenario: A stream subscription reads its edition as a query does
    Given order "order-1" is forked into edition "what-if" at sequence 3
    And order "order-1" in edition "what-if" has its own event at sequence 4
    When a client subscribes to order "order-1" in edition "what-if" from_sequence 0
    Then the client receives main's pages at sequences 0 to 2, the fork event at 3, then the edition's own event at 4

  # ---------------------------------------------------------------
  # One correlation (CorrelationFrom)
  # ---------------------------------------------------------------

  @subscribe @correlation
  @C-0827
  Scenario: A correlation subscription catches up every stream of the correlation, in every domain, then goes live
    Given correlation "0192a6b0-0000-7000-8000-0000000000c1" has pages on order "order-1" at sequences 0 to 2 and on payment "pay-1" at sequences 0 and 1
    When a client subscribes to correlation "0192a6b0-0000-7000-8000-0000000000c1" with no cursors
    And shipping "ship-1" then commits an event at sequence 0 with that correlation
    Then the client receives order "order-1"'s pages 0 to 2, payment "pay-1"'s pages 0 and 1, and shipping "ship-1"'s page 0
    And each stream's pages arrive in sequence order
    And no page of another correlation is received

  @subscribe @correlation
  @C-0828
  Scenario: A correlation subscription resumes from each listed stream's cursor, and from 0 for a stream not listed
    Given correlation "0192a6b0-0000-7000-8000-0000000000c1" has pages on order "order-1" at sequences 0 to 2 and on payment "pay-1" at sequences 0 to 3
    And shipping "ship-1" has a page at sequence 0 with that correlation, which the client never received
    When a client subscribes to correlation "0192a6b0-0000-7000-8000-0000000000c1" with seen cursors order "order-1" next_sequence 3 and payment "pay-1" next_sequence 2
    Then the client receives payment "pay-1"'s pages 2 and 3 and shipping "ship-1"'s page 0
    And it receives no page of order "order-1" below sequence 3 and none of payment "pay-1" below sequence 2

  @subscribe @correlation
  @C-0829
  Scenario: A correlation subscription promises no order across streams
    Given correlation "0192a6b0-0000-7000-8000-0000000000c1" has pages on order "order-1" and payment "pay-1"
    When a client subscribes to correlation "0192a6b0-0000-7000-8000-0000000000c1" with no cursors
    Then within each stream the pages arrive in sequence order
    And the books of order "order-1" and payment "pay-1" may interleave in any order
    And no page carries a store-wide position

  # ---------------------------------------------------------------
  # Refusals
  # ---------------------------------------------------------------

  @subscribe @refusal
  @C-0830
  Scenario Outline: A subscription request that is malformed is refused INVALID_ARGUMENT
    When a client subscribes with <request>
    Then the subscription is refused INVALID_ARGUMENT
    And nothing is delivered

    Examples:
      | request                                                                                        |
      | no target set                                                                                  |
      | a stream target whose cover names no domain                                                    |
      | a stream target whose root is not 16 bytes                                                     |
      | a correlation target with no correlation_id                                                    |
      | a correlation target whose correlation_id is not 16 bytes                                      |
      | a correlation target in edition "what-if" with a cursor of order "order-1" on the main timeline |
      | a correlation target with two cursors for order "order-1"                                      |
      | a correlation target with a set empty edition ""                                               |

  @subscribe @refusal
  @C-0831
  Scenario Outline: A subscription naming an edition with no fork is refused NOT_FOUND
    Given no fork event of edition "what-if" exists anywhere
    When a client subscribes to <target> in edition "what-if"
    Then the subscription is refused NOT_FOUND

    Examples:
      | target                                                      |
      | order "order-1"                                             |
      | correlation "0192a6b0-0000-7000-8000-0000000000c1"          |

  # ---------------------------------------------------------------
  # Listing streams (EventQueryService.ListStreams; decision 14.105)
  # ---------------------------------------------------------------

  @list_streams
  @C-0832
  Scenario: ListStreams answers one Cover per stream, of every kind and edition
    Given payment "pay-1" has events on the main timeline
    And a process manager "Fulfillment" has an instance stream at root "0192a6b0-0000-7000-8000-000000000031"
    And order "order-1" is forked into edition "what-if"
    When a client calls ListStreams
    Then it receives exactly one Cover for each of: order "order-1" on the main timeline, order "order-1" in edition "what-if", payment "pay-1" on the main timeline and "fulfillment" "0192a6b0-0000-7000-8000-000000000031" on the main timeline
    And each Cover names its domain, its edition (unset for main) and its root
