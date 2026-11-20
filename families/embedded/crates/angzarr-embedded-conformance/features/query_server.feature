Feature: Query server - reading the event store with no component behind it
  Reading the event store is its own read-only interface, separate from
  sending commands. A coordinator offers it for its own component's domain.
  It can also run on its own, as a standalone query server for downstream
  readers, with no component behind it. A standalone server accepts no
  commands, and since it has no component it has no registration to judge
  snapshots by, so it never returns one.

  @query_server
  @C-0588
  Scenario: A standalone query server answers queries but takes no commands
    Given a standalone query server over the "order" event store with no component behind it
    And order "order-1" has events at sequences 0 to 3
    When a client queries the events of order "order-1"
    Then the result carries events 0 to 3
    When a client sends an AddItem command for order "order-1" to the query server
    Then the command is refused UNIMPLEMENTED with reason DISTRIBUTED_REASON_COMMANDS_NOT_SERVED and arg domain "order"

  @query_server
  @C-0589
  Scenario Outline: A standalone query server never returns a snapshot
    Given a standalone query server over the "order" event store with no component behind it
    And order "order-1" has a snapshot at sequence 5 of state type "/orders.v1.OrderState" recorded under rules version "1.4.2+build.7"
    And order "order-1" has events at sequences 0 to 9
    When a client queries order "order-1" <how>
    Then the result carries no snapshot and events 0 to <last>

    Examples:
      | how                 | last |
      | for all its events  | 9    |
      | as of sequence 7    | 7    |
