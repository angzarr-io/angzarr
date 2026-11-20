Feature: Rules version - recording who decided, and choosing snapshots
  Every business binary has a full version, fixed when it is built (for
  example "1.4.2+build.7"). It gives that version, together with its state
  type, when its coordinator asks it to describe its component (see
  component_discovery.feature), and reports the same version with every
  answer. The coordinator records that version, unaltered, on every
  event and snapshot the answer produced, so each event names the build that
  decided it.

  The coordinator alone decides whether a stored snapshot may be used. It
  offers a snapshot only when the name its state's type URL ends in is the
  registered state type name (StateDescriptor.type_name; decision 14.101)
  and its version passes the domain's compatibility rule against
  the registered version. By default, versions are compatible when their
  major and minor numbers are equal, ignoring one leading "v" or "V"; a
  version that is not a semantic version is compatible only with exactly
  the same version. Nothing replaces that rule.
  A snapshot that is not offered costs nothing but time: business code gets
  the full history from sequence 0 instead. A query is judged by the same
  rule against the answering coordinator's own registration, and uses no
  snapshot while that coordinator holds none.

  Every answer must report the version the binary described: an answer
  reporting any other version is a bug, and nothing it produced is kept.

  Background:
    Given an "order" aggregate registered with state type "orders.v1.OrderState" and rules version "1.4.2+build.7"

  # ===========================================================================
  # Recording the version
  # ===========================================================================

  @rules_version
  @C-0518
  Scenario: Every event of an answer records the reported version in full
    When business code answers an AddItem command with ItemAdded and ShippingQuoted events, reporting rules version "1.4.2+build.7"
    Then the ItemAdded event records rules version "1.4.2+build.7"
    And the ShippingQuoted event records rules version "1.4.2+build.7"

  @rules_version
  @C-0519
  Scenario: A snapshot returned with an answer records the reported version in full
    When business code answers an AddItem command with an ItemAdded event and a snapshot, reporting rules version "1.4.2+build.7"
    Then the snapshot records rules version "1.4.2+build.7"
    And the snapshot records state type "/orders.v1.OrderState"

  @rules_version
  @C-0520
  Scenario: During a rolling deployment each event names the build that decided it
    Given two instances of the "order" business binary, at rules versions "1.4.2+build.7" and "1.5.0+build.9"
    When the instance at "1.4.2+build.7" decides an AddItem command for order "order-1"
    And the instance at "1.5.0+build.9" decides an AddItem command for order "order-1"
    Then the first ItemAdded event records rules version "1.4.2+build.7"
    And the second ItemAdded event records rules version "1.5.0+build.9"

  @rules_version
  @C-0521
  Scenario: A process manager's own events record the version it reported
    Given a process manager "Fulfillment" registered with rules version "2.0.1"
    When the process manager handles a StartFulfillment command with a FulfillmentStarted event, reporting rules version "2.0.1"
    Then the FulfillmentStarted event records rules version "2.0.1"

  # ===========================================================================
  # Choosing a snapshot: the default rule
  # ===========================================================================

  @snapshot_selection
  @C-0522
  Scenario Outline: A snapshot of the registered state type and the same major.minor is offered
    Given order "order-1" has a snapshot at sequence 5 of state type "/orders.v1.OrderState" recorded under rules version "<snapshot version>"
    And order "order-1" has events at sequences 6 and 7
    When the coordinator delivers an AddItem command for order "order-1"
    Then business code receives the snapshot at sequence 5
    And business code receives the events at sequences 6 and 7

    Examples:
      | snapshot version |
      | 1.4.2+build.7    |
      | 1.4.0            |
      | 1.4.9+build.12   |
      | 1.4.3-rc.1       |

  @snapshot_selection
  @C-0523
  Scenario Outline: A snapshot from another major.minor is not offered
    Given order "order-1" has a snapshot at sequence 5 of state type "/orders.v1.OrderState" recorded under rules version "<snapshot version>"
    And order "order-1" has events at sequences 0 to 7
    When the coordinator delivers an AddItem command for order "order-1"
    Then business code receives no snapshot
    And business code receives the events at sequences 0 to 7
    And the command succeeds

    Examples:
      | snapshot version |
      | 1.3.9            |
      | 1.5.0            |
      | 2.4.2            |
      | 0.4.2            |

  @snapshot_selection
  @C-0524
  Scenario: A snapshot of another state type is not offered, whatever its version
    Given order "order-1" has a snapshot at sequence 5 of state type "/orders.v1.LegacyOrderState" recorded under rules version "1.4.2+build.7"
    And order "order-1" has events at sequences 0 to 7
    When the coordinator delivers an AddItem command for order "order-1"
    Then business code receives no snapshot
    And business code receives the events at sequences 0 to 7

  @snapshot_selection
  @C-0525
  Scenario Outline: Against a registered version that is not a semantic version, only the same version is offered
    Given an "order" aggregate registered with state type "orders.v1.OrderState" and rules version "build-2026-10-19.3"
    And order "order-1" has a snapshot at sequence 5 of state type "/orders.v1.OrderState" recorded under rules version "<snapshot version>"
    When the coordinator delivers an AddItem command for order "order-1"
    Then the snapshot is <offered>

    Examples:
      | snapshot version   | offered     |
      | build-2026-10-19.3 | offered     |
      | build-2026-10-19.4 | not offered |
      | 1.4.2              | not offered |

  @snapshot_selection
  @C-0526
  Scenario Outline: A snapshot version that is not a semantic version is not offered against a semantic one
    Given order "order-1" has a snapshot at sequence 5 of state type "/orders.v1.OrderState" recorded under rules version "<snapshot version>"
    When the coordinator delivers an AddItem command for order "order-1"
    Then the snapshot is not offered

    Examples:
      | snapshot version |
      | 1.4              |
      | release-1.4.2    |
      |                  |

  @snapshot_selection
  @C-0553
  Scenario Outline: One leading "v" is ignored when comparing semantic versions
    Given an "order" aggregate registered with state type "orders.v1.OrderState" and rules version "<registered>"
    And order "order-1" has a snapshot at sequence 5 of state type "/orders.v1.OrderState" recorded under rules version "<snapshot version>"
    When the coordinator delivers an AddItem command for order "order-1"
    Then the snapshot is <offered>

    Examples:
      | registered    | snapshot version | offered     |
      | 1.4.2+build.7 | v1.4.0           | offered     |
      | 1.4.2+build.7 | V1.4.9           | offered     |
      | v1.4.2        | 1.4.0            | offered     |
      | v1.4.2        | v1.5.0           | not offered |
      | 1.4.2+build.7 | vv1.4.2          | not offered |
      | vv1.4.2       | vv1.4.2          | offered     |

  # ===========================================================================
  # Choosing a snapshot: a domain's own rule
  # ===========================================================================

  # C-0527 to C-0530 (a domain's snapshot accept and reject patterns, and
  # a pattern refused when the configuration loads) are retired: a
  # snapshot passes the default rule or none (decision 14.66).

  # ===========================================================================
  # Queries
  # ===========================================================================

  @snapshot_selection @query
  @C-0554
  Scenario Outline: A query returns a snapshot only if it passes the registered component's rule
    Given order "order-1" has a snapshot at sequence 5 of state type "/orders.v1.OrderState" recorded under rules version "<snapshot version>"
    And order "order-1" has events at sequences 0 to 7
    When a client queries the events of order "order-1"
    Then the result <carries>

    Examples:
      | snapshot version | carries                                             |
      | 1.4.0            | carries the snapshot at sequence 5 and events 6 to 7 |
      | 1.5.0            | carries no snapshot and events 0 to 7                |

  @snapshot_selection @query
  @C-0555
  Scenario Outline: A query as of a point in history applies the same rule
    Given order "order-1" has a snapshot at sequence 5 of state type "/orders.v1.OrderState" recorded under rules version "<snapshot version>"
    And order "order-1" has events at sequences 0 to 9
    When a client queries order "order-1" as of sequence 7
    Then the result <carries>

    Examples:
      | snapshot version | carries                                             |
      | 1.4.9            | carries the snapshot at sequence 5 and events 6 to 7 |
      | 2.0.0            | carries no snapshot and events 0 to 7                |

  @snapshot_selection @query
  @C-0556
  Scenario: A query server holding no registration answers a query with no snapshot
    # A coordinator before its first successful describe, or a standalone
    # query server, which has no component behind it (query_server.feature).
    Given the answering query server holds no registration for the "order" aggregate
    And order "order-1" has a snapshot at sequence 5 of state type "/orders.v1.OrderState" recorded under rules version "1.4.2+build.7"
    And order "order-1" has events at sequences 0 to 7
    When a client queries the events of order "order-1"
    Then the result carries no snapshot and events 0 to 7

  @snapshot_selection @query
  @C-0560
  Scenario Outline: During a rolling deployment each coordinator judges a query by its own instance's version
    Given a second "order" instance at rules version "1.5.0+build.1" behind its own coordinator
    And order "order-1" has a snapshot at sequence 5 of state type "/orders.v1.OrderState" recorded under rules version "1.4.0"
    And order "order-1" has events at sequences 0 to 7
    When a client's query for the events of order "order-1" is answered by the coordinator of the instance at "<instance>"
    Then the result <carries>

    Examples:
      | instance      | carries                                              |
      | 1.4.2+build.7 | carries the snapshot at sequence 5 and events 6 to 7 |
      | 1.5.0+build.1 | carries no snapshot and events 0 to 7                |

  # ===========================================================================
  # Registering and reporting a version
  # ===========================================================================

  @rules_version
  @C-0558
  Scenario Outline: An answer reporting another version than the registered one is refused
    When business code answers an AddItem command sent <how> with an ItemAdded event, reporting rules version "<reported>"
    Then no events are persisted
    And <consequence>

    Examples:
      | how                       | reported      | consequence                                         |
      | awaiting the decision     | 1.5.0+build.9 | the caller is answered with INTERNAL status         |
      | awaiting the decision     |               | the caller is answered with INTERNAL status         |
      | awaiting the projections  | 1.4.2+build.8 | the caller is answered with INTERNAL status         |
      | without waiting           | 1.5.0+build.9 | the command is dead-lettered and an alert is raised |
      | by a policy                 | 1.5.0+build.9 | the command is dead-lettered and an alert is raised |

  @rules_version
  @C-0748
  Scenario: An answer reporting another version is refused under its key
    # Decision 14.56: a bug, fatal; an asynchronous or reaction delivery is
    # dead-lettered with the same key as its reason (14.48). The key
    # concerns one stream, so it names it (decision 14.65).
    When business code answers an AddItem command for order "order-1" sent awaiting the decision with an ItemAdded event, reporting rules version "1.5.0+build.9"
    Then no events are persisted
    And the caller is answered INTERNAL with reason ANGZARR_REASON_RULES_VERSION_MISMATCH, stream the stream of order "order-1", component "order", registered "1.4.2+build.7" and reported "1.5.0+build.9"

  @rules_version
  @C-0818
  Scenario: An answer whose state is not the registered state type is refused under its key
    # Decision 14.102: the coordinator owns every descriptor check; an
    # answer whose state type is not StateDescriptor.type_name would
    # never be offered its snapshots back. A bug, fatal.
    When business code answers an AddItem command for order "order-1" sent awaiting the decision with an ItemAdded event and a state of type "/orders.v1.LegacyOrderState", reporting rules version "1.4.2+build.7"
    Then no events are persisted and no snapshot is stored
    And the caller is answered INTERNAL with reason ANGZARR_REASON_STATE_TYPE_MISMATCH, stream the stream of order "order-1", component "order", registered "orders.v1.OrderState" and state_type "orders.v1.LegacyOrderState"

  @rules_version
  @C-0559
  Scenario: A process manager answer reporting another version is refused
    Given a process manager "Fulfillment" registered with rules version "2.0.1"
    When a policy's StartFulfillment command is answered by the process manager with a FulfillmentStarted event, reporting rules version "2.0.2"
    Then no FulfillmentStarted event is persisted
    And the command is dead-lettered and an alert is raised

  # ===========================================================================
  # Where snapshots come from
  # ===========================================================================

  @snapshot_source
  @C-0568
  Scenario: The state returned with an answer is stored and offered, unchanged, with the next command
    When business code answers an AddItem command for order "order-1" with an ItemAdded event at sequence 3, reporting rules version "1.4.2+build.7"
    Then a snapshot of order "order-1" is stored at sequence 3 with rules version "1.4.2+build.7"
    When the coordinator delivers the next AddItem command for order "order-1"
    Then business code receives that snapshot's stored bytes unchanged

  @snapshot_source
  @C-0569
  Scenario: By default only the newest snapshot is kept
    Given the "order" domain has the default snapshot storage policy
    When business code answers three AddItem commands for order "order-1"
    Then order "order-1" has exactly one stored snapshot
    And it is the state returned with the third answer

