Feature: Merge Strategy - Concurrency Control
  The MergeStrategy enum controls how the aggregate coordinator handles sequence
  conflicts when multiple commands target the same aggregate concurrently.

  Three strategies are available:
  - STRICT: Reject mismatched sequences immediately (optimistic concurrency)
  - COMMUTATIVE: Return retryable error, allowing client to reload and retry
  - AGGREGATE_HANDLES: Bypass coordinator validation, let aggregate decide

  Why different strategies exist:
  - Not all operations need the same concurrency semantics
  - Some operations are order-dependent (fund transfers), others are not (counters)
  - Framework provides the mechanism; business logic chooses the policy

  Patterns enabled by merge strategies:
  - STRICT enables saga compensation: if the target rejects, the source compensates.
    Same pattern applies to payment processing, inventory allocation.
  - COMMUTATIVE enables automatic retry: framework handles reload/retry transparently.
    Same pattern applies to idempotent operations, eventual consistency scenarios.
  - AGGREGATE_HANDLES enables CRDT-style operations: counters, sets, last-writer-wins.
    Same pattern applies to distributed counters, collaborative editing.

  Why poker exercises merge strategy patterns well:
  - STRICT for fund operations: ReserveFunds must see current balance to prevent
    over-reserving. Two players can't both reserve the same $500.
  - COMMUTATIVE for non-critical updates: AddBonusPoints can retry automatically
    if another operation updated the player concurrently.
  - AGGREGATE_HANDLES for visit tracking: IncrementVisits doesn't care about
    current sequence - just add 1 to whatever the current value is.

  Background:
    Given an aggregate "player" with initial events:
      | sequence | type             |
      | 0        | PlayerRegistered |
      | 1        | FundsDeposited   |
      | 2        | FundsDeposited   |
    # Aggregate is at sequence 3 (next expected)

  # ===========================================================================
  # MERGE_STRICT - Optimistic Concurrency (Fail Fast)
  # ===========================================================================
  # Use when: Commands MUST see latest state before execution.
  # Behavior: Immediate rejection on sequence mismatch.
  # Client action: Fetch fresh state, re-evaluate, resubmit.

  @merge_strict
  Scenario: Strict - command at correct sequence succeeds
    Given a command with merge_strategy STRICT
    And the command targets sequence 3
    When the coordinator processes the command
    Then the command succeeds
    And events are persisted

  @merge_strict
  Scenario: Strict - command at stale sequence is rejected
    Given a command with merge_strategy STRICT
    And the command targets sequence 2
    When the coordinator processes the command
    Then the command fails with ABORTED status
    And the error message contains "Sequence mismatch"
    And no events are persisted

  @merge_strict
  Scenario: Strict - command at future sequence is rejected
    Given a command with merge_strategy STRICT
    And the command targets sequence 5
    When the coordinator processes the command
    Then the command fails with ABORTED status
    And the error message contains "Sequence mismatch"

  @merge_strict
  Scenario: Strict - rejection includes current state for client retry
    Given a command with merge_strategy STRICT
    And the command targets sequence 1
    When the coordinator processes the command
    Then the command fails with ABORTED status
    And the error details include the current EventBook
    And the EventBook shows next_sequence 3

  # ===========================================================================
  # MERGE_COMMUTATIVE - Automatic Retry Support
  # ===========================================================================
  # Use when: Commands can be safely re-executed with fresh state.
  # Behavior: Returns retryable error with fresh state.
  # Client action: Reload state from error, rebuild command, retry automatically.
  #
  # This is the DEFAULT strategy (enum value 0).

  @merge_commutative
  Scenario: Commutative - command at correct sequence succeeds
    Given a command with merge_strategy COMMUTATIVE
    And the command targets sequence 3
    When the coordinator processes the command
    Then the command succeeds
    And events are persisted

  @merge_commutative
  Scenario: Commutative - command at stale sequence returns retryable error
    Given a command with merge_strategy COMMUTATIVE
    And the command targets sequence 1
    When the coordinator processes the command
    Then the command fails with FAILED_PRECONDITION status
    And the error is marked as retryable
    And the error details include the current EventBook

  @merge_commutative
  Scenario: Commutative - client can retry with fresh state
    Given a command with merge_strategy COMMUTATIVE
    And the command targets sequence 1
    When the coordinator processes the command
    Then the command fails with FAILED_PRECONDITION status
    When the client extracts the EventBook from the error
    And rebuilds the command with sequence 3
    And resubmits the command
    Then the command succeeds

  @merge_commutative
  Scenario: Commutative - saga automatic retry on conflict
    Given a saga emits a command with merge_strategy COMMUTATIVE
    And the destination aggregate has advanced
    When the saga coordinator executes the command
    Then the command fails with retryable status
    And the saga retries with backoff
    And the saga fetches fresh destination state
    And the retried command succeeds

  @merge_commutative
  Scenario: Commutative - default strategy when unspecified
    Given a command with no explicit merge_strategy
    When the coordinator processes the command
    Then the effective merge_strategy is COMMUTATIVE

  # ===========================================================================
  # MERGE_AGGREGATE_HANDLES - Aggregate-Managed Concurrency
  # ===========================================================================
  # Use when: Aggregate has domain-specific concurrency logic.
  # Behavior: Coordinator skips sequence validation entirely.
  # Aggregate action: Receives full EventBook, implements own conflict resolution.
  #
  # Examples: Counter aggregates, set operations, CRDTs

  @merge_aggregate_handles
  Scenario: AggregateHandles - command bypasses coordinator validation
    Given a command with merge_strategy AGGREGATE_HANDLES
    And the command targets sequence 0
    When the coordinator processes the command
    Then the coordinator does NOT validate the sequence
    And the aggregate handler is invoked
    And the aggregate receives the prior EventBook

  @merge_aggregate_handles
  Scenario: AggregateHandles - aggregate can accept stale sequence
    Given a command with merge_strategy AGGREGATE_HANDLES
    And the command targets sequence 1
    And the aggregate accepts the command
    When the coordinator processes the command
    Then the command succeeds
    And events are persisted at the correct sequence

  @merge_aggregate_handles
  Scenario: AggregateHandles - aggregate can reject based on state
    Given a command with merge_strategy AGGREGATE_HANDLES
    And the command targets sequence 1
    And the aggregate rejects due to state conflict
    When the coordinator processes the command
    Then the command fails with aggregate's error
    And no events are persisted

  @merge_aggregate_handles
  Scenario: AggregateHandles - counter increment is commutative
    Given a counter aggregate at value 10
    And two concurrent IncrementBy commands:
      | client | amount | sequence |
      | A      | 5      | 0        |
      | B      | 3      | 0        |
    When both commands use merge_strategy AGGREGATE_HANDLES
    And both are processed
    Then both commands succeed
    And the final counter value is 18
    And no sequence conflicts occur

  @merge_aggregate_handles
  Scenario: AggregateHandles - set addition is idempotent
    Given a set aggregate containing ["apple", "banana"]
    And two concurrent AddItem commands for "cherry":
      | client | sequence |
      | A      | 0        |
      | B      | 0        |
    When both commands use merge_strategy AGGREGATE_HANDLES
    And both are processed
    Then the first command succeeds with ItemAdded event
    And the second command succeeds with no event (idempotent)
    And the set contains ["apple", "banana", "cherry"]

  # ===========================================================================
  # Cross-Strategy Scenarios
  # ===========================================================================

  @merge_strategy
  Scenario Outline: Strategy determines conflict response
    Given a command with merge_strategy <strategy>
    And the command targets sequence 1
    And the aggregate is at sequence 3
    When the coordinator processes the command
    Then the response status is <status>
    And the behavior is <behavior>

    Examples:
      | strategy          | status              | behavior                        |
      | STRICT            | ABORTED             | immediate rejection             |
      | COMMUTATIVE       | FAILED_PRECONDITION | retryable with fresh state      |
      | AGGREGATE_HANDLES | varies              | aggregate decides               |

  @merge_strategy
  Scenario: Different commands can use different strategies
    Given commands for the same aggregate:
      | command         | merge_strategy    |
      | ReserveFunds    | STRICT            |
      | AddBonusPoints  | COMMUTATIVE       |
      | IncrementVisits | AGGREGATE_HANDLES |
    When processed with sequence conflicts
    Then ReserveFunds is rejected immediately
    And AddBonusPoints is retryable
    And IncrementVisits delegates to aggregate

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  @merge_strategy @edge_case
  Scenario: New aggregate - all strategies accept sequence 0
    Given a new aggregate with no events
    And a command targeting sequence 0
    When the command uses merge_strategy <strategy>
    Then the command succeeds

    Examples:
      | strategy          |
      | STRICT            |
      | COMMUTATIVE       |
      | AGGREGATE_HANDLES |

  @merge_strategy @edge_case
  Scenario: Snapshot affects next_sequence calculation
    Given an aggregate with snapshot at sequence 50
    And events at sequences 51, 52
    And the next expected sequence is 53
    When a STRICT command targets sequence 53
    Then the command succeeds

  @merge_strategy @edge_case
  Scenario: Empty command pages uses default strategy
    Given a CommandBook with no pages
    When merge_strategy is extracted
    Then the result is COMMUTATIVE
