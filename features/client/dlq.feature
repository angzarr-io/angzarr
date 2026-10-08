Feature: Dead-letter queue is operator-observable across handler types
  Failures that the framework cannot recover from must surface in the
  operator's DLQ admin so the missing-audit-trail bug class (R2-15) is
  closed. This feature pins the four cross-cutting contracts:

  1. Aggregate MergeManual sequence-mismatch routes the failed command
     to DLQ (the original site that R2-15 generalized from).
  2. Saga commands that return a permanent (4xx-class) error route to
     DLQ immediately; saga compensation still fires alongside.
  3. Saga commands that fail transient (5xx-class) errors run the
     retry budget; only retry-exhaustion routes to DLQ.
  4. Projector handlers that return permanent errors route to DLQ
     and the message is acked (no redelivery hot-loop). Transient
     errors propagate through the bus so its own retry/redelivery
     handles them.

  Classification follows
  `crate::dlq::trigger::CodeDlqExt::classify_for_dlq`: 4xx-class codes
  (`InvalidArgument`, `NotFound`, `FailedPrecondition`, `Aborted`,
  `Unimplemented`, `PermissionDenied`, `Unauthenticated`, `OutOfRange`,
  `AlreadyExists`) classify as `Immediate`; 5xx-class codes
  (`Unavailable`, `DeadlineExceeded`, `ResourceExhausted`, `Internal`,
  `Unknown`, `DataLoss`, `Cancelled`) classify as `RetryThenDlq`.

  Background:
    Given the operator configures dlq.targets with a database backend
    And the operator configures dlq.audit pointing at the same backend

  # ==========================================================================
  # Aggregate (R2-15 step 4)
  # ==========================================================================

  Scenario: Aggregate sequence-mismatch under MergeManual is dead-lettered
    Given an aggregate in MergeManual merge mode
    When the aggregate receives a stale command with a sequence mismatch
    Then the command is rejected with Aborted
    And the dead letter is visible via the status admin DLQ listing
    And the dead letter source_component_type is "aggregate"
    And the dead letter payload contains the rejected command

  # ==========================================================================
  # Saga (R2-15 step 5a)
  # ==========================================================================

  Scenario: Saga handler returns a permanent error
    Given a saga handler whose outbound command returns InvalidArgument
    When the saga receives an event that produces that command
    Then no retry is attempted for that command
    And the dead letter is visible via the status admin DLQ listing
    And the dead letter source_component_type is "saga"
    And the dead letter retry_count is 0
    And the dead letter is_transient is false
    And the dead letter carries the source event and the rejected command

  Scenario: Saga handler returns a transient error then succeeds
    Given a saga handler whose outbound command returns Unavailable on the first attempt
    When the saga receives an event that produces that command
    Then the framework retries the command with backoff
    And the eventual success is not dead-lettered

  Scenario: Saga handler returns a transient error until retry exhaustion
    Given a saga handler whose outbound command returns Unavailable on every attempt
    When the saga receives an event that produces that command
    Then the framework retries the command up to the configured backoff budget
    And the dead letter is visible via the status admin DLQ listing
    And the dead letter source_component_type is "saga"
    And the dead letter retry_count is greater than 0
    And the dead letter is_transient is true

  # ==========================================================================
  # Process Manager (R2-15 step 5b)
  # ==========================================================================

  Scenario: PM persistence retry-exhaustion is dead-lettered
    Given a process manager whose PM event persistence returns sequence-conflict on every attempt
    When the PM receives a trigger event
    Then the framework retries persistence up to the configured backoff budget
    And the dead letter is visible via the status admin DLQ listing
    And the dead letter source_component_type is "process_manager"
    And the dead letter payload contains the failed PM event book

  Scenario: PM command returning permanent error is dead-lettered
    Given a process manager whose outbound command returns InvalidArgument
    When the PM receives a trigger event that produces that command
    Then the dead letter is visible via the status admin DLQ listing
    And the dead letter source_component_type is "process_manager"
    And the dead letter retry_count is 0
    And the PM compensation handler is invoked alongside the dead letter

  Scenario: PM Decision-mode command degrading to Retryable is dead-lettered
    Given a process manager whose outbound command requests SYNC_MODE_DECISION
    And the executor returns Retryable for that command
    When the PM dispatches the command
    Then the framework degrades the outcome to a rejection
    And the dead letter is visible via the status admin DLQ listing
    And the dead letter source_component_type is "process_manager"
    And the dead letter rejection_reason names SYNC_MODE_DECISION

  # ==========================================================================
  # Projector (R2-15 step 6)
  # ==========================================================================

  Scenario: Projector handler returns a permanent error
    Given a projector handler that returns FailedPrecondition for a malformed payload
    When the projector receives that event
    Then the dead letter is visible via the status admin DLQ listing
    And the dead letter source_component_type is "projector"
    And the dead letter retry_count is 0
    And the message is acked from the bus
    And subsequent events for the same projector continue to be processed

  Scenario: Projector handler returns a transient error
    Given a projector handler that returns Unavailable
    When the projector receives an event
    Then no dead letter is published by the projector
    And the failure propagates to the bus for redelivery
