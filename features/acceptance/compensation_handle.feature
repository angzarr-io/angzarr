@compensation @wip
Feature: Compensation Flow - Components Handle Notification
  When a Notification arrives (indicating a downstream command was rejected),
  the source component must compensate - typically by emitting events that
  undo the partial work that triggered the failed command.

  Why components handle their own compensation:
  - Business logic knows what to undo (release reserved resources, cancel pending action)
  - Compensation may vary by rejection reason (retry vs cancel vs escalate)
  - Framework can't know domain-specific rollback semantics

  Patterns enabled by component compensation handling:
  - Domain-specific rollback: Only the aggregate knows how to undo its operations.
  - Reason-based branching: Different rejection reasons may need different
    compensation. "temporary_failure" might retry; "permanent_failure" might not.
  - State-aware compensation: Handler can access current state to calculate
    correct compensation amount.

  Two patterns for handling:
  - @rejected decorator (OO): method annotated with domain/command it handles
  - on_rejected() fluent API: functional style for simpler aggregates

  If no handler matches, the framework emits a generic revocation event,
  which may be insufficient for complex business compensation.

  Background:
    Given the angzarr framework is initialized

  # ============================================================================
  # Aggregate @rejected decorator - OO pattern
  # ============================================================================
  # The @rejected decorator routes Notifications to the right handler based
  # on which domain/command was rejected. The handler emits compensation events.

  @handle @aggregate @oo
  Scenario: Rejection routes to handler matching domain and command
    Given SourceAggregate has reserved_amount 500
    And a @rejected handler registered for domain "target" command "CommandThatWillFail"
    When SourceAggregate receives Notification for target/CommandThatWillFail rejection
    Then the matching @rejected handler is invoked
    And receives the Notification with rejection reason and failed command
    And can access current aggregate state to calculate compensation

  @handle @aggregate @oo
  Scenario: Handler emits events to compensate for the failed operation
    Given SourceAggregate with reserved_amount 500
    And a @rejected handler that returns ResourceReleased event
    When the handler processes a target rejection with reason "precondition_not_met"
    Then ResourceReleased is emitted with:
      | amount | 500                              |
      | reason | Command failed: precondition_not_met |

  @handle @aggregate @oo
  Scenario: Compensation events are applied and persisted atomically
    Given SourceAggregate with reserved_amount 100
    When @rejected handler returns ResourceReleased
    Then state.reserved_amount becomes 0 after event applied
    And ResourceReleased is added to the event book

  @handle @aggregate @oo
  Scenario: Multiple handlers route to the correct one by domain/command
    Given SourceAggregate has @rejected handlers for:
      | domain  | command              | handler                |
      | target  | CommandThatWillFail  | handle_target_rejected |
      | other   | OtherCommand         | handle_other_rejected  |
    When a rejection arrives for target/CommandThatWillFail
    Then handle_target_rejected is called
    And handle_other_rejected is NOT called

  @handle @aggregate @oo
  Scenario: Missing handler delegates compensation to framework
    Given SourceAggregate has no @rejected handlers configured
    When SourceAggregate receives a Notification
    Then response.emit_system_revocation = true
    And reason indicates "no custom compensation handler"

  # ============================================================================
  # Process Manager @rejected decorator - OO pattern
  # ============================================================================
  # PMs receive Notifications BEFORE the source aggregate. This lets them
  # update workflow state (mark step failed, decide retry) before compensation.

  @handle @pm @oo
  Scenario: PM rejection routes to handler matching the failed command
    Given WorkflowPM at step "awaiting_response" for workflow-123
    And a @rejected handler for domain "target" command "TargetCommand"
    When PM receives Notification for target/TargetCommand rejection
    Then the matching @rejected handler is invoked
    And receives the Notification with rejection details
    And can access PM state including current step and workflow_id

  @handle @pm @oo
  Scenario: PM handler emits workflow events (not aggregate events)
    Given WorkflowPM tracking workflow-123
    And @rejected handler returning WorkflowStepFailed
    When PM handles target rejection with reason "step_failed"
    Then WorkflowStepFailed is recorded in PM's event stream:
      | workflow_id | workflow-123              |
      | reason      | Step failed: step_failed  |
      | step        | awaiting_response         |

  @handle @pm @oo
  Scenario: After PM handles rejection, framework routes to source aggregate
    Given WorkflowPM handles a rejection
    When PM @rejected handler completes
    Then PM events are persisted
    And framework routes Notification to source aggregate next

  @handle @pm @oo
  Scenario: PM without handler delegates to framework
    Given WorkflowPM with no @rejected handlers
    When PM receives a Notification
    Then PM returns no process events
    And emit_system_revocation = true

  # ============================================================================
  # CommandRouter.on_rejected() - Fluent pattern
  # ============================================================================
  # Alternative to @rejected decorator. Same routing logic, functional style.

  @handle @aggregate @fluent
  Scenario: Fluent API routes rejections to registered handlers
    Given CommandRouter for "source" domain configured with:
      | type        | key                        | handler                |
      | on          | CreateResource             | handle_create          |
      | on_rejected | target/CommandThatWillFail | handle_target_rejected |
    When Notification arrives for target/CommandThatWillFail rejection
    Then handle_target_rejected receives:
      | notification | the Notification details   |
      | state        | current aggregate state    |

  @handle @aggregate @fluent
  Scenario: Fluent handler returns EventBook with compensation events
    Given CommandRouter with on_rejected handler
    And handler builds EventBook containing ResourceReleased
    When router dispatches the Notification
    Then BusinessResponse contains the EventBook
    And ResourceReleased will be persisted and applied

  @handle @aggregate @fluent
  Scenario: No matching fluent handler delegates to framework
    Given CommandRouter with no on_rejected handlers
    When router dispatches a Notification
    Then BusinessResponse has emit_system_revocation = true
    And reason indicates "no custom compensation handler"

  @handle @aggregate @fluent
  Scenario: Multiple on_rejected handlers in fluent chain
    Given CommandRouter configured as:
      """
      CommandRouter("source", rebuild)
        .on("CreateResource", handle_create)
        .on_rejected("target", "CommandThatWillFail", handle_target)
        .on_rejected("other", "OtherCommand", handle_other)
      """
    When rejection arrives for target/CommandThatWillFail
    Then handle_target is called
    And handle_other is NOT called

  # ============================================================================
  # Dispatch key extraction
  # ============================================================================
  # The router must extract domain/command from the Notification to find
  # the right handler. This tests the key extraction logic.

  @handle @dispatch
  Scenario: Domain extracted from rejected command's cover
    Given Notification with rejected_command.cover.domain = "target"
    When router extracts dispatch key
    Then domain part = "target"

  @handle @dispatch
  Scenario: Command type extracted from type_url
    Given Notification with rejected_command.type_url = "type.googleapis.com/test.CommandThatWillFail"
    When router extracts dispatch key
    Then command part = "CommandThatWillFail"

  @handle @dispatch
  Scenario: Full dispatch key combines domain and command
    Given Notification with domain "target" and command "CommandThatWillFail"
    When router builds dispatch key
    Then key = "target/CommandThatWillFail"

  # ============================================================================
  # Compensation event recording
  # ============================================================================

  @handle @events
  Scenario: Compensation events have proper metadata
    Given a SourceAggregate handling rejection
    When a ResourceReleased compensation event is emitted
    Then the event has a created_at timestamp
    And the event has the correct sequence number
    And the event is packed as Any with proper type_url

  @handle @events
  Scenario: Multiple compensation events in single handler
    Given an aggregate @rejected handler returning multiple events:
      | event              |
      | ReservationCancelled |
      | RefundIssued         |
    When the handler completes
    Then both events are in the EventBook
    And the events have sequential sequence numbers

  # ============================================================================
  # State access during compensation
  # ============================================================================

  @handle @state
  Scenario: Aggregate state is rebuilt before rejection handling
    Given a SourceAggregate with prior events:
      | event            | amount |
      | ResourceAcquired | 500    |
      | ResourceReserved | 100    |
    When a rejection handler accesses state.balance
    Then the balance is 400
    And reserved_amount is 100

  @handle @state
  Scenario: PM state is rebuilt before rejection handling
    Given a WorkflowPM with prior events:
      | event           | data                  |
      | WorkflowStarted | workflow_id: wf-123   |
      | StepRequested   | step_count: 3         |
    When a rejection handler accesses state
    Then workflow_id is "wf-123"
    And step is "awaiting_response"

  # ============================================================================
  # Edge cases and error handling
  # ============================================================================

  @handle @edge
  Scenario: Handler throws error during compensation
    Given a @rejected handler that raises an exception
    When the aggregate handles the Notification
    Then the exception propagates
    And no compensation events are persisted
    And the framework can retry or escalate

  @handle @edge
  Scenario: Empty rejected_command in Notification
    Given a malformed Notification with no rejected_command
    When the router attempts dispatch
    Then the dispatch key is empty
    And no handler matches
    And framework delegation occurs

  @handle @edge
  Scenario: Handler returns None/empty
    Given a @rejected handler that returns None
    When the aggregate handles the rejection
    Then no events are added to the event book
    And the response still indicates success

  # ============================================================================
  # Integration with saga retry
  # ============================================================================

  @handle @retry
  Scenario: Compensation enables saga retry
    Given a saga with retry logic
    And the first attempt rejected with "temporary_failure"
    When compensation completes successfully
    Then the saga can retry the operation
    And the retry has fresh state

  @handle @retry
  Scenario: PM updates step after compensation
    Given a WorkflowPM in step "awaiting_response"
    And a @rejected handler that emits StepRolledBack
    When compensation completes
    Then the PM step changes to "step_failed"
    And the PM can transition to a recovery path
