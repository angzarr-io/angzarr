@compensation @wip
Feature: Compensation Flow - Framework Emits Notification
  When a saga or process manager issues a command that gets rejected by the
  target aggregate, the system must notify the original components so they
  can compensate (undo partial work, update their state, or retry).

  Why automatic notification matters:
  - Sagas are stateless - they can't poll for rejection status
  - The source aggregate needs to know its triggered action failed
  - Process managers need to update their workflow state
  - Without notification, partial operations would remain uncommitted forever

  Patterns enabled by compensation notification:
  - Distributed saga rollback: Each step in a multi-step saga can be undone
    when a downstream step fails.
  - Workflow state tracking: PMs record which step failed for reporting and
    retry logic.
  - Source aggregate cleanup: The originating aggregate releases held resources.

  The flow:
  1. Saga/PM issues command (triggered by source event)
  2. Target aggregate rejects with FAILED_PRECONDITION
  3. Framework wraps rejection in Notification message
  4. Framework routes Notification back through the chain:
     - To PM first (if PM issued the command) - can update workflow state
     - To source aggregate - can emit compensation events

  Background:
    Given the angzarr framework is initialized

  # ============================================================================
  # Saga-issued command rejected
  # ============================================================================
  # Sagas translate events from domain A into commands for domain B.
  # When domain B rejects the command, domain A needs to compensate.

  @emit @saga
  Scenario: Saga rejection triggers Notification creation
    Given a SourceAggregate that emitted ResourceReserved
    And a cross-domain-saga listening for ResourceReserved, issuing CommandThatWillFail to target domain
    When the TargetAggregate rejects CommandThatWillFail with "precondition_not_met"
    Then the framework creates a Notification containing:
      | issuer_name      | saga-cross-domain    |
      | rejection_reason | precondition_not_met |
      | rejected_command | CommandThatWillFail  |

  @emit @saga
  Scenario: Notification routes back to the source aggregate
    Given SourceAggregate emitted ResourceReserved which triggered cross-domain-saga which was rejected
    When the framework routes the rejection
    Then SourceAggregate receives the Notification
    And the notification identifies source_event_type "ResourceReserved"

  @emit @saga
  Scenario: Notification preserves full command context for debugging
    Given a saga command targeting root-123 with correlation_id corr-456
    When the command is rejected with reason "precondition_not_met"
    Then the Notification contains the full rejected command
    And includes cover.domain, cover.root for routing
    And includes rejection_reason for compensation logic decisions

  # ============================================================================
  # PM-issued command rejected
  # ============================================================================
  # Process managers coordinate multi-domain workflows. When a step fails,
  # the PM needs to update its workflow state AND the source aggregate needs
  # to compensate. The framework routes to both.

  @emit @pm
  Scenario: PM rejection creates Notification with PM identity
    Given SourceAggregate emitted WorkflowTriggered
    And WorkflowPM reacts by issuing CommandThatWillFail to target domain
    When TargetAggregate rejects with "step_failed"
    Then a Notification is created identifying:
      | issuer_name      | pmg-workflow  |
      | rejection_reason | step_failed   |

  @emit @pm
  Scenario: PM receives Notification before source aggregate
    Given SourceAggregate triggered WorkflowPM which issued a command that was rejected
    When the framework routes the rejection
    Then WorkflowPM receives the Notification first
    And can update its workflow state
    Then SourceAggregate receives the Notification second
    And can emit compensation events

  @emit @pm
  Scenario: Notification links back to PM's correlation context
    Given WorkflowPM tracking workflow-789 at step "awaiting_response"
    When its CommandThatWillFail is rejected
    Then the Notification includes correlation_id linking to this PM instance
    And the PM can load its state to make compensation decisions

  # ============================================================================
  # Rejection triggers
  # ============================================================================
  # Not all errors trigger compensation. Only business rejections do.

  @emit
  Scenario: Business rejection (FAILED_PRECONDITION) triggers compensation
    # FAILED_PRECONDITION = "I understood your request but can't fulfill it"
    Given a saga issues a command to an aggregate
    When the aggregate returns gRPC FAILED_PRECONDITION
    Then the framework creates a Notification
    And routes it for compensation

  @emit
  Scenario: Input validation errors do not trigger compensation
    # INVALID_ARGUMENT = "your request is malformed"
    # This is a bug in the caller, not a business condition.
    Given a saga issues a malformed command
    When the aggregate returns gRPC INVALID_ARGUMENT
    Then no Notification is created
    And the error propagates to the original caller

  @emit
  Scenario: Notification captures full provenance for debugging
    Given this chain of components:
      | step | component         | action                      |
      | 1    | SourceAggregate   | emits WorkflowTriggered     |
      | 2    | WorkflowPM        | issues CommandThatWillFail  |
      | 3    | TargetAggregate   | rejects (precondition)      |
    When the framework creates the Notification
    Then it contains the full provenance:
      | source_event_type | WorkflowTriggered       |
      | rejected_command  | CommandThatWillFail     |
      | issuer_type       | process_manager         |
      | issuer_name       | pmg-workflow            |

  # ============================================================================
  # Edge cases
  # ============================================================================

  @emit @edge
  Scenario: Multi-command saga stops on first rejection
    Given a saga that issues commands sequentially:
      | command              | target |
      | FirstCommand         | agg-a  |
      | SecondCommand        | agg-b  |
    When FirstCommand is rejected with "first_failed"
    Then SecondCommand is never issued
    And exactly one Notification is created for the first rejection

  @emit @edge
  Scenario: Nested PM chain bubbles rejection through all levels
    Given a PM chain: OuterPM issues to InnerPM issues to TargetAggregate
    When TargetAggregate rejects the command
    Then Notifications route through the chain in reverse:
      | order | component       | action                    |
      | 1     | InnerPM         | updates workflow state    |
      | 2     | OuterPM         | updates workflow state    |
      | 3     | SourceAggregate | emits compensation events |
