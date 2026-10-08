# docs:start:compensation_contract
Feature: Compensation - Saga Rejection Handling
  When saga commands are rejected by target aggregates, compensation
  flows notify the source aggregate. The CompensationContext captures
  all information needed to build rejection notifications.

  Compensation enables:
  - Source aggregates to handle downstream failures
  - Maintaining consistency across domain boundaries
  - Tracking rejection chains for debugging
# docs:end:compensation_contract

  Background:
    Given a compensation handling context

  # ==========================================================================
  # CompensationContext Construction
  # ==========================================================================

  Scenario: Build context from rejected command
    Given a saga command that was rejected
    When I build a CompensationContext
    Then the context should include the rejected command
    And the context should include the rejection reason
    And the context should include the saga origin

  Scenario: Context preserves saga origin details
    Given a saga "order-fulfillment" triggered by "orders" aggregate at sequence 5
    And the saga command was rejected
    When I build a CompensationContext
    Then the saga_origin saga_name should be "order-fulfillment"
    And the triggering_aggregate should be "orders"
    And the triggering_event_sequence should be 5

  Scenario: Context preserves correlation ID
    Given a saga command with correlation ID "workflow-123"
    And the command was rejected
    When I build a CompensationContext
    Then the context correlation_id should be "workflow-123"

  # ==========================================================================
  # RejectionNotification Building
  # ==========================================================================

  Scenario: Build rejection notification from context
    Given a CompensationContext for rejected command
    When I build a RejectionNotification
    Then the notification should include the rejected command
    And the notification should include the rejection reason
    And the notification should have issuer_type "saga"

  Scenario: Rejection notification includes source aggregate
    Given a CompensationContext from "orders" aggregate at sequence 5
    When I build a RejectionNotification
    Then the source_aggregate should have domain "orders"
    And the source_event_sequence should be 5

  Scenario: Rejection notification has correct issuer name
    Given a CompensationContext from saga "order-fulfillment"
    When I build a RejectionNotification
    Then the issuer_name should be "order-fulfillment"
    And the issuer_type should be "saga"

  # ==========================================================================
  # Notification Building
  # ==========================================================================

  Scenario: Build notification wrapper for rejection
    Given a CompensationContext for rejected command
    When I build a Notification from the context
    Then the notification should have a cover
    And the notification payload should contain RejectionNotification
    And the payload type_url should be "type.googleapis.com/io.angzarr.v1.RejectionNotification"

  Scenario: Notification has sent_at timestamp
    When I build a Notification from a CompensationContext
    Then the notification should have a sent_at timestamp
    And the timestamp should be recent

  # ==========================================================================
  # Command Book Building
  # ==========================================================================

  Scenario: Build notification command book for routing
    Given a CompensationContext for rejected command
    When I build a notification CommandBook
    Then the command book should target the source aggregate
    And the command book should have MERGE_COMMUTATIVE strategy
    And the command book should preserve correlation ID

  Scenario: Command book targets triggering aggregate
    Given a CompensationContext from "orders" aggregate root "order-123"
    When I build a notification CommandBook
    Then the command book cover should have domain "orders"
    And the command book cover should have root "order-123"

  # ==========================================================================
  # Rejection Reason Handling
  # ==========================================================================

  Scenario: Rejection reason is preserved exactly
    Given a command rejected with reason "insufficient_funds"
    When I build a RejectionNotification
    Then the rejection_reason should be "insufficient_funds"

  Scenario: Complex rejection reason
    Given a command rejected with structured reason
    When I build a RejectionNotification
    Then the rejection_reason should contain the full error details

  # ==========================================================================
  # Chain of Command Tracking
  # ==========================================================================

  Scenario: Rejected command is preserved for debugging
    Given a saga command with specific payload
    And the command was rejected
    When I build a RejectionNotification
    Then the rejected_command should be the original command
    And all command fields should be preserved

  Scenario: Saga origin chain is maintained
    Given a nested saga scenario
    And an inner saga command was rejected
    When I build a RejectionNotification
    Then the full saga origin chain should be preserved
    And root cause can be traced through the chain

  # ==========================================================================
  # Integration with Routers
  # ==========================================================================

  Scenario: Compensation context works with saga router
    Given a saga router handling rejections
    When a command execution fails with precondition error
    Then the router should build a CompensationContext
    And the router should emit a rejection notification

  Scenario: Compensation context works with PM router
    Given a process manager router
    When a PM command is rejected
    Then the router should build a CompensationContext
    And the context should have issuer_type "process_manager"
