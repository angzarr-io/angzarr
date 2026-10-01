@container
Feature: End-to-End Container Integration
  These tests verify angzarr works as a deployed system, not just in-process.
  They exercise the full path: client → gateway → aggregate coordinator → storage.

  Why container tests matter:
  - Networking: gRPC actually crosses process boundaries
  - Serialization: proto messages survive real wire encoding
  - Configuration: Helm charts, env vars, and secrets work correctly
  - Lifecycle: Services start, connect, and handle requests properly

  Patterns exercised by container tests:
  - Wire protocol verification: proto messages encode/decode correctly across
    process boundaries. Same concern applies to any distributed system.
  - Service discovery: DNS resolution finds the right aggregate coordinator.
    Same concern applies to microservice deployments.
  - Configuration validation: Helm values, env vars, secrets all work together.
    Same concern applies to any containerized deployment.

  Why the blackjack example's player domain is used here:
  - Player aggregate is simple: RegisterPlayer → PlayerRegistered
  - Single command/event pair minimizes variables when debugging container issues
  - Easy to verify: "player exists with this name" is binary success/fail

  If unit/integration tests pass but container tests fail, the problem is
  in deployment configuration, not business logic.

  Background:
    Given the angzarr system is deployed and reachable at "localhost:9084"

  # ==========================================================================
  # Command Processing
  # ==========================================================================
  # Verify commands flow through the gateway to aggregates and persist events.

  @container
  Scenario: Commands reach aggregates through the gateway
    # The gateway routes commands by domain to the correct aggregate coordinator.
    # This verifies: DNS resolution, gRPC connectivity, proto compatibility.
    Given a new player aggregate (unique ID for test isolation)
    When a RegisterPlayer command is sent with name "Container Test" and email "container@test.com"
    Then the command succeeds (aggregate processed it)
    And a PlayerRegistered event was persisted
    And the aggregate's event count is 1

  # ==========================================================================
  # Event Query
  # ==========================================================================
  # Verify events can be queried back after persistence.

  @container
  Scenario: Persisted events are queryable
    # After commands succeed, events must be retrievable for:
    # - State reconstruction on next command
    # - Projector catch-up
    # - Debugging and auditing
    Given a player aggregate that has processed a RegisterPlayer command
    When we query that aggregate's event history
    Then we receive the PlayerRegistered event at sequence 0
    # This verifies event store connectivity and query path works

  # ==========================================================================
  # Projector Integration
  # ==========================================================================
  # Verify projectors receive events and return projections synchronously.

  @container
  Scenario: Synchronous projectors return results with command response
    # Some projectors are configured as synchronous - their output is included
    # in the command response. This enables read-after-write consistency.
    Given a new player aggregate
    When a RegisterPlayer command is processed
    Then the response includes any synchronous projections
    # Projections are returned when projector coordinators are configured
