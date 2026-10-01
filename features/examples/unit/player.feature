# docs:start:feature_overview
Feature: Player aggregate logic
  The Player aggregate manages a player's bankroll and table reservations.
  It's the source of truth for how much money a player has and where it's allocated.
# docs:end:feature_overview

  # Why this aggregate exists:
  # - Players can only sit at tables if they have funds to reserve
  # - Reserved funds are locked until the table session ends
  # - Withdrawals cannot touch reserved funds (preventing mid-game cashout)
  #
  # What breaks if this is wrong:
  # - Players could buy into tables they can't afford
  # - Funds could be double-spent across multiple tables
  # - Players could withdraw chips currently in play
  #
  # Patterns enabled by this aggregate:
  # - Two-phase reservation: ReserveFunds locks money, ReleaseFunds returns it. This
  #   pattern applies anywhere resources must be held pending confirmation (e-commerce
  #   inventory holds, ticket reservations, hotel bookings).
  # - Saga compensation: When JoinTable fails, FundsReserved must be undone via
  #   FundsReleased. The player aggregate handles the Notification and compensates.
  # - Balance tracking with allocation: Available vs reserved funds. Same pattern
  #   applies to inventory (available vs allocated), accounts (balance vs holds).
  #
  # Why poker exercises these patterns well:
  # - Fund reservation is explicit: $500 reserved for Table-1 is clearly separate
  #   from the $500 available balance - easy to verify in tests
  # - Compensation is visible: A rejected JoinTable must release exactly the reserved
  #   amount - the math is obvious and testable
  # - Multiple concurrent reservations: A player at 3 tables has 3 separate holds,
  #   exercising the allocation tracking thoroughly

  # ==========================================================================
  # Player Registration
  # ==========================================================================
  # Players must register before participating. Registration captures identity
  # and distinguishes human players from AI bots (for fair play tracking).

  # docs:start:registration_scenarios
  Scenario: Register a new human player
    Given no prior events for the player aggregate
    When I handle a RegisterPlayer command with name "Alice" and email "alice@example.com"
    Then the result is a examples.PlayerRegistered event
    And the player event has display_name "Alice"
    And the player event has player_type "HUMAN"

  Scenario: Cannot register player twice
    Given a PlayerRegistered event for "Alice"
    When I handle a RegisterPlayer command with name "Alice2" and email "alice@example.com"
    Then the command fails with status "FAILED_PRECONDITION"
    And the error message contains "already exists"
  # docs:end:registration_scenarios

  Scenario: Register an AI player
    Given no prior events for the player aggregate
    When I handle a RegisterPlayer command with name "Bot1" and email "bot1@example.com" as AI
    Then the result is a examples.PlayerRegistered event
    And the player event has player_type "AI"

  # ==========================================================================
  # Deposits - Adding Funds to Bankroll
  # ==========================================================================
  # Deposits increase the player's bankroll. The full amount becomes available
  # for table buy-ins or withdrawals. Deposits are always allowed for registered
  # players (no upper limit by default).

  Scenario: Deposit funds to bankroll
    Given a PlayerRegistered event for "Alice"
    When I handle a DepositFunds command with amount 1000
    Then the result is a examples.FundsDeposited event
    And the player event has amount 1000
    And the player event has new_balance 1000

  Scenario: Multiple deposits accumulate
    Given a PlayerRegistered event for "Alice"
    And a FundsDeposited event with amount 500
    When I handle a DepositFunds command with amount 300
    Then the result is a examples.FundsDeposited event
    And the player event has new_balance 800

  Scenario: Cannot deposit to non-existent player
    Given no prior events for the player aggregate
    When I handle a DepositFunds command with amount 1000
    Then the command fails with status "FAILED_PRECONDITION"
    And the error message contains "does not exist"

  Scenario: Cannot deposit zero or negative
    Given a PlayerRegistered event for "Alice"
    When I handle a DepositFunds command with amount 0
    Then the command fails with status "INVALID_ARGUMENT"
    And the error message contains "positive"

  # ==========================================================================
  # Withdrawals - Removing Funds from Bankroll
  # ==========================================================================
  # Withdrawals remove funds from the player's bankroll. Only AVAILABLE funds
  # can be withdrawn - reserved funds (chips at tables) are locked until
  # the player leaves the table. This prevents mid-session cashouts.

  Scenario: Withdraw funds from bankroll
    Given a PlayerRegistered event for "Alice"
    And a FundsDeposited event with amount 1000
    When I handle a WithdrawFunds command with amount 400
    Then the result is a examples.FundsWithdrawn event
    And the player event has amount 400
    And the player event has new_balance 600

  Scenario: Cannot withdraw more than available
    Given a PlayerRegistered event for "Alice"
    And a FundsDeposited event with amount 500
    When I handle a WithdrawFunds command with amount 600
    Then the command fails with status "FAILED_PRECONDITION"
    And the error message contains "insufficient"

  Scenario: Cannot withdraw with funds reserved
    Given a PlayerRegistered event for "Alice"
    And a FundsDeposited event with amount 1000
    And a FundsReserved event with amount 800 for table "table-1"
    When I handle a WithdrawFunds command with amount 300
    Then the command fails with status "FAILED_PRECONDITION"
    And the error message contains "insufficient"

  # ==========================================================================
  # Fund Reservation - Locking Funds for Table Buy-ins
  # ==========================================================================
  # When a player joins a table, funds are RESERVED (not spent). Reserved
  # funds are locked against withdrawal but still belong to the player.
  # This two-phase pattern (reserve → release) enables saga compensation:
  # if the table join fails, the reservation is released atomically.

  # docs:start:reservation_scenario
  Scenario: Reserve funds for table buy-in
    Given a PlayerRegistered event for "Alice"
    And a FundsDeposited event with amount 1000
    When I handle a ReserveFunds command with amount 500 for table "table-1"
    Then the result is a examples.FundsReserved event
    And the player event has amount 500
    And the player event has new_available_balance 500
  # docs:end:reservation_scenario

  Scenario: Cannot reserve more than available
    Given a PlayerRegistered event for "Alice"
    And a FundsDeposited event with amount 500
    When I handle a ReserveFunds command with amount 600 for table "table-1"
    Then the command fails with status "FAILED_PRECONDITION"
    And the error message contains "insufficient"

  Scenario: Cannot reserve for same table twice
    Given a PlayerRegistered event for "Alice"
    And a FundsDeposited event with amount 1000
    And a FundsReserved event with amount 500 for table "table-1"
    When I handle a ReserveFunds command with amount 200 for table "table-1"
    Then the command fails with status "FAILED_PRECONDITION"
    And the error message contains "already reserved for this table"

  # ==========================================================================
  # Fund Release - Returning Reserved Funds
  # ==========================================================================
  # When a player leaves a table, their stack (remaining chips) is released
  # back to available balance. The release amount may differ from reservation
  # if the player won or lost chips during play.

  Scenario: Release reserved funds back to bankroll
    Given a PlayerRegistered event for "Alice"
    And a FundsDeposited event with amount 1000
    And a FundsReserved event with amount 500 for table "table-1"
    When I handle a ReleaseFunds command for table "table-1"
    Then the result is a examples.FundsReleased event
    And the player event has amount 500
    And the player event has new_available_balance 1000

  Scenario: Cannot release non-existent reservation
    Given a PlayerRegistered event for "Alice"
    And a FundsDeposited event with amount 1000
    When I handle a ReleaseFunds command for table "table-1"
    Then the command fails with status "FAILED_PRECONDITION"
    And the error message contains "No funds reserved"

  # ==========================================================================
  # State Reconstruction
  # ==========================================================================
  # Player state is rebuilt by replaying all events in order. This verifies
  # that the event sequence correctly captures the full financial history.

  Scenario: Rebuild state with deposits and reservations
    Given a PlayerRegistered event for "Alice"
    And a FundsDeposited event with amount 1000
    And a FundsReserved event with amount 400 for table "table-1"
    When I rebuild the player state
    Then the player state has bankroll 1000
    And the player state has reserved_funds 400
    And the player state has available_balance 600

  # ==========================================================================
  # Compensation Flow - Releasing Reserved Funds
  # ==========================================================================
  # When a table join is rejected or a player leaves, reserved funds must be
  # released back to their available balance. This exercises the compensation
  # pattern where a failed operation triggers a compensating action.

  Scenario: Funds released when table join is rejected
    Given a PlayerRegistered event for "Alice"
    And a FundsDeposited event with amount 500
    And a FundsReserved event with amount 200 for table "high-stakes"
    When I handle a ReleaseFunds command for table "high-stakes"
    Then the result is a examples.FundsReleased event
    And the player event has amount 200
    And the player event has new_available_balance 500
