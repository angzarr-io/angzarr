Feature: Process manager delivery - a process manager is a domain, behind its own coordinator
  A process manager is one component, served by its own business process
  behind its own coordinator, exactly as an aggregate is (decision 14.31).
  Its inputs are commands, which it may refuse, and facts, which it never
  refuses; its root is the one its input's cover names (by convention a
  policy sets it to its trigger's correlation; decision 14.36). Which inputs
  may start an instance is the PM's declaration,
  ProcessManagerDecl.starting_types, copied into its descriptor (decision
  14.91); an input for a root with no instance, of any other type, is not
  delivered: a command is refused, a fact dead-lettered and its sender
  answered INTERNAL ANGZARR_REASON_FACT_DEAD_LETTERED
  (docs/design/process-managers.md §4; decision 14.100). It subscribes to no
  domain: policies turn other domains' events into commands and facts to it
  (decision 14.69). It records
  only its own events and sends no commands. The correlation ids below are
  UUIDs, as in every scenario (decision 14.18).

  @process_manager
  @C-0578
  Scenario: Two process managers told of one event each get their own delivery and state
    Given process managers "Fulfillment" and "Billing", each behind its own coordinator
    And a policy from "order" to "Fulfillment"'s domain and a policy from "order" to "Billing"'s domain, each turning OrderPlaced into a command
    And "Fulfillment" has state "awaiting stock" and "Billing" has state "awaiting payment" at the order's root
    When an OrderPlaced event is recorded for order "order-1"
    Then "Fulfillment"'s coordinator delivers its command to "Fulfillment" with its state "awaiting stock"
    And "Billing"'s coordinator delivers its command to "Billing" with its state "awaiting payment"
    And each answer's events are appended to that process manager's own stream
    And each answer's state is stored for that process manager alone

  # ===========================================================================
  # Starting an instance (ruling B1, adapted)
  # ===========================================================================

  @process_manager @pm_start
  @C-0592
  Scenario: A starting command for a root with no instance starts an instance
    Given a process manager "Fulfillment" owning domain "fulfillment" whose starting_types lists StartFulfillment
    And "Fulfillment" has no instance at root "0192a6b0-0000-7000-8000-000000000010"
    When a StartFulfillment command is sent to "fulfillment" root "0192a6b0-0000-7000-8000-000000000010"
    Then "Fulfillment"'s coordinator delivers the command to "Fulfillment" with an empty state
    And the events are appended to stream "fulfillment" root "0192a6b0-0000-7000-8000-000000000010"

  @process_manager @pm_start
  @C-0593
  Scenario: A non-starting fact for a root with no instance is dead-lettered, its sender answered INTERNAL
    # Owner's ruling (decision 14.69): dead-lettered, never skipped; a fact
    # is never refused, so its sender is answered INTERNAL
    # ANGZARR_REASON_FACT_DEAD_LETTERED, never retried (decision 14.100).
    Given a process manager "Fulfillment" owning domain "fulfillment" whose starting_types lists StartFulfillment
    And "Fulfillment" also takes a FulfillmentTimedOut fact, which its starting_types does not list
    And "Fulfillment" has no instance at root "0192a6b0-0000-7000-8000-000000000011"
    When a FulfillmentTimedOut fact is sent to "fulfillment" root "0192a6b0-0000-7000-8000-000000000011"
    Then "Fulfillment" is not called
    And nothing is appended to stream "fulfillment" root "0192a6b0-0000-7000-8000-000000000011"
    And the fact is dead-lettered with reason ANGZARR_REASON_PM_NO_INSTANCE, naming its stream and type_url, and an alert is raised
    And the sender is answered INTERNAL with reason ANGZARR_REASON_FACT_DEAD_LETTERED, cause ANGZARR_REASON_PM_NO_INSTANCE

  @process_manager @pm_start
  @C-0594
  Scenario: A non-starting input for a root with an instance is delivered
    Given a process manager "Fulfillment" owning domain "fulfillment" whose starting_types lists StartFulfillment
    And "Fulfillment" also takes a RecordStockReserved command, which its starting_types does not list
    And "Fulfillment" has an instance at root "0192a6b0-0000-7000-8000-000000000012" with state "awaiting stock"
    When a RecordStockReserved command is sent to "fulfillment" root "0192a6b0-0000-7000-8000-000000000012"
    Then "Fulfillment"'s coordinator delivers the command to "Fulfillment" with its state "awaiting stock"

  @process_manager @pm_start
  @C-0595
  Scenario: A non-starting command for a root with no instance is refused
    # Owner's ruling, decision 14.31: refused, recording nothing; a fact
    # is dead-lettered instead (C-0593).
    Given a process manager "Fulfillment" owning domain "fulfillment" whose starting_types lists StartFulfillment
    And "Fulfillment" also takes a RecordStockReserved command, which its starting_types does not list
    And "Fulfillment" has no instance at root "0192a6b0-0000-7000-8000-000000000013"
    When a client sends a RecordStockReserved command to "fulfillment" root "0192a6b0-0000-7000-8000-000000000013"
    Then the client is answered FAILED_PRECONDITION with reason ANGZARR_REASON_PM_NO_INSTANCE, naming its stream and type_url
    And "Fulfillment" is not called
    And nothing is appended to stream "fulfillment" root "0192a6b0-0000-7000-8000-000000000013"

  # ===========================================================================
  # Commands and facts, as for any domain
  # ===========================================================================

  @process_manager @pm_input
  @C-0596
  Scenario: A process manager may refuse a command, and records nothing
    Given a process manager "Fulfillment" owning domain "fulfillment" with an instance at root "0192a6b0-0000-7000-8000-000000000014" that is completed
    And "Fulfillment"'s CancelFulfillment lists the refusal AlreadyCompleted, declared FAILED_PRECONDITION, and refuses with it once completed
    When a client sends a CancelFulfillment command to "fulfillment" root "0192a6b0-0000-7000-8000-000000000014"
    Then the client is answered FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED, refusal_type AlreadyCompleted, and the AlreadyCompleted object in the status details
    And nothing is appended to stream "fulfillment" root "0192a6b0-0000-7000-8000-000000000014"

  @process_manager @pm_dedup
  @C-0597
  Scenario: A redelivered command is answered from its stored events, before the handler runs
    Given a process manager "Fulfillment" owning domain "fulfillment" whose starting_types lists StartFulfillment
    And a StartFulfillment command with command_id "0192a6b0-0000-7000-8000-0000000000c2" has been applied by "Fulfillment" at root "0192a6b0-0000-7000-8000-000000000015"
    When the same command is sent again
    Then "Fulfillment" is not called
    And the sender is answered DUPLICATE with the stored events

  # C-0598 (an input naming another instance than its correlation refused)
  # is retired: a PM instance's root is its correlation by default only,
  # and an input whose correlation differs from its cover's root is
  # delivered to that root, keeping its correlation (decision 14.36;
  # policy_addressing.feature, C-0659).

  @process_manager @pm_failure
  @C-0599
  Scenario: A failing fact handler dead-letters the fact and raises an alert
    Given a process manager "Fulfillment" owning domain "fulfillment" with an instance at root "0192a6b0-0000-7000-8000-000000000017"
    And "Fulfillment"'s FulfillmentTimedOut handler fails with INTERNAL
    When a client sends a FulfillmentTimedOut fact to "fulfillment" root "0192a6b0-0000-7000-8000-000000000017"
    Then the fact is dead-lettered as rejected_command, its reason the handler failure's key, and an alert is raised
    And the client is answered INTERNAL with reason ANGZARR_REASON_FACT_DEAD_LETTERED, naming the stream and its cause
    And nothing is appended to stream "fulfillment" root "0192a6b0-0000-7000-8000-000000000017"

  # ===========================================================================
  # Policies in and out
  # ===========================================================================

  @process_manager @pm_policy_in
  @C-0600
  Scenario: Another domain's event reaches a process manager only through a policy
    Given a process manager "Fulfillment" owning domain "fulfillment" whose starting_types lists StartFulfillment
    And no policy translates from "order" to "fulfillment"
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000018" is recorded for order "order-1"
    Then "Fulfillment" is not called

  @process_manager @pm_policy_in
  @C-0601
  Scenario: A policy's command whose root is its trigger's correlation reaches that instance
    Given a process manager "Fulfillment" owning domain "fulfillment" whose starting_types lists StartFulfillment
    And a policy "OrderToFulfillment" translating from "order" to "fulfillment" that builds StartFulfillment from OrderPlaced, setting its root to the trigger's correlation
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000019" is recorded for order "order-1"
    Then "Fulfillment" handles StartFulfillment on stream "fulfillment" root "0192a6b0-0000-7000-8000-000000000019"
    And the events record the command cause with the reaction's derived command_id

  @process_manager @pm_dedup
  @C-0602
  Scenario: A policy re-run sends the same command id, which the process manager answers DUPLICATE
    Given a process manager "Fulfillment" owning domain "fulfillment" whose starting_types lists StartFulfillment
    And a policy "OrderToFulfillment" translating from "order" to "fulfillment" that builds StartFulfillment from OrderPlaced
    And "Fulfillment" has applied the policy's command for the OrderPlaced event at sequence 0 of order "order-1"
    When the policy is run again over that OrderPlaced event
    Then the policy's command carries the same command_id as before
    And "Fulfillment" is not called

  @process_manager @pm_output
  @C-0603
  Scenario: A process manager's decision reaches another domain through a policy
    Given a process manager "Fulfillment" owning domain "fulfillment" that records ShipmentRequested on StartFulfillment
    And a policy "FulfillmentToShipping" translating from "fulfillment" to "shipping" that builds a CreateShipment command from ShipmentRequested
    When a StartFulfillment command with correlation "0192a6b0-0000-7000-8000-00000000001a" is sent to "fulfillment" root "0192a6b0-0000-7000-8000-00000000001a"
    Then "Fulfillment" sends no command
    And "FulfillmentToShipping" delivers CreateShipment to "shipping" with correlation "0192a6b0-0000-7000-8000-00000000001a"
