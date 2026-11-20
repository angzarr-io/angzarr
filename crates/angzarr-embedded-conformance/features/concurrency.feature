Feature: Concurrency - the per-stream append, race re-runs and the expected sequence
  The coordinator has no conflict-resolution concept (decision 14.92; the
  owner's decision 47): no resolution, no conflict window, no conflict
  units, no conflict report, no nonce. What stays is not conflict handling
  (docs/design/pipeline.md §4 and §5):

  - The store's per-stream optimistic append: the coordinator loads a
    stream (its snapshot, its pages, its head) and appends the decision's
    events AT THE HEAD IT LOADED. Ordering is per stream: no other
    stream's writes matter.
  - A race lost at the write is re-run by the coordinator on the new head,
    so the handler always decides on current state, under the race re-run
    cap (a count of runs, the first included, default 10, at least 1;
    decision 14.84), with backoff and jitter between runs. At the cap
    nothing is persisted. A command whose caller is waiting (Handle,
    HandleSync) is answered INTERNAL ANGZARR_REASON_RETRY_CAP_EXHAUSTED and
    is NOT dead-lettered: the caller holds it (decision 14.85). Work with
    no caller waiting (HandleAsync, a reaction) is dead-lettered with that
    key and alerted. A fact is dead-lettered at the cap EVEN WHEN ITS
    SENDER IS WAITING, and its sender is answered INTERNAL
    ANGZARR_REASON_FACT_DEAD_LETTERED (decisions 14.100 and 14.109).
  - The client's expected sequence (Handling.expected_sequence, "I decided
    having seen version N") is the one thing the coordinator checks of
    staleness (decision 14.94): set, and not the head it loaded, the
    command is refused before the handler is called (pipeline step 4,
    after deduplication), behind or ahead alike, with the framework's one
    refusal, io.angzarr.v1.ExpectedSequenceMismatch { expected, head }:
    FAILED_PRECONDITION, reason ANGZARR_REASON_COMMAND_REFUSED, refusal_type
    io.angzarr.v1.ExpectedSequenceMismatch, the object in the status
    details. No command lists it in its refuses. A command with no
    expected sequence is decided on the current state; the coordinator
    never stamps one. A command with one that loses a race at its write is
    re-run, finds the head moved and is refused.

  Reactions (a policy's outputs, delivered as a ReactionDelivery) carry no
  Handling, so they never carry an expected sequence (decision 14.79).

  Retired with decision 14.92 (no conflict concept; the file was
  conflict_resolution.feature): C-0531 (a resolution the stream does not
  support), C-0164, C-0532, C-0165, C-0166, C-0551 and C-0683
  (AGGREGATE_HANDLES: the window, the snapshot inside it, the nonce),
  C-0684 and C-0149 (COMMUTATIVE: conflict units, the head shortcut),
  C-0721 and C-0172 (a moved stream decided by the resolution's rule),
  C-0722 and C-0723 (the widened window re-delivered at a race). Retired
  with decision 14.84 (the race re-run cap is at least 1): C-0740, C-0741
  and C-0742 (a cap of 0 as an opt-out, answered as the store's conflict).
  Retired with decision 14.79: C-0552 (a reaction naming an expected
  sequence; a ReactionDelivery has no Handling to name one). Earlier,
  with decision 14.47: C-0162.

  Background:
    Given an "order" aggregate
    And the aggregate has events:
      | sequence | type           |
      | 0        | OrderCreated   |
      | 1        | ItemAdded      |
      | 2        | OrderNoteAdded |
    # next_sequence is 3

  # ===========================================================================
  # No expected sequence: decided on the current state
  # ===========================================================================

  @current_state
  @C-0155
  Scenario: A command with no expected sequence is decided on the current state
    # Decision 14.94: there is no mode and no resolution; the coordinator
    # never stamps an expected sequence onto a command.
    Given an AddItem command with no expected sequence
    When the coordinator processes the command
    Then business code is invoked with the stream's history up to its head at sequence 3
    And business code receives the command with no expected sequence
    And an ItemAdded event is persisted at sequence 3

  # ===========================================================================
  # The expected sequence: the coordinator's staleness check (decision 14.94)
  # ===========================================================================

  @expected_sequence
  @C-0160
  Scenario: A command whose expected sequence is the head is decided
    Given an AddItem command with expected sequence 3
    When the coordinator processes the command
    Then business code is invoked
    And business code receives the command with expected sequence 3, equal to the history's next_sequence
    And an ItemAdded event is persisted at sequence 3

  @expected_sequence
  @C-0161
  Scenario: A stale command is refused before business code
    Given a ChangeShippingAddress command with expected sequence 1
    When the coordinator processes the command
    Then the command fails FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED and refusal_type io.angzarr.v1.ExpectedSequenceMismatch
    And business code is not invoked
    And no events are persisted

  @expected_sequence
  @C-0717
  Scenario Outline: A command behind the head is refused with its expected sequence and the head
    Given an AddItem command with expected sequence <expected>
    When the coordinator processes the command
    Then the command fails FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED and refusal_type io.angzarr.v1.ExpectedSequenceMismatch
    And the status details hold an ExpectedSequenceMismatch with expected <expected> and head 3
    And business code is not invoked
    And nothing is persisted

    Examples:
      | expected |
      | 1        |
      | 2        |

  @expected_sequence
  @C-0727
  Scenario Outline: A command ahead of the head is refused with the same refusal
    # Decisions 14.93 and 14.94: behind and ahead are one refusal; the
    # client decided against a version that is not the head either way.
    Given an AddItem command with expected sequence <expected>
    When the coordinator processes the command
    Then the command fails FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED and refusal_type io.angzarr.v1.ExpectedSequenceMismatch
    And the status details hold an ExpectedSequenceMismatch with expected <expected> and head 3
    And business code is not invoked
    And nothing is persisted

    Examples:
      | expected |
      | 4        |
      | 7        |

  @expected_sequence
  @C-0800
  Scenario Outline: The coordinator refuses a stale command whatever the command declares it refuses
    # Decision 14.94: ExpectedSequenceMismatch is the coordinator's, listed
    # in no command's refuses; a command whose refuses names nothing (which
    # its handler can never refuse) is refused by the check all the same.
    Given AddItem's (command) declaration names no refuses type
    And an AddItem command with expected sequence <expected>
    When the coordinator processes the command
    Then the command fails FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED and refusal_type io.angzarr.v1.ExpectedSequenceMismatch
    And the status details hold an ExpectedSequenceMismatch with expected <expected> and head 3
    And the refusal is the coordinator's: business code is not invoked
    And the command is never answered under any other key

    Examples:
      | expected |
      | 1        |
      | 7        |

  @expected_sequence
  @C-0728
  Scenario: A retry of an applied command answers DUPLICATE whatever its expected sequence
    # Decision 14.94: deduplication (pipeline step 3) runs before the check
    # (step 4), so the retry is answered as its first delivery was.
    Given an AddItem command with command_id "0192f3a4-5b6c-7d8e-9f01-23456789ac28" was applied, its ItemAdded event persisted at sequence 3
    And a retry of that AddItem command, with the same command_id and expected sequence 9
    When the coordinator processes the command
    Then the caller is answered with delivery DUPLICATE and the ItemAdded event at sequence 3
    And business code is not invoked
    And no events are persisted
    And the command is never refused ExpectedSequenceMismatch

  @expected_sequence
  @C-0729
  Scenario Outline: An asynchronous command whose expected sequence is not the head is dead-lettered with the refusal and alerted
    # The check needs the head, so the refusal comes when the command is
    # processed, with no caller to answer (decisions 14.47 and 14.101).
    When an AddItem command with expected sequence <expected> is sent asynchronously
    Then HandleAsync answers OK
    And business code is not invoked
    And no events are persisted
    And the command is dead-lettered, its why the refusal arm holding an ExpectedSequenceMismatch with expected <expected> and head 3, and an alert is raised
    And the command is not retried

    Examples:
      | expected |
      | 1        |
      | 7        |

  # ===========================================================================
  # A same-stream race at the write
  # ===========================================================================

  @write_race
  @C-0718
  Scenario: A command with an expected sequence that loses a race at its write is refused on its re-run
    # Decision 14.94: the re-run finds the head moved, and step 4 refuses it.
    Given an AddItem command with expected sequence 3
    And another write to the "order" stream lands at sequence 3 after the command is loaded and before it is written
    When the coordinator processes the command
    Then business code is invoked once
    And the coordinator re-runs the command on the new head
    And the command fails FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED and refusal_type io.angzarr.v1.ExpectedSequenceMismatch
    And the status details hold an ExpectedSequenceMismatch with expected 3 and head 4
    And the command persists nothing

  @write_race
  @C-0719
  Scenario: A command without an expected sequence that loses a race at its write is re-run on the new head
    Given an AddItem command with no expected sequence
    And another write to the "order" stream lands at sequence 3 after the command is loaded and before it is written
    When the coordinator processes the command
    Then business code is invoked twice, the second time against the head at sequence 4
    And the command succeeds
    And an ItemAdded event is persisted at sequence 4

  @write_race
  @C-0725
  Scenario Outline: A command whose caller is waiting stops at the race re-run cap, answered and not dead-lettered
    # Decision 14.85 (the owner's ruling of 2026-10-31): "sync commands
    # write no dead letter at the retry cap"; the caller holds the command.
    Given the race re-run cap is 3
    And an AddItem command with no expected sequence, sent through <rpc> by a caller waiting for the answer
    And the business process answers every run with an ItemAdded event
    And another write to the "order" stream lands after each run's load and before the command is written
    When the coordinator processes the command
    Then business code is invoked 3 times, each time against a newer head
    And the command persists nothing
    And the caller is answered INTERNAL with reason ANGZARR_REASON_RETRY_CAP_EXHAUSTED, attempts "3" and cap "3"
    And nothing is dead-lettered

    Examples:
      | rpc        |
      | Handle     |
      | HandleSync |

  @write_race
  @C-0724
  Scenario: A reaction that loses a race at every write until the race re-run cap is dead-lettered
    # Decision 14.85: work with no caller waiting is dead-lettered and
    # alerted; the policy step that sent it then follows on_policy_bug
    # (docs/design/command-handling.md §4).
    Given the race re-run cap is 3
    And a policy "PaymentToOrder" translating from "payment" to "order" whose reaction to a PaymentReceived event is an AddItem command for this "order" stream
    And the business process answers every run with an ItemAdded event
    And another write to the "order" stream lands after each run's load and before the reaction is written
    When the policy's coordinator delivers the reaction through Deliver
    Then business code is invoked 3 times, each time against a newer head
    And the reaction persists nothing
    And the reaction is dead-lettered as rejected_reaction with reason ANGZARR_REASON_RETRY_CAP_EXHAUSTED, attempts "3" and cap "3", and an alert is raised
    And the policy step follows its on_policy_bug setting

  @write_race @fact
  @C-0726
  Scenario Outline: A fact that loses a race at every write until the race re-run cap is dead-lettered, its waiting sender answered non-retryable
    # Decisions 14.100 and 14.109: facts differ from commands at the cap; a
    # fact is never refused, so nothing but the dead letter holds it.
    Given the race re-run cap is 3
    And the order aggregate declares fact ShipmentDispatched
    And a ShipmentDispatched fact for the "order" stream, sent through <rpc> by a client waiting for the answer
    And the handler answers every run with a ShipmentDispatched event
    And another write to the "order" stream lands after each run's load and before the fact is written
    When the coordinator processes the fact
    Then the handler is invoked 3 times, each time against a newer head
    And the fact persists nothing
    And the fact is dead-lettered with reason ANGZARR_REASON_RETRY_CAP_EXHAUSTED, attempts "3" and cap "3", and an alert is raised
    And the client is answered INTERNAL with reason ANGZARR_REASON_FACT_DEAD_LETTERED, its cause ANGZARR_REASON_RETRY_CAP_EXHAUSTED with attempts "3" and cap "3"
    And the client is never answered ANGZARR_REASON_RETRY_CAP_EXHAUSTED itself

    Examples:
      | rpc        |
      | Handle     |
      | HandleSync |

  @write_race @fact
  @C-0801
  Scenario Outline: At the race re-run cap a waiting command is answered and kept, a waiting fact is dead-lettered
    # Decision 14.109: command-handling.md §4 states the difference.
    Given the race re-run cap is 3
    And the order aggregate declares <kind> <type>
    And a <type> <kind> for the "order" stream, sent through Handle by a caller waiting for the answer
    And another write to the "order" stream lands after each run's load and before the input is written
    When the coordinator processes the input
    Then the input persists nothing
    And the caller is answered INTERNAL with reason <answered>
    And <dead_letter>

    Examples:
      | kind    | type               | answered                          | dead_letter                                                                    |
      | command | AddItem            | ANGZARR_REASON_RETRY_CAP_EXHAUSTED | nothing is dead-lettered                                                       |
      | fact    | ShipmentDispatched | ANGZARR_REASON_FACT_DEAD_LETTERED  | the fact is dead-lettered with reason ANGZARR_REASON_RETRY_CAP_EXHAUSTED and alerted |

  @write_race
  @C-0720
  Scenario: A write to another stream does not make a command stale
    Given an AddItem command with expected sequence 3
    And another write lands on a different "order" root's stream after the command is loaded and before it is written
    When the coordinator processes the command
    Then the command succeeds
    And an ItemAdded event is persisted at sequence 3

  # ===========================================================================
  # Reactions: no Handling, decided on the current state
  # ===========================================================================

  @reaction
  @C-0458
  Scenario: A reaction is decided against the head
    Given a policy's AddItem reaction, delivered as a ReactionDelivery with no Handling
    When the coordinator processes the reaction
    Then the reaction succeeds
    And an ItemAdded event is persisted at sequence 3

  @reaction
  @C-0459
  Scenario: A reaction is guarded only by the destination's own decision
    # Decision 14.95: typed refusals only; the refusal reaches the policy's
    # Translate (docs/design/policies.md §3).
    Given AddItem's (command) declaration names the refusal OrderClosed in its refuses, declared FAILED_PRECONDITION
    And a policy's AddItem reaction, delivered as a ReactionDelivery with no Handling
    And business code refuses the reaction with an OrderClosed refusal
    When the coordinator processes the reaction
    Then the reaction fails FAILED_PRECONDITION with reason ANGZARR_REASON_COMMAND_REFUSED and refusal_type naming OrderClosed
    And no events are persisted
    And the reaction is never refused ExpectedSequenceMismatch

  @reaction
  @C-0460
  Scenario: A redelivered reaction is applied once
    Given a policy's AddItem reaction
    When the coordinator processes the reaction
    And the coordinator processes the same reaction again
    Then exactly one ItemAdded event is persisted
    And the second delivery answers delivery DUPLICATE with the events of the first

  # ===========================================================================
  # Edge cases
  # ===========================================================================

  @edge_case
  @C-0174
  Scenario Outline: A new stream accepts its first command at sequence 0
    Given a new "order" aggregate with no events
    And a CreateOrder command <handling>
    When the coordinator processes the command
    Then the command succeeds
    And an OrderCreated event is persisted at sequence 0

    Examples:
      | handling                 |
      | with expected sequence 0 |
      | with no expected sequence |

  @edge_case
  @C-0175
  Scenario: A snapshot moves the head forward
    Given an "order" aggregate with a snapshot at sequence 50 and events at sequences 51 and 52
    And an AddItem command with expected sequence 53
    When the coordinator processes the command
    Then the command succeeds
    And an ItemAdded event is persisted at sequence 53
