Feature: Editions are explicit per-stream forks
  An edition is a set of per-stream forks and nothing else (decision
  14.32). Every fork is made explicitly, by EditionAdminService.ForkStream,
  at the fork_sequence the caller states: any at or below main's head for
  that stream (main's next sequence), never above it (decision 14.36).
  ForkStream appends one EditionCreated meta-event to (D, X, R) at that
  sequence: its own EventPage.sequence is the fork point, and its page's
  cover names the edition; EditionCreated itself holds nothing (decisions
  14.86 and 14.107). (D, X, R) reads main's events below the fork point,
  then X's own. There is no automatic propagation: a command or fact in X,
  from a client or a policy, to a stream with no EditionCreated in X is
  refused FAILED_PRECONDITION ANGZARR_REASON_EDITION_NOT_FORKED before
  business code runs (decision 14.107). A meta-event is its own arm of an
  EventPage's body, never a domain event (decision 14.106). There is no
  edition registry: an edition is listed while any fork event of it
  exists. DeleteEdition enumerates the edition's forked streams by their
  EditionCreated events and removes each stream's rows directly: nothing
  is appended and nothing propagates (decision 14.107). Main knows nothing
  of an edition (docs/design/edition-meta-events.md).

  Background:
    Given a "player" aggregate "p-1" with 5 events on the main timeline
    And a policy "PlayerToTable" translating from "player" to "table" that sends SeatPlayer to "table" "t-1" on PlayerReady
    And a "table" aggregate "t-1" with 3 events on the main timeline

  # ---------------------------------------------------------------
  # Forking is explicit (decision 14.107)
  # ---------------------------------------------------------------

  @editions
  @C-0628
  Scenario: Forking a stream at main's next sequence appends the fork event there
    When ForkStream forks "player" "p-1" into edition "what-if" at fork_sequence 5
    Then "player" "p-1" in edition "what-if" holds an EditionCreated event at sequence 5, in its page's meta_event arm
    And that page's cover names edition "what-if"
    And ForkStream answers the fork event's EventRef: "player" "p-1" in edition "what-if", sequence 5
    And the main timeline of "player" "p-1" is unchanged

  @editions
  @C-0629
  Scenario: A forked stream reads main below its fork sequence, then its own events
    Given "player" "p-1" is forked into edition "what-if" at sequence 5
    And "player" "p-1" on the main timeline gains 2 more events
    And "player" "p-1" in edition "what-if" gains 1 event of its own
    When "player" "p-1" is loaded in edition "what-if"
    Then the loaded history is main's events 0 to 4, then the fork event at 5, then the edition's own event at 6
    And none of main's events at or after sequence 5 are in it

  @editions
  @C-0631
  Scenario: A stream is forked once: forking it again is refused
    Given "player" "p-1" is forked into edition "what-if" at sequence 5
    When ForkStream forks "player" "p-1" into edition "what-if" at fork_sequence 5
    Then the request is refused ALREADY_EXISTS
    And "player" "p-1" in edition "what-if" still holds exactly one EditionCreated event

  @editions
  @C-0812
  Scenario Outline: ForkStream refuses a request that names no edition or no stream
    When ForkStream is requested for <stream> at fork_sequence 0
    Then the request is refused INVALID_ARGUMENT
    And no EditionCreated event is written anywhere

    Examples:
      | stream                                         |
      | "player" "p-1" with no edition (main)          |
      | "player" "p-1" with an empty edition ""        |
      | a cover with no domain, in edition "what-if"   |
      | a cover with no root, in edition "what-if"     |

  # C-0632 (a propagation hop bound) is retired: there is no propagation
  # (decision 14.107).

  # ---------------------------------------------------------------
  # The not-forked rule (decision 14.107)
  # ---------------------------------------------------------------

  @editions
  @C-0633
  Scenario Outline: A client's input to a stream not forked in the edition is refused
    Given "player" "p-1" is forked into edition "what-if"
    And "table" "t-9" with 4 events on the main timeline has no fork in edition "what-if"
    When a client sends <input> to "table" "t-9" in edition "what-if"
    Then the client is answered FAILED_PRECONDITION with reason ANGZARR_REASON_EDITION_NOT_FORKED, stream "table" "t-9" and edition "what-if"
    And the router is never called
    And "table" "t-9" holds no event in edition "what-if"

    Examples:
      | input       |
      | a command   |
      | a fact      |

  @editions
  @C-0813
  Scenario: Once the client forks the stream, its resent input is handled
    Given "table" "t-9" with 4 events on the main timeline has no fork in edition "what-if"
    And a client's SeatPlayer command to "table" "t-9" in edition "what-if" was refused ANGZARR_REASON_EDITION_NOT_FORKED
    When ForkStream forks "table" "t-9" into edition "what-if" at fork_sequence 4
    And the client resends the SeatPlayer command with the same command_id
    Then the SeatPlayer command is handled in edition "what-if", its events at sequence 5 and after
    And the main timeline of "table" "t-9" is unchanged

  @editions
  @C-0630
  Scenario: A policy's command to a stream not forked in the edition is dead-lettered, never translated
    # A failed delivery like any other: the framework's key, not a typed
    # refusal, so it never reaches Translate (decision 14.107).
    Given "player" "p-1" is forked into edition "what-if" at sequence 5
    And "table" "t-1" has no fork in edition "what-if"
    When "player" "p-1" records PlayerReady in edition "what-if"
    Then the SeatPlayer command is refused at "table" "t-1" before business code, with reason ANGZARR_REASON_EDITION_NOT_FORKED, stream "table" "t-1" and edition "what-if"
    And it is dead-lettered as a rejected reaction with that reason, and an alert is raised
    And the policy's Translate is never called
    And "table" "t-1" holds no event in edition "what-if"

  @editions
  @C-0781
  Scenario: A policy's fact to a stream not forked in the edition is dead-lettered, never recorded
    # A policy emits the facts its destination declares (decision 14.69);
    # the not-forked rule refuses a fact as it does a command (decision
    # 14.107).
    Given "player" "p-1" is forked into edition "what-if" at sequence 5
    And "table" "t-1" has no fork in edition "what-if"
    And "PlayerToTable" sends PlayerArrived, a fact "table" declares, to "table" "t-1" on PlayerReady in place of SeatPlayer
    When "player" "p-1" records PlayerReady in edition "what-if"
    Then the PlayerArrived fact is dead-lettered as a rejected reaction with reason ANGZARR_REASON_EDITION_NOT_FORKED, stream "table" "t-1" and edition "what-if", and an alert is raised
    And "table" "t-1" holds no event in edition "what-if"

  @editions
  @C-0639
  Scenario: A process manager's instance stream is forked like any domain's
    Given a process manager "BuyIn" owning domain "buy-in" with an instance at root "0192a6b0-0000-7000-8000-000000000030"
    And a policy "PlayerToBuyIn" that sends a command to "buy-in" "0192a6b0-0000-7000-8000-000000000030" on PlayerReady
    And "player" "p-1" is forked into edition "what-if"
    And ForkStream has forked "buy-in" "0192a6b0-0000-7000-8000-000000000030" into edition "what-if"
    When "player" "p-1" records PlayerReady in edition "what-if"
    Then the command is handled by the process manager in edition "what-if"
    And "buy-in" "0192a6b0-0000-7000-8000-000000000030" in edition "what-if" holds exactly one EditionCreated event, the one ForkStream wrote

  @editions
  @C-0654
  Scenario Outline: An input in an edition is refused not-forked before the router is called
    Given "player" "p-1" is forked into edition "what-if"
    When <input>
    Then it is refused FAILED_PRECONDITION with reason ANGZARR_REASON_EDITION_NOT_FORKED, naming the stream and the edition
    And nothing is written
    And the router is never called

    Examples:
      | input                                                                                     |
      | a client sends a command to "table" "t-9" in edition "what-if", which has no fork there   |
      | a client sends a fact to "table" "t-9" in edition "what-if", which has no fork there      |
      | a policy's command arrives at "table" "t-1" in edition "gone", which has been deleted     |

  @editions
  @C-0635
  Scenario Outline: An input naming an edition that does not exist is refused not-forked
    # The same rule as an unforked stream: no stream of the edition holds
    # an EditionCreated (decision 14.107). Queries stay NOT_FOUND (C-0817).
    Given <history>
    When a command is sent to "player" "p-1" in edition "what-if"
    Then the command is refused FAILED_PRECONDITION with reason ANGZARR_REASON_EDITION_NOT_FORKED, stream "player" "p-1" and edition "what-if"
    And nothing is written in edition "what-if"

    Examples:
      | history                                                                      |
      | no fork event of edition "what-if" exists anywhere                           |
      | edition "what-if" was forked and then deleted with DeleteEdition             |

  @editions
  @C-0634
  Scenario: A query of a stream not yet forked reads main and forks nothing
    Given "player" "p-1" is forked into edition "what-if"
    And "table" "t-9" with 4 events on the main timeline is not forked in "what-if"
    When "table" "t-9" is queried in edition "what-if"
    Then the result is main's 4 events of "table" "t-9"
    And "table" "t-9" holds no event in edition "what-if"

  @editions
  @C-0817
  Scenario Outline: A query naming an edition with no fork is refused NOT_FOUND
    # Subscriptions likewise: subscriptions.feature, C-0831.
    Given no fork event of edition "what-if" exists anywhere
    When a client <reads> in edition "what-if"
    Then the request is refused NOT_FOUND
    And no EditionCreated event is written anywhere

    Examples:
      | reads                                                   |
      | queries the events of "player" "p-1"                    |
      | queries the correlated events of a correlation          |

  # ---------------------------------------------------------------
  # Meta-events are their own arm (decision 14.106)
  # ---------------------------------------------------------------

  @editions @meta_events
  @C-0814
  Scenario: The fork event is the page's meta_event arm, never a domain event
    When ForkStream forks "player" "p-1" into edition "what-if" at fork_sequence 5
    And "player" "p-1" is queried in edition "what-if"
    Then the page at sequence 5 has its body in the meta_event arm, holding an EditionCreated in its created arm
    And that page has no event arm set
    And it carries an EventMeta like any persisted page

  @editions @meta_events
  @C-0655
  Scenario: Policies and handlers never see a meta-event
    Given a policy "PlayerToTable" with a handler for every "player" event
    And a projector with a handler for every "player" event
    When ForkStream forks "player" "p-1" into edition "what-if"
    And "player" "p-1" is loaded in edition "what-if" for a command
    Then the system layer delivers the EditionCreated to no policy, process manager, aggregate or projector handler
    And business code is handed only pages of the event arm
    And the router is never given it

  @editions @meta_events
  @C-0815
  Scenario: A command whose payload is an EditionCreated is an ordinary command of an unknown type
    # A CommandPage has no meta arm, so no input carries a meta-event
    # (decision 14.106; docs/design/edition-meta-events.md §3).
    When a client sends a command whose payload is an io.angzarr.v1.EditionCreated to "player" "p-1"
    Then the client is refused INTERNAL with reason ANGZARR_REASON_UNKNOWN_COMMAND_TYPE, type_url "/io.angzarr.v1.EditionCreated"
    And no EditionCreated event is written anywhere
    And the main timeline of "player" "p-1" is unchanged

  # ---------------------------------------------------------------
  # Listing, with no registry
  # ---------------------------------------------------------------

  @editions
  @C-0636
  Scenario: Editions are listed from the fork events, with no registry
    Given "player" "p-1" is forked into edition "what-if" at sequence 5
    And "table" "t-1" is forked into edition "what-if" at sequence 3
    When ListEditions is called
    Then the answer lists edition "what-if" with forks the EventRefs of "player" "p-1" at sequence 5 and "table" "t-1" at sequence 3, both in edition "what-if"

  # ---------------------------------------------------------------
  # Deletion by enumeration (decision 14.107)
  # ---------------------------------------------------------------

  # C-0637 (a delete recorded after the in-flight work before it) is
  # retired: a deletion records no event and waits on nothing; work in
  # flight that reaches a removed stream is refused (C-0647; decision
  # 14.107).

  @editions
  @C-0644
  Scenario: DeleteEdition removes every forked stream's rows directly, appending nothing
    Given "player" "p-1" is forked into edition "what-if"
    And "table" "t-1" is forked into edition "what-if"
    When DeleteEdition deletes "what-if"
    Then it finds "player" "p-1" and "table" "t-1" by their EditionCreated events in edition "what-if"
    And it removes each stream's rows of edition "what-if": its events, the fork event included, snapshots, projector output, outbox entries and positions
    And no event is appended to any stream for the deletion
    And no policy is called for the deletion
    And DeleteEdition answers once both streams are removed
    And ListEditions no longer lists edition "what-if"

  @editions
  @C-0646
  Scenario: Every forked stream is removed, whether or not a policy flow leads to it
    Given "player" "p-1" is forked into edition "what-if"
    And "table" "t-9" was forked into edition "what-if" with ForkStream, and no policy flow leads to it from "player"
    When DeleteEdition deletes "what-if"
    Then the rows of "player" "p-1" in edition "what-if" are removed
    And the rows of "table" "t-9" in edition "what-if" are removed
    And ListEditions no longer lists edition "what-if"

  # C-0645 (a stream's rows never removed before its EditionDeleted has
  # propagated onward) is retired: EditionDeleted is deleted and nothing
  # propagates (decision 14.107).

  @editions
  @C-0816
  Scenario: Deleting an edition that does not exist is refused NOT_FOUND
    Given no fork event of edition "what-if" exists anywhere
    When DeleteEdition deletes "what-if"
    Then the request is refused NOT_FOUND
    And nothing is removed

  @editions
  @C-0641
  Scenario: A removal that fails is dead-lettered and alerted, and the delete still answers
    Given "player" "p-1" and "table" "t-1" are forked into edition "what-if"
    And the edition store keeps refusing to remove the rows of "player" "p-1" in edition "what-if"
    When DeleteEdition deletes "what-if"
    Then the removal of "player" "p-1" is retried under the retry cap
    And it is then dead-lettered with failed_edition_removal the cover of "player" "p-1" in edition "what-if"
    And the dead letter's reason is ANGZARR_REASON_EDITION_REMOVAL_FAILED with stream "player" "p-1" in edition "what-if", edition "what-if", attempts and cause
    And its retry_count is the retries after the first attempt
    And an alert is raised
    And the rows of "table" "t-1" in edition "what-if" are removed
    And DeleteEdition answers once every stream is removed or so dead-lettered

  @editions
  @C-0640
  Scenario: After a delete, a later arrival in the edition is refused and re-creates nothing
    Given edition "what-if" was deleted and no row of it remains
    When a policy command in edition "what-if" arrives at "table" "t-1"
    Then the command is refused before business code with reason ANGZARR_REASON_EDITION_NOT_FORKED, stream "table" "t-1" and edition "what-if"
    And it is dead-lettered as a rejected reaction with that reason, and an alert is raised
    And "table" "t-1" holds no event in edition "what-if"
    And ListEditions does not list edition "what-if"
    And only ForkStream can bring the name "what-if" back

  @editions
  @C-0647
  Scenario: Work in flight that reaches a removed stream is refused not-forked
    Given "player" "p-1" and "table" "t-1" are forked into edition "what-if"
    And "player" "p-1" recorded PlayerReady in edition "what-if", and its SeatPlayer is still in the policy's outbox
    When DeleteEdition deletes "what-if", removing "table" "t-1" in edition "what-if"
    And the SeatPlayer command then arrives at "table" "t-1" in edition "what-if"
    Then it is refused before business code with reason ANGZARR_REASON_EDITION_NOT_FORKED, stream "table" "t-1" and edition "what-if"
    And it is dead-lettered as a rejected reaction with that reason, and an alert is raised
    And "table" "t-1" holds no event in edition "what-if"

  # C-0652 (the system layer creating a stream in the edition when a
  # policy's command arrives) is retired: there is no automatic fork; an
  # arrival at an unforked stream is refused (C-0630; decision 14.107).

  # C-0653 (the system layer propagating a deletion along the streams the
  # policies reached) is retired: deletion is by enumeration and
  # propagates nothing (C-0644; decision 14.107).

  # ---------------------------------------------------------------
  # Main knows nothing (decision 14.32)
  # ---------------------------------------------------------------

  @editions
  @C-0642
  Scenario Outline: Creating and deleting an edition leaves main byte-for-byte unchanged
    Given a snapshot of main's <part> for "player" "p-1" and "table" "t-1"
    When ForkStream forks "player" "p-1" and "table" "t-1" into edition "what-if"
    And "player" "p-1" records PlayerReady in edition "what-if", and "table" "t-1" handles its SeatPlayer there
    And DeleteEdition deletes "what-if" and the deletion completes
    Then main's <part> are byte-for-byte unchanged

    Examples:
      | part                            |
      | streams                         |
      | policy outbox entries           |
      | policy and projector positions  |
      | projections                     |

  @editions
  @C-0643
  Scenario: Edition lifecycle is never written to, or read from, main
    When ForkStream forks "player" "p-1" into edition "what-if"
    And ListEditions is called
    And DeleteEdition deletes "what-if"
    Then no EditionCreated event exists on any main stream
    And no main stream gained any event
    And ListEditions and the existence check read only the edition store
    And main's coordinators, projectors and readers received nothing about edition "what-if"

  # C-0638 (a fork at one store-wide position) is retired: there is no
  # global position, and every fork is per stream (decision 14.35).

  # ---------------------------------------------------------------
  # The fork point is explicit, per stream (decision 14.36)
  # ---------------------------------------------------------------

  @editions @fork_point
  @C-0661
  Scenario: A fork stated below main's head lands at its stated sequence and reads main below it
    When ForkStream forks "player" "p-1" into edition "what-if" at fork_sequence 3
    Then "player" "p-1" in edition "what-if" holds an EditionCreated event at sequence 3
    And "player" "p-1" loaded in edition "what-if" is main's events 0 to 2, then the fork event at 3
    And none of main's events at or after sequence 3 are in it
    And the main timeline of "player" "p-1" is unchanged

  @editions @fork_point
  @C-0662
  Scenario: A fork stated above main's head is a conflict, and writes nothing
    When ForkStream forks "player" "p-1" into edition "what-if" at fork_sequence 6
    Then the request is refused ABORTED
    And "player" "p-1" holds no event in edition "what-if"
    And no EditionCreated event of edition "what-if" exists anywhere

  # C-0663 (an arrival's fork at the main head the system layer read) is
  # retired: nothing forks on arrival; every fork point is stated to
  # ForkStream (decision 14.107).
