Feature: Dead letter administration - plain results, failures as keyed statuses
  DlqAdminService (the distributed transport's; docs/design/dlq-admin.md)
  lists, fetches, replays and deletes the dead letters a distributed
  deployment stores. Every method returns its plain result (decision
  14.106): ListDeadLetters a page of StoredDeadLetter entries and a
  next_page_token, GetDeadLetter the StoredDeadLetter itself (AIP-131),
  ReplayDeadLetter the original correlation_id and the replay as recorded,
  and DeleteDeadLetter which of removed or no_entry happened. A failure is
  a gRPC status with a google.rpc.ErrorInfo key:

  - UNAVAILABLE DISTRIBUTED_REASON_DEAD_LETTER_STORE_UNAVAILABLE (arg
    cause): the admin store unreachable or failing, on any method;
  - NOT_FOUND DISTRIBUTED_REASON_DEAD_LETTER_NOT_FOUND (arg id): Get or
    Replay of an id with no entry;
  - INVALID_ARGUMENT DISTRIBUTED_REASON_DEAD_LETTER_QUERY_INVALID (arg
    field, "filter" or "page_token"): a List filter or page token that
    does not parse.

  None of these keys is ever a dead letter's reason
  (docs/design/dead-letters.md §5).

  Background:
    Given a distributed deployment whose dead-letter admin store holds:
      | id | domain | why                                                                |
      | 7  | order  | reason ANGZARR_REASON_RETRY_CAP_EXHAUSTED                           |
      | 8  | wallet | refusal InsufficientFunds with available 200 and requested 500     |

  @C-0802
  Scenario: GetDeadLetter answers the stored entry itself, its why mirrored
    When the operator gets dead letter 8
    Then the answer is the StoredDeadLetter with id 8 and domain "wallet"
    And its why is the refusal arm, the InsufficientFunds refusal object packed
    And its payload is the DeadLetterEnvelope exactly as the publisher wrote it

  @C-0803
  Scenario Outline: GetDeadLetter and ReplayDeadLetter of an id with no entry are NOT_FOUND
    When the operator <action> dead letter 99
    Then the call fails NOT_FOUND with reason DISTRIBUTED_REASON_DEAD_LETTER_NOT_FOUND and arg id "99"
    And nothing is replayed

    Examples:
      | action  |
      | gets    |
      | replays |

  @C-0804
  Scenario: DeleteDeadLetter is idempotent, the outcome's arm saying which
    When the operator deletes dead letter 7
    Then the answer's outcome is removed
    When the operator deletes dead letter 7 again
    Then the answer's outcome is no_entry
    And neither call fails, and neither is ever answered DISTRIBUTED_REASON_DEAD_LETTER_NOT_FOUND

  @C-0805
  Scenario Outline: A filter or page token that does not parse is INVALID_ARGUMENT, naming the field
    When the operator lists dead letters with <request>
    Then the call fails INVALID_ARGUMENT with reason DISTRIBUTED_REASON_DEAD_LETTER_QUERY_INVALID and arg field "<field>"

    Examples:
      | request                                      | field      |
      | the filter domain = "order" OR              | filter     |
      | the filter correlation_id = "not-a-uuid"   | filter     |
      | the page token "garbage"                   | page_token |

  @C-0806
  Scenario Outline: An admin store that cannot answer fails every method UNAVAILABLE with its own key
    # Decision 14.106: the event store's ANGZARR_REASON_STORE_UNAVAILABLE is
    # not reused; the admin store has its own key.
    Given the dead-letter admin store cannot be reached
    When the operator <action>
    Then the call fails UNAVAILABLE with reason DISTRIBUTED_REASON_DEAD_LETTER_STORE_UNAVAILABLE and arg cause
    And nothing is changed

    Examples:
      | action                     |
      | lists dead letters         |
      | gets dead letter 7         |
      | replays dead letter 7      |
      | deletes dead letter 7      |

  @C-0807
  Scenario Outline: A refusal entry is listed by its implied key
    # Decision 14.101: a refusal entry's reason_code is its implied key.
    When the operator lists dead letters with the filter <filter>
    Then the page holds exactly the entries <ids>

    Examples:
      | filter                                                   | ids |
      | reason_code = "ANGZARR_REASON_COMMAND_REFUSED"          | 8   |
      | reason_code = "ANGZARR_REASON_RETRY_CAP_EXHAUSTED"      | 7   |
