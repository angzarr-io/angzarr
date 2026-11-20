Feature: Projector delivery - runs per stream, marked only after an affirmative answer
  A projector is handed a run of one stream's events, in sequence order,
  upcast (ProjectorService.Project; decision 14.78). A projector over
  several domains receives each stream's run independently, in any order
  across streams: there is no cross-stream order (decision 14.96). The
  coordinator records a run's position (its processed state, decision
  14.68) ONLY after the projector's affirmative ProjectResponse: an error
  status, or no answer, leaves the run unmarked, and it is delivered again
  (decision 14.97). A run whose read-model write landed but whose mark did
  not is delivered again, and the projector skips what it has already
  applied, by (stream, sequence). See docs/design/projectors.md.

  A projector publishes CloudEvents by answering a projection whose type is
  exactly io.angzarr.transport.distributed.v1.CloudEventsResponse, read as
  every type URL is, by the full name after the last "/" (decision 14.108;
  docs/design/cloudevents.md). An edition's CloudEvents publish to the
  configured sinks like main's (decision 14.107).

  Background:
    Given a projector "OrderSummary" of domain "order"
    And order "0192a6b0-0000-7000-8000-000000000070" holds events at sequences 0 to 4

  @projector_delivery
  @C-0837
  Scenario Outline: A projector that does not answer affirmatively leaves its run unmarked, and the run is delivered again
    Given "OrderSummary" has processed order "0192a6b0-0000-7000-8000-000000000070" to next_sequence 2
    And "OrderSummary" <failure> for its next run
    When the coordinator delivers the run of sequences 2 to 4 to "OrderSummary"
    Then the record of "OrderSummary" still holds order "0192a6b0-0000-7000-8000-000000000070" at next_sequence 2
    And the run of sequences 2 to 4 is delivered to "OrderSummary" again

    Examples:
      | failure                         |
      | answers UNAVAILABLE             |
      | answers INTERNAL                |
      | does not answer before its deadline |

  @projector_delivery
  @C-0838
  Scenario: A run whose write landed but whose mark did not is delivered again, and the projector skips what it applied
    Given "OrderSummary" has processed order "0192a6b0-0000-7000-8000-000000000070" to next_sequence 2
    And "OrderSummary" writes its read model for the run of sequences 2 to 4 and the coordinator crashes before recording the run
    When the coordinator restarts
    Then the run of sequences 2 to 4 is delivered to "OrderSummary" again, each page carrying its stream and sequence
    And "OrderSummary" applies none of those pages twice
    And after its affirmative answer the record of "OrderSummary" holds order "0192a6b0-0000-7000-8000-000000000070" at next_sequence 5

  @projector_delivery
  @C-0839
  Scenario: An affirmative answer records the run's position as the last page's sequence plus one
    When the coordinator delivers the run of sequences 0 to 4 to "OrderSummary" and it answers a projection
    Then the coordinator builds the Projection with cover order "0192a6b0-0000-7000-8000-000000000070", projector "OrderSummary" and next_sequence 5
    And the record of "OrderSummary" holds order "0192a6b0-0000-7000-8000-000000000070" at next_sequence 5

  @projector_delivery
  @C-0840
  Scenario: A projector over two domains receives each stream's run on its own, with no cross-stream order
    Given a projector "AuditLog" of domains "order" and "payment"
    And order "0192a6b0-0000-7000-8000-000000000070" and payment "0192a6b0-0000-7000-8000-000000000071" each record events
    When the coordinator delivers to "AuditLog"
    Then each Project call holds pages of one stream only, in sequence order
    And no order is promised between the order stream's runs and the payment stream's runs

  @projector_delivery
  @cloudevents
  @C-0841
  Scenario Outline: CloudEvents are published only for a projection of exactly the framework's CloudEventsResponse type
    Given "OrderSummary" answers a projection whose type URL is "<type_url>" holding one CloudEvent
    When the coordinator delivers the run of sequences 0 to 4 to "OrderSummary"
    Then <published>

    Examples:
      | type_url                                                                    | published                                         |
      | type.googleapis.com/io.angzarr.transport.distributed.v1.CloudEventsResponse | the CloudEvent is published to the configured sinks |
      | /io.angzarr.transport.distributed.v1.CloudEventsResponse                    | the CloudEvent is published to the configured sinks |
      | type.googleapis.com/CloudEventsResponse                                     | nothing is published                              |
      | type.googleapis.com/orders.v1.CloudEventsResponse                           | nothing is published                              |

  @projector_delivery
  @cloudevents
  @C-0842
  Scenario: An edition's projected CloudEvents publish to the configured sinks like main's
    Given order "0192a6b0-0000-7000-8000-000000000070" is forked into edition "what-if" and records an event there
    And "OrderSummary" answers a CloudEventsResponse for it, leaving id and source empty
    When the coordinator delivers that edition's run to "OrderSummary"
    Then the CloudEvent is published to the configured sinks
    And its id and source carry the edition "what-if", so its id differs from any main-timeline event's
