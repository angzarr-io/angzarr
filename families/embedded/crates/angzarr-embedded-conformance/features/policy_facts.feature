Feature: Policy facts - a policy emits what its destination declares
  A policy translates its source's events into what its destination takes
  (decision 14.69): a command for a type the destination declares a
  command, which it may refuse (policy_refusal.feature), and a fact for a
  type it declares a fact (its own option, (io.angzarr.v1.fact);
  decision 14.91), which it records and never refuses. One reaction may
  hold both arms (Output's oneof body: command or fact), in the order
  business code returned them; which arm a type takes is the destination's
  declaration, read by generated code, never business code's choice.

  The policy's coordinator checks a fact as it checks a command
  (policy_addressing.feature) and stamps it the same identity, as a
  ReactionDelivery in its fact arm (decision 14.79): its command_id is the
  reaction command id of its trigger, the policy's name and its index in
  the reaction's output, commands and facts counted in one index space;
  its causation is the trigger and its correlation the trigger's. The
  destination derives the fact's origin: external_id that id's canonical
  text, description naming the type and the policy
  (docs/design/pipeline.md §10). A fact whose handler fails, or whose
  business process answers it with any error status (a bug; decision
  14.72), is dead-lettered at its destination as rejected_reaction, and its
  sender, the policy, is answered INTERNAL ANGZARR_REASON_FACT_DEAD_LETTERED:
  to the policy that is its step done, never retried or dead-lettered again
  (decision 14.100). An arm its destination does not declare is a bug,
  refused at the destination. A policy may address its own domain, at
  another root, never its trigger's own stream (decision 14.71).
  The correlation ids below are UUIDs, as in every scenario (decision
  14.18).

  Background:
    Given an "order" aggregate "order-1" on the main timeline
    And an "inventory" aggregate "inv-7" on the main timeline
    And the inventory aggregate declares StockHoldRequested a fact, with the (fact) option

  @policy_facts
  @C-0775
  Scenario: A policy's fact for a type its destination declares is recorded with its reaction's identity
    Given a policy "OrderToInventory" translating from "order" to "inventory" that sends the fact StockHoldRequested to "inventory" "inv-7" on OrderPlaced
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000060" is recorded for order "order-1"
    Then "inventory" "inv-7" records the StockHoldRequested fact
    And the fact's command_id is the reaction command id of that OrderPlaced event, the policy "OrderToInventory" and index 0
    And the events it records carry the fact cause with that command_id and an origin whose external_id is that command_id's canonical text and whose description names StockHoldRequested and "OrderToInventory"
    And the StockHoldRequested events carry correlation_id "0192a6b0-0000-7000-8000-000000000060"
    And nothing is dead-lettered and no alert is raised

  @policy_facts
  @C-0776
  Scenario: A reaction's commands and facts share one index space
    Given a policy "OrderToInventory" translating from "order" to "inventory" whose business code answers an OrderPlaced event with a ReserveStock command and then a StockHoldRequested fact for "inventory" "inv-7"
    When an OrderPlaced event is recorded for order "order-1"
    Then "inventory" "inv-7" handles ReserveStock
    And "inventory" "inv-7" records the StockHoldRequested fact
    And the ReserveStock command's command_id is the reaction command id of that OrderPlaced event, the policy "OrderToInventory" and index 0
    And the fact's command_id is the reaction command id of that OrderPlaced event, the policy "OrderToInventory" and index 1

  @policy_facts
  @C-0777
  Scenario: A type its destination turns from a command into a fact keeps its reaction's id, so a re-run deduplicates
    # Owner's ruling (decision 14.69): one id for either arm, so a re-run
    # across the change is answered DUPLICATE.
    Given a policy "OrderToInventory" translating from "order" to "inventory" that sends ReserveStock to "inventory" "inv-7" on OrderPlaced
    And an OrderPlaced event was recorded for order "order-1" and its ReserveStock applied by "inventory" "inv-7"
    And "inventory" now declares ReserveStock with the (fact) option instead of (command), and the policy's generated code sends it in the fact arm
    When the policy is run again over that OrderPlaced event
    Then the ReserveStock fact carries the command_id the ReserveStock command carried
    And it is answered DUPLICATE, and nothing new is appended to stream "inventory" root "inv-7"

  @policy_facts
  @C-0778
  Scenario: A policy's fact whose handler fails is dead-lettered at its destination, and the policy's step completes
    Given a policy "OrderToInventory" translating from "order" to "inventory" whose business code answers an OrderPlaced event with a StockHoldRequested fact and then a ReserveStock command for "inventory" "inv-7"
    And "inventory"'s StockHoldRequested handler fails with INTERNAL
    When an OrderPlaced event is recorded for order "order-1"
    Then the StockHoldRequested fact is dead-lettered at "inventory" as rejected_reaction, and an alert is raised
    And the policy's delivery of it is answered INTERNAL with reason ANGZARR_REASON_FACT_DEAD_LETTERED, naming the stream "inventory" "inv-7" and its cause
    And "inventory" "inv-7" handles ReserveStock
    And the OrderPlaced trigger is not dead-lettered
    And the policy "OrderToInventory"'s position on order "order-1" is past the OrderPlaced event

  @policy_facts
  @C-0779
  Scenario Outline: An arm its destination does not declare is refused there as a bug and dead-lettered
    # Owner's ruling (decision 14.69): the descriptor names no fact types,
    # so the policy's coordinator cannot check the arm; the destination's
    # router refuses one that does not match its handler's kind.
    Given a policy "OrderToInventory" translating from "order" to "inventory" that sends <type> as a <arm> to "inventory" "inv-7" on OrderPlaced
    When an OrderPlaced event is recorded for order "order-1"
    Then the <type> <arm> is refused INTERNAL with reason <reason> by "inventory"'s router, dead-lettered as rejected_reaction, and an alert is raised
    And nothing is appended to stream "inventory" root "inv-7"

    Examples:
      | type               | arm     | reason                             |
      | ReserveStock       | fact    | ANGZARR_REASON_NO_FACT_HANDLER      |
      | StockHoldRequested | command | ANGZARR_REASON_UNKNOWN_COMMAND_TYPE |

  @policy_facts
  @C-0780
  Scenario: A policy may address its own domain, at another root
    # Owner's ruling (decision 14.69): a same-domain policy is allowed.
    Given a policy "OrderToOrder" translating from "order" to "order" that sends AddItem to "order" "order-2" on OrderPlaced
    When an OrderPlaced event is recorded for order "order-1"
    Then "order" "order-2" handles AddItem
    And nothing is dead-lettered and no alert is raised

  @policy_facts
  @C-0784
  Scenario Outline: A policy's output addressed to its trigger's own stream is a policy bug
    # Owner's ruling (decision 14.71): a same-domain policy is allowed, as
    # long as it never addresses its trigger's exact stream (its domain,
    # its root, and its edition, which every output is stamped with). Its
    # own domain at another root stays allowed (C-0780).
    Given "order" "order-1" is forked into edition "what-if"
    And a policy "OrderToOrder" translating from "order" to "order" that sends AddItem as a <arm> to "order" "order-1" on OrderPlaced
    When an OrderPlaced event is recorded for order "order-1" <timeline>
    Then the policy's answer is refused INTERNAL with reason ANGZARR_REASON_POLICY_TARGETS_ITS_TRIGGER, naming the trigger's stream and command_type AddItem
    And nothing is delivered and nothing is appended to stream "order" root "order-1"
    And the OrderPlaced trigger is dead-lettered with reason ANGZARR_REASON_POLICY_TARGETS_ITS_TRIGGER as rejected_events, retry_count 0, and an alert is raised

    Examples:
      | arm     | timeline             |
      | command | on the main timeline |
      | fact    | on the main timeline |
      | command | in edition "what-if" |

  @policy_facts
  @C-0785
  Scenario: A policy's fact its destination's business process answers with an error status is dead-lettered there, and the policy's step completes
    # Owner's ruling (decision 14.72): a fact is never refused, so any
    # error status its business process answers is that process's bug; the
    # fact is dead-lettered like any input that cannot be delivered, never
    # taken down the refusal path, and the policy takes
    # ANGZARR_REASON_FACT_DEAD_LETTERED as its step done (decision 14.100).
    Given a policy "OrderToInventory" translating from "order" to "inventory" whose business code answers an OrderPlaced event with a StockHoldRequested fact and then a ReserveStock command for "inventory" "inv-7"
    And "inventory"'s business process answers the StockHoldRequested fact FAILED_PRECONDITION
    When an OrderPlaced event is recorded for order "order-1"
    Then the StockHoldRequested fact is dead-lettered at "inventory" as rejected_reaction, and an alert is raised
    And the policy's delivery of it is answered INTERNAL with reason ANGZARR_REASON_FACT_DEAD_LETTERED
    And "OrderToInventory"'s Translate is not called
    And no refusal fact is recorded on "order" "order-1"
    And "inventory" "inv-7" handles ReserveStock
    And the OrderPlaced trigger is not dead-lettered
    And the policy "OrderToInventory"'s position on order "order-1" is past the OrderPlaced event
