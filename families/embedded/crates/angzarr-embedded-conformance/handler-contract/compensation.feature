Feature: Compensation - a process manager's rollback, kept by the client library
  A process manager that cannot finish undoes what it did (decision 14.74).
  Each step is paired in code with its undo, never in proto (decision 2.3):
  an event of the PM's own that its outbound policy turns into what the
  step's destination declares, a fact where it can (decision 14.69), so an
  undo is never refused and compensation never loops. The journal is the
  PM's own events, folded into its state: a step is completed once its
  reply is folded. The client library keeps the pairing and builds the
  rollback, so authors never write it twice.

  Bound by each client library's process-manager tooling, angzarr-embedded's
  markup (Rust) and client-python, so the two agree; it is no router's
  dispatch. The fixture is fixture/compensation.proto: the shipment process
  manager, whose steps are, in order, "reserve stock" (completed by
  StockReserved, undone by StockReleaseDue, which becomes ReleaseStock) and
  "charge payment" (PaymentCharged, undone by RefundDue, which becomes
  RefundPayment); its failed event is ShipmentFailed.

  An undo's target must be a fact (decision 14.83): a step whose undo
  targets a command is refused when it is registered, so no undo is ever
  refused and there is no CompensationFailed.

  Background:
    Given the shipment process manager's steps, registered at startup

  @C-0787
  Scenario: A rollback records the failure, then undoes each completed step, newest first
    Given the shipment has completed "reserve stock" and "charge payment"
    When it rolls back because "address undeliverable"
    Then it records ShipmentFailed "address undeliverable", RefundDue and StockReleaseDue, in that order

  @C-0788
  Scenario: A rollback undoes only the steps that completed
    Given the shipment has completed "reserve stock"
    When it rolls back because "payment declined"
    Then it records ShipmentFailed "payment declined" and StockReleaseDue, in that order

  @C-0789
  Scenario: A rollback before any step completed records the failure alone
    Given the shipment has completed no step
    When it rolls back because "out of stock"
    Then it records ShipmentFailed "out of stock" alone

  @C-0790
  Scenario: A reply that arrives after the rollback is recorded and undone at once
    # The one subtle case (steps in flight together): the tooling owns it,
    # so no author reinvents it.
    Given the shipment has completed "reserve stock" and rolled back
    When the reply completing "charge payment" arrives
    Then it records PaymentCharged and RefundDue, in that order

  @C-0791
  Scenario: A reply before any rollback is recorded alone
    Given the shipment has completed "reserve stock"
    When the reply completing "charge payment" arrives
    Then it records PaymentCharged alone

  @C-0792
  Scenario Outline: Registering a step checks that its undo's target is a fact
    # Decision 14.74, the owner's rulings 6 and Q10: which event undoes which
    # step is known only in code, so the library checks at startup, when the
    # step is registered. An undo targeting a command is refused, not
    # warned of (decision 14.83).
    Given "reserve stock" is registered with an undo that becomes <target>
    When the shipment process manager starts
    Then <outcome>

    Examples:
      | target                           | outcome                                                                                                                      |
      | "test.compensation.ReleaseStock" | registration succeeds                                                                                                        |
      | "test.compensation.RestockItems" | registration is refused, naming the step "reserve stock" and its undo's target, the command "test.compensation.RestockItems" |

  # C-0793 (a refused undo recorded as CompensationFailed) and C-0796 (that
  # handling succeeding, nothing dead-lettered) are retired: an undo's target
  # must be a fact, so no undo can be refused, and CompensationFailed is
  # deleted (decision 14.83; owner decision 38 is moot).
