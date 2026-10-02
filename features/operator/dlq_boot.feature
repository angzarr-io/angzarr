Feature: DLQ configuration is enforced at bin boot
  R2-15 closes the silent-drop-on-misconfigured-DLQ bug class by making
  every sidecar binary hard-fail at startup when the operator configures
  DLQ but the backend cannot be reached. The contract applies uniformly
  to angzarr-aggregate, angzarr-saga, angzarr-process-manager,
  angzarr-projector, and to the read-side audit reader in angzarr-status.

  The flip side: an operator who deliberately leaves `dlq.targets` (or
  `dlq.audit`) empty keeps a working binary that logs a WARN explaining
  the always-empty audit trail. Both halves are exercised below.

  # ==========================================================================
  # Publisher-side (steps 3-4 + 5a + 5b + 6 wiring)
  # ==========================================================================

  Scenario Outline: Operator-configured DLQ broker is unreachable for <binary>
    Given the operator configures dlq.targets pointing at an unreachable AMQP broker
    When the <binary> binary starts
    Then the binary exits with a non-zero status
    And the operator sees an error message naming the unreachable target

    Examples:
      | binary                   |
      | angzarr-aggregate        |
      | angzarr-saga             |
      | angzarr-process-manager  |
      | angzarr-projector        |

  Scenario Outline: Operator omits dlq configuration entirely for <binary>
    Given the operator's config has no dlq section
    When the <binary> binary starts
    Then the binary logs a WARN naming the empty dlq.targets configuration
    And the binary proceeds to serve requests

    Examples:
      | binary                   |
      | angzarr-aggregate        |
      | angzarr-saga             |
      | angzarr-process-manager  |
      | angzarr-projector        |

  Scenario Outline: Unknown DLQ backend type fails boot for <binary>
    Given the operator configures dlq.targets with backend type "no-such-backend"
    When the <binary> binary starts
    Then the binary exits with a non-zero status
    And the operator sees an error message naming the unknown backend type

    Examples:
      | binary                   |
      | angzarr-aggregate        |
      | angzarr-saga             |
      | angzarr-process-manager  |
      | angzarr-projector        |

  # ==========================================================================
  # Reader-side (step 8 wiring)
  # ==========================================================================

  Scenario: Operator-configured DLQ audit backend is unreachable
    Given the operator configures dlq.audit pointing at an unreachable database
    When the angzarr-status binary starts
    Then the binary exits with a non-zero status
    And the operator sees an error message naming the unreachable audit backend

  Scenario: Operator omits dlq.audit configuration
    Given the operator's config has no dlq.audit section
    When the angzarr-status binary starts
    Then the binary logs a WARN naming the missing dlq.audit configuration
    And the binary proceeds to serve requests
    And the DLQ admin listing reports zero entries

  Scenario: Unknown DLQ audit storage_type fails boot
    Given the operator configures dlq.audit.storage_type as "no-such-db"
    When the angzarr-status binary starts
    Then the binary exits with a non-zero status
    And the operator sees an error message naming the unknown storage_type
