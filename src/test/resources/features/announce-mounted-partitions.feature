Feature: A mounter announces each partition it writes, and a reactive mounter of gold mounts what silver has mounted

  When a mount writes results, the mounter publishes that each partition of its data mart that received one has been
  mounted. A reactive mounter of gold mounts when a partition of silver it reads has been mounted, and not when bronze
  is written.

  @DEV-113
  Scenario: A mount announces once each partition of the data mart it wrote
    Given bronze has these payments:
      | id        | country |
      | payment-1 | ES      |
      | payment-2 | ES      |
      | payment-3 | FR      |
    And a mounter that copies each payment to the silver data mart "payments" partitioned by "country"
    When the mounter mounts
    Then the partition "country=ES" of "silver/payments" is announced as mounted once
    And the partition "country=FR" of "silver/payments" is announced as mounted once

  @DEV-113
  Scenario: A mount that fails to write announces nothing
    Given bronze has these payments:
      | id        | country |
      | payment-1 | ES      |
    And a mounter that copies each payment to the silver data mart "payments" partitioned by "country"
    And silver cannot be written
    When the mounter mounts
    Then no partition is announced as mounted

  @DEV-113
  Scenario: A result that goes to the quarantine is not announced
    Given bronze has these payments:
      | id        | country |
      | payment-1 | ES      |
      | payment-2 |         |
    And a mounter that copies each payment to the silver data mart "payments" partitioned by "country"
    When the mounter mounts
    Then the partition "country=ES" of "silver/payments" is announced as mounted once
    And nothing else is announced as mounted

  @DEV-113
  Scenario: A mount that writes nothing announces nothing
    Given bronze has no payments
    And a mounter that copies each payment to the silver data mart "payments" partitioned by "country"
    When the mounter mounts
    Then no partition is announced as mounted

  @DEV-113
  Scenario: A reactive mounter of gold mounts when a partition of silver it reads is mounted
    Given silver has these payments in the partition "source=payments":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And a reactive mounter of gold that totals the payments of "source=payments" into "payments-total"
    When the partition "source=payments::country=ES" of "silver/payments" is announced as mounted
    Then gold has the total 35 in "payments-total"

  @DEV-113
  Scenario Outline: A reactive mounter of gold does not mount for what it does not read
    Given silver has these payments in the partition "source=payments":
      | id        | amount |
      | payment-1 | 10     |
    And a reactive mounter of gold that totals the payments of "source=payments" into "payments-total"
    When <something happens>
    Then gold has nothing in "payments-total"

    Examples:
      | something happens                                                                 |
      | the partition "source=refunds" of "silver/refunds" is announced as mounted        |
      | the partition "source=payments" of "gold/payments-by-day" is announced as mounted |
      | an envelope of "source=payments" is persisted in bronze                            |

  @DEV-132
  Scenario: A result taken out of the quarantine is announced in the partition it is written to
    Given a mounter that copies each payment to the silver data mart "payments" partitioned by "country"
    When the payment "payment-2" of the country "ES" is taken out of the quarantine
    Then the partition "country=ES" of "silver/payments" is announced as mounted once
    And nothing else is announced as mounted

  @DEV-132
  Scenario: A result taken out of the quarantine that still cannot be written announces nothing
    Given a mounter that copies each payment to the silver data mart "payments" partitioned by "country"
    When the payment "payment-2" without a country is taken out of the quarantine
    Then the payment "payment-2" is rejected again
    And no partition is announced as mounted

  @DEV-132
  Scenario: A result taken out of the quarantine announces nothing when its data mart cannot be written
    Given a mounter that copies each payment to the silver data mart "payments" partitioned by "country"
    And silver cannot be written
    When the payment "payment-2" of the country "ES" is taken out of the quarantine
    Then the payment "payment-2" is not written
    And no partition is announced as mounted
