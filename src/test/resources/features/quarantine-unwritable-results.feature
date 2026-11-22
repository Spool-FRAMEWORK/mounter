Feature: The mounter keeps in quarantine the results it cannot write

  A result that cannot be written to its data mart, because it lacks an attribute its partition is built from,
  is kept in the quarantine with the reason instead of being lost. The rest of the results are still written.

  Background:
    Given these orders are in bronze:
      | order   | day        | country | amount |
      | order-1 | 2026-11-20 | ES      | 10     |
      | order-2 | 2026-11-20 | ES      | 5      |
      | order-3 | 2026-11-21 |         | 7      |

  @DEV-97
  Scenario: An aggregated result without a partition attribute goes to quarantine
    Given a mount that adds up the amount of each day and country into the gold data mart "payments"
    And the data mart is partitioned by "country"
    When the mounter mounts bronze
    Then "payments" has the result of day "2026-11-20" in the partition "country=ES"
    And the result of day "2026-11-21" is not in "payments"
    And the result of day "2026-11-21" is pending in quarantine because a partition attribute is missing
    And its quarantine says the attribute "country" is missing
    And its quarantine says it belongs to the data mart "gold/payments"
    And a quarantine of the result of day "2026-11-21" is announced

  @DEV-97
  Scenario: A transformed result without a partition attribute goes to quarantine
    Given a mount that turns each order into a payment in the silver data mart "payments"
    And the data mart is partitioned by "country"
    When the mounter mounts bronze
    Then "payments" has the payment of "order-1" in the partition "country=ES"
    And the payment of "order-3" is not in "payments"
    And the payment of "order-3" is pending in quarantine because a partition attribute is missing
    And its quarantine says it belongs to the data mart "silver/payments"

  @DEV-97
  Scenario: A result that cannot be written again keeps a single quarantine entry
    Given a mount that adds up the amount of each day and country into the gold data mart "payments"
    And the data mart is partitioned by "country"
    When the mounter mounts bronze
    And the mounter mounts bronze
    Then the result of day "2026-11-21" is pending in quarantine because a partition attribute is missing
    And its quarantine has a single entry

  @DEV-97
  Scenario: An order copied as it is to silver without a partition attribute goes back to bronze
    Given a mount that copies each order as it is to the silver data mart "orders"
    And the data mart is partitioned by "country"
    When the mounter mounts bronze
    Then "order-3" is pending in quarantine because a partition attribute is missing
    And its quarantine says it stayed in bronze
    And "order-3" is no longer in bronze

  @DEV-97
  Scenario: Without a quarantine, a result that cannot be written is reported as an error
    Given a mount without quarantine that adds up the amount of each day and country into the gold data mart "payments"
    And the data mart is partitioned by "country"
    When the mounter mounts bronze
    Then the result of day "2026-11-21" is not in "payments"
    And an error reports that the attribute "country" is missing
