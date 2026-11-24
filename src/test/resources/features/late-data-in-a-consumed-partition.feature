Feature: A record that arrives late to a partition already read makes the incremental gold aggregate again

  An incremental mounter of gold keeps how many files each partition of silver had when it read it. When that number
  changes in a partition it has already read, the round aggregates the whole data mart again from the start, with
  every closed partition, and warns which partitions made it do so (ADR-037).

  Background:
    Given aggregate metadata in memory
    And a margin of 15 minutes to close a partition

  @DEV-136
  Scenario: A record that arrives late to a partition already read is added to its aggregate
    Given silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
      | ES      | 20     |
    And silver has the payments of the closed partition "year=2026/month=11/day=17/hour=11":
      | country | amount |
      | FR      | 40     |
    And the mounter totals the amount of each country
    And the mounter has mounted once
    And silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 5      |
    When the mounter mounts again
    Then gold has the total 35 for the country "ES"
    And gold has the total 40 for the country "FR"
    And gold has a single result for the country "ES"
    And the mounter warns that the partition "year=2026/month=11/day=17/hour=10" changed after it was read

  @DEV-136
  Scenario: A payment of silver taken out of the quarantine reaches the incremental gold
    Given silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
    And the mounter totals the amount of each country
    And the mounter has mounted once
    And a payment taken out of the quarantine is written back to the partition "year=2026/month=11/day=17/hour=10" of silver:
      | country | amount |
      | ES      | 5      |
    When the mounter mounts again
    Then gold has the total 15 for the country "ES"

  @DEV-136
  Scenario: A round in which no partition already read has changed does not aggregate again
    Given silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
    And the mounter totals the amount of each country
    And the mounter has mounted once
    When the mounter mounts again
    Then the mounter reads no partition
    And the mounter does not warn that any partition changed

  @DEV-136
  Scenario: A partition read before the mounter kept how many files it had does not make it aggregate again
    Given silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
    And the mounter totals the amount of each country
    And the mounter has mounted once without keeping how many files each partition had
    When the mounter mounts again
    Then the mounter reads no partition
    And the mounter does not warn that any partition changed

  @DEV-136
  Scenario: From then on, a record that arrives late to that partition is added to its aggregate
    Given silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
    And the mounter totals the amount of each country
    And the mounter has mounted once without keeping how many files each partition had
    And the mounter has mounted again
    And silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 5      |
    When the mounter mounts again
    Then gold has the total 15 for the country "ES"
