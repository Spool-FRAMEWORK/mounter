Feature: A mounter aggregates incrementally and keeps where each aggregate comes from

  With an incremental aggregator and aggregate metadata, a mounter of gold reads in each round only the closed
  partitions of silver it has not read yet, folds them into the state of each aggregate and replaces its result in
  gold (ADR-027). A partition is closed when its last date entry has ended and a margin has passed.

  Background:
    Given aggregate metadata in memory
    And a margin of 15 minutes to close a partition

  @DEV-87
  Scenario: A round reads only the closed partitions it has not read yet
    Given silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
      | ES      | 20     |
    And the mounter totals the amount of each country
    And the mounter has mounted once
    And silver has the payments of the closed partition "year=2026/month=11/day=17/hour=11":
      | country | amount |
      | ES      | 5      |
    When the mounter mounts again
    Then the mounter reads only the partition "year=2026/month=11/day=17/hour=11"
    And gold has the total 35 for the country "ES"

  @DEV-87
  Scenario Outline: Aggregating round by round gives the same result as aggregating everything at once
    Given silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
      | FR      | 40     |
      | ES      | 30     |
    And the mounter <aggregates> each country
    And the mounter has mounted once
    And silver has the payments of the closed partition "year=2026/month=11/day=17/hour=11":
      | country | amount |
      | ES      | 50     |
      | FR      | 20     |
    When the mounter mounts again
    Then gold has the same result for each country as aggregating every partition at once

    Examples:
      | aggregates            |
      | totals the amount of  |
      | averages the amount of |

  @DEV-87
  Scenario: Each aggregate keeps the partitions that form it and the state of its aggregator
    Given silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
    And silver has the payments of the closed partition "year=2026/month=11/day=17/hour=11":
      | country | amount |
      | ES      | 30     |
      | FR      | 5      |
    And the mounter averages the amount of each country
    When the mounter mounts
    Then the metadata of the aggregate "country=ES" names the partitions "year=2026/month=11/day=17/hour=10" and "year=2026/month=11/day=17/hour=11"
    And the metadata of the aggregate "country=ES" keeps the sum 40 and the count 2
    And the metadata of the aggregate "country=FR" names only the partition "year=2026/month=11/day=17/hour=11"

  @DEV-87
  Scenario: A partition is not read until its hour has ended and the margin has passed
    Given the time is "2026-11-17T11:10:00Z"
    And silver has the payments of the partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
    And the mounter totals the amount of each country
    When the mounter mounts
    Then gold has no result
    When the time is "2026-11-17T11:16:00Z"
    And the mounter mounts again
    Then gold has the total 10 for the country "ES"

  @DEV-87
  Scenario: The result of an aggregate that changes replaces the previous one in gold
    Given silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
    And the mounter totals the amount of each country
    And the mounter has mounted once
    And silver has the payments of the closed partition "year=2026/month=11/day=17/hour=11":
      | country | amount |
      | ES      | 5      |
    When the mounter mounts again
    Then gold has a single result for the country "ES"
    And gold has the total 15 for the country "ES"

  @DEV-87
  Scenario: A scope with a partition without a date is aggregated again in full in every round
    Given silver has the payments of the partition "country=ES":
      | country | amount |
      | ES      | 10     |
    And the mounter totals the amount of each country
    And the mounter has mounted once
    And silver has the payments of the partition "country=ES":
      | country | amount |
      | ES      | 5      |
    When the mounter mounts again
    Then the mounter reads every partition of its scope
    And gold has a single result for the country "ES"
    And gold has the total 15 for the country "ES"

  @DEV-87
  Scenario: A result that fails to be published is published in the next round
    Given silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
    And the mounter totals the amount of each country
    And gold fails to write once
    And the mounter has mounted once
    When the mounter mounts again
    Then gold has the total 10 for the country "ES"
    And the metadata of the aggregate "country=ES" keeps the sum 10

  @DEV-87
  Scenario: Without aggregate metadata the mounter aggregates everything in every round
    Given there is no aggregate metadata
    And silver has the payments of the closed partition "year=2026/month=11/day=17/hour=10":
      | country | amount |
      | ES      | 10     |
    And the mounter totals the amount of each country
    And the mounter has mounted once
    When the mounter mounts again
    Then the mounter reads every partition of its scope
