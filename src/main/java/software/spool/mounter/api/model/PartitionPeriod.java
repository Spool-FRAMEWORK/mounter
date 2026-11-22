package software.spool.mounter.api.model;

import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The time a partition covers, from its date entries in UTC: {@code year=2026::month=11::day=17::hour=10} covers from
 * 10:00 to 11:00 of that day, and {@code year=2026::month=11} the whole month.
 *
 * @param start when it begins
 * @param end   when its last date entry ends
 */
public record PartitionPeriod(Instant start, Instant end) {
    private static final String NAME_VALUE_SEPARATOR = "=";
    private static final List<DateEntry> DATE_ENTRIES = List.of(
            new DateEntry("year", ChronoUnit.YEARS, 1),
            new DateEntry("month", ChronoUnit.MONTHS, 1),
            new DateEntry("day", ChronoUnit.DAYS, 1),
            new DateEntry("hour", ChronoUnit.HOURS, 0));

    private record DateEntry(String name, ChronoUnit unit, int firstValue) {}

    /** The period of the partition, none if it has no year or its date entries are not a date. */
    public static Optional<PartitionPeriod> of(PartitionKey partition) {
        Map<String, String> entries = entriesOf(partition);
        int[] values = DATE_ENTRIES.stream().mapToInt(DateEntry::firstValue).toArray();
        Optional<ChronoUnit> lastUnit = Optional.empty();
        try {
            for (int position = 0; position < DATE_ENTRIES.size() && entries.containsKey(DATE_ENTRIES.get(position).name()); position++) {
                values[position] = Integer.parseInt(entries.get(DATE_ENTRIES.get(position).name()));
                lastUnit = Optional.of(DATE_ENTRIES.get(position).unit());
            }
            LocalDateTime start = LocalDateTime.of(values[0], values[1], values[2], values[3], 0);
            return lastUnit.map(unit -> new PartitionPeriod(start.toInstant(ZoneOffset.UTC),
                    start.plus(1, unit).toInstant(ZoneOffset.UTC)));
        } catch (NumberFormatException | DateTimeException e) {
            return Optional.empty();
        }
    }

    private static Map<String, String> entriesOf(PartitionKey partition) {
        Map<String, String> entries = new LinkedHashMap<>();
        KeyEntries.split(partition.value()).stream()
                .filter(entry -> entry.contains(NAME_VALUE_SEPARATOR))
                .map(KeyEntries::parse)
                .forEach(entries::putAll);
        return entries;
    }
}
