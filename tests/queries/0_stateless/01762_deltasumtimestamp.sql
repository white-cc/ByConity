select deltaSumTimestampMerge(state) from (select deltaSumTimestampState(value, timestamp) as state from (select toDate(number) as timestamp, [4, 5, 5, 5][number-4] as value from numbers(5, 4)) UNION ALL select deltaSumTimestampState(value, timestamp) as state from (select toDate(number) as timestamp, [0, 4, 8, 3][number] as value from numbers(1, 4)));
select deltaSumTimestampMerge(state) from (select deltaSumTimestampState(value, timestamp) as state from (select number as timestamp, [0, 4, 8, 3][number] as value from numbers(1, 4)) UNION ALL select deltaSumTimestampState(value, timestamp) as state from (select number as timestamp, [4, 5, 5, 5][number-4] as value from numbers(5, 4)));
select deltaSumTimestamp(value, timestamp) from (select toDateTime(number) as timestamp, [0, 4, 8, 3][number] as value from numbers(1, 4));
select deltaSumTimestamp(value, timestamp) from (select toDateTime(number) as timestamp, [0, 4.5, 8, 3][number] as value from numbers(1, 4));
select deltaSumTimestamp(value, timestamp) from (select number as timestamp, [0, 4, 8, 3, 0, 0, 0, 1, 3, 5][number] as value from numbers(1, 10));
