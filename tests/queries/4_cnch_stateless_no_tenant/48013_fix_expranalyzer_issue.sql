SELECT toDateTime(t, 'UTC') between toDateTime('1970-01-02 00:00:00', 'UTC') and toDateTime('2022-01-03 06:51:40', 'UTC') FROM (SELECT '1970-01-01 00:00:00' AS t FROM system.one);