DROP TABLE IF EXISTS test_ingest_partition_map_target;
DROP TABLE IF EXISTS test_ingest_partition_map_source;

CREATE TABLE test_ingest_partition_map_target (`date` Date, `id` Int32, `name` Map(String, String)) ENGINE = CnchMergeTree PARTITION BY date ORDER BY id;
CREATE TABLE test_ingest_partition_map_source (`date` Date, `id` Int32, `name` Map(String, String)) ENGINE = CnchMergeTree PARTITION BY date ORDER BY id;

SYSTEM START MERGES test_ingest_partition_map_target;
SELECT 'Empty target data: START';
INSERT INTO test_ingest_partition_map_source VALUES ('2020-01-01', 1, {'key1': 'val1', 'key2': 'val2', 'key3': 'val3'});
SELECT 'SOURCE';
SELECT * from test_ingest_partition_map_source ORDER BY id;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_map_target INGEST PARTITION '2020-01-01'  COLUMNS name{'key1'}, name{'key2'} FROM test_ingest_partition_map_source;
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'Empty target data: END';
SELECT 'Empty source data: START';
TRUNCATE TABLE test_ingest_partition_map_source;
SELECT 'TARGET';
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_map_target INGEST PARTITION '2020-01-01'  COLUMNS name{'key1'}, name{'key2'} FROM test_ingest_partition_map_source;
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'Empty source data: END';
SELECT 'SOURCE and TARGET have same key, same row count, map key overlap: START';
INSERT INTO test_ingest_partition_map_source VALUES ('2020-01-01', 1, {'key1': 'update_val1', 'key2': 'update_val2', 'key3': 'val3'});
SELECT 'SOURCE';
SELECT * from test_ingest_partition_map_source ORDER BY id;
SELECT 'TARGET';
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_map_target INGEST PARTITION '2020-01-01'  COLUMNS name{'key1'}, name{'key3'} FROM test_ingest_partition_map_source;
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'SOURCE and TARGET have same key, same row count, map key overlap: END';
SELECT 'SOURCE has key that TARGET dont have, TARGET has key that SOURCE dont have: START';
INSERT INTO test_ingest_partition_map_source VALUES ('2020-01-01', 2, {'key1': 'update_val1', 'key2': 'update_val2', 'key3': 'update_val3'});
SELECT 'SOURCE';
SELECT * from test_ingest_partition_map_source ORDER BY id;
SELECT 'TARGET';
INSERT INTO test_ingest_partition_map_target VALUES ('2020-01-01', 3, {'key1': 'val1', 'key2': 'val2', 'key3': 'val3'});
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_map_target INGEST PARTITION '2020-01-01'  COLUMNS name{'key1'}, name{'key2'} FROM test_ingest_partition_map_source;
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'SOURCE has key that TARGET dont have, TARGET has key that SOURCE dont have: END';

SELECT 'SOURCE has key that TARGET dont have, TARGET has key that SOURCE dont have, ingest_default_column_value_if_not_provided=false: START';
DROP TABLE IF EXISTS test_ingest_partition_map_target;
CREATE TABLE test_ingest_partition_map_target (`date` Date, `id` Int32, `name` Map(String, String)) ENGINE = CnchMergeTree PARTITION BY date ORDER BY id SETTINGS ingest_default_column_value_if_not_provided = 0;
SYSTEM START MERGES test_ingest_partition_map_target;
SELECT 'SOURCE';
SELECT * from test_ingest_partition_map_source ORDER BY id;
SELECT 'TARGET';
INSERT INTO test_ingest_partition_map_target VALUES ('2020-01-01', 1, {'key1': 'val1', 'key2': 'val2', 'key3': 'val3'}), ('2020-01-01', 3, {'key1': 'val1', 'key2': 'val2', 'key3': 'val3'});
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_map_target INGEST PARTITION '2020-01-01'  COLUMNS name{'key1'}, name{'key2'} FROM test_ingest_partition_map_source;
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'SOURCE has key that TARGET dont have, TARGET has key that SOURCE dont have, ingest_default_column_value_if_not_provided=false: END';
SELECT 'SOURCE and TARGET dont share key, ingest_default_column_value_if_not_provided=false: START';
SELECT 'SOURCE';
TRUNCATE TABLE test_ingest_partition_map_source;
INSERT INTO test_ingest_partition_map_source VALUES ('2020-01-01', 4, {'key1': 'update_val1', 'key2': 'update_val2', 'key3': 'update_val3'});
SELECT * FROM test_ingest_partition_map_source ORDER BY id;
SELECT 'TARGET';
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_map_target INGEST PARTITION '2020-01-01'  COLUMNS name{'key1'}, name{'key2'} FROM test_ingest_partition_map_source;
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'SOURCE and TARGET dont share key, ingest_default_column_value_if_not_provided=false: END';
SELECT 'SOURCE and TARGET dont share key: START';
DROP TABLE IF EXISTS test_ingest_partition_map_target;
CREATE TABLE test_ingest_partition_map_target (`date` Date, `id` Int32, `name` Map(String, String)) ENGINE = CnchMergeTree PARTITION BY date ORDER BY id;
SYSTEM START MERGES test_ingest_partition_map_target;
SELECT 'SOURCE';
SELECT * FROM test_ingest_partition_map_source ORDER BY id;
SELECT 'TARGET';
INSERT INTO test_ingest_partition_map_target VALUES ('2020-01-01', 2, {'key1': 'val1', 'key2': 'val2', 'key3': 'val3'});
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_map_target INGEST PARTITION '2020-01-01'  COLUMNS name{'key1'}, name{'key2'} FROM test_ingest_partition_map_source;
SELECT * FROM test_ingest_partition_map_target ORDER BY id;
SELECT 'SOURCE and TARGET dont share key: END';
SELECT 'INSERT many map key: START';
TRUNCATE TABLE test_ingest_partition_map_source;
TRUNCATE TABLE test_ingest_partition_map_target;
INSERT INTO test_ingest_partition_map_source VALUES ('2020-01-01', 1, {'key1': 'val1', 'key2': 'val2', 'key3': 'val3'}), ('2020-01-01', 2, {'key4': 'val4', 'key5': 'val5', 'key6': 'val6'}), ('2020-01-01', 3, {'key7': 'val7', 'key8': 'val8', 'key9': 'val9'}), ('2020-01-01', 4, {'key10': 'val10', 'key11': 'val11', 'key12': 'val12'});
INSERT INTO test_ingest_partition_map_target VALUES ('2020-01-01', 1, {'key13': 'val13'});
ALTER TABLE test_ingest_partition_map_target INGEST PARTITION '2020-01-01'  COLUMNS name{'key1'}, name{'key2'}, name{'key3'}, name{'key4'}, name{'key5'}, name{'key6'}, name{'key7'}, name{'key8'}, name{'key9'}, name{'key10'} FROM test_ingest_partition_map_source;
SELECT * FROM test_ingest_partition_map_target ORDER BY id;

DROP TABLE IF EXISTS test_ingest_partition_map_target;
DROP TABLE IF EXISTS test_ingest_partition_map_source;
