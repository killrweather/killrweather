// This will use the new compaction strategy DateTieredCompactionStrategy (DTCS)
// available in Cassandra 2.0.11 and 2.1.1
// Use this alter table before loading data. If used
// after, use the nodetool updatesstables command to re-write your sstables

alter TABLE raw_weather_data with compaction = {'class': 'DateTieredCompactionStrategy'};

