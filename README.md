# MIGRATOR

[![Build Status](https://secure.travis-ci.org/jbuchbinder/migrator.png)](http://travis-ci.org/jbuchbinder/migrator)
[![Go Report Card](https://goreportcard.com/badge/github.com/jbuchbinder/migrator)](https://goreportcard.com/report/github.com/jbuchbinder/migrator)
[![GoDoc](https://godoc.org/github.com/jbuchbinder/migrator?status.png)](https://godoc.org/github.com/jbuchbinder/migrator)

ETL / data migrator.

## Parameters

| Parameter             | Type    | Default | Description                                                            |
| --------------------- | ------- | ------- | ---------------------------------------------------------------------- |
| ``BatchSize``         | integer | 1000    | Extractor: Number of rows polled from the source database at a time    |
| ``Debug``             | bool    | false   | Show additional debugging information                                  |
| ``InsertBatchSize``   | integer | 100     | Loader: Number of rows inserted per statement                          |
| ``OnlyPast``          | bool    | false   | Extractor(timestamp): Only poll for timestamps in the past ( #1 )      |
| ``SequentialReplace`` | bool    | false   | Loader: Use REPLACE instead of INSERT for sequentially extracted data. |
| ``SleepBetweenRuns``  | integer | 5       | Migrator: Seconds to sleep when no data has been found                 |

## Extractors

* **Sequential**: Tracks status via a table's primary key to see whether or not the table entries have been migrated. Useful for RO data which is written in sequence and not updated.
* **Timestamp**: Tracks status via a table's written timestamp column to determine whether table entries have been migrated from that point on.
* **Queue**: Tracks status via a triggered table which contains indexed entries which need to be migrated. This requires modification of the source database to include Insert and Update triggers. Useful for all kinds of data, but needs modification to source database.

## Tracking Table

```
CREATE TABLE `EtlTracking` (
	sourceDatabase		VARCHAR(100) DEFAULT '',
	sourceTable		VARCHAR(100) DEFAULT '',
	columnName		VARCHAR(100) DEFAULT '',
	sequentialPosition	BIGINT DEFAULT 0,
	timestampPosition	TIMESTAMP NULL DEFAULT NULL,
	lastRun			TIMESTAMP NULL DEFAULT NULL
);
```

## RecordQueue Table

```
CREATE TABLE `MigratorRecordQueue` (
	sourceDatabase		VARCHAR(100) NOT NULL,
	sourceTable			VARCHAR(100) NOT NULL,
	pkColumn 			VARCHAR(100) NOT NULL,
	pkValue 			VARCHAR(100) NOT NULL,
	timestampUpdated 	TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

	KEY (sourceDatabase, sourceTable),
	KEY (timestampUpdated)
);
```
