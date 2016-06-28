# MIGRATOR

[![Build Status](https://secure.travis-ci.org/jbuchbinder/migrator.png)](http://travis-ci.org/jbuchbinder/migrator)
[![Go Report Card](https://goreportcard.com/badge/github.com/jbuchbinder/migrator)](https://goreportcard.com/report/github.com/jbuchbinder/migrator)
[![GoDoc](https://godoc.org/github.com/jbuchbinder/migrator?status.png)](https://godoc.org/github.com/jbuchbinder/migrator)

ETL / data migrator.

## Parameters

| Parameter            | Type    | Default Value | Description                                                         |
| -------------------- | ------- | ------------- | ------------------------------------------------------------------- |
| ``BatchSize``        | integer | 1000          | Extractor: Number of rows polled from the source database at a time |
| ``Debug``            | bool    | false         | Show additional debugging information                               |
| ``InsertBatchSize``  | integer | 100           | Loader: Number of rows inserted per statement                       |
| ``SleepBetweenRuns`` | integer | 5             | Migrator: Seconds to sleep when no data has been found              |

## Tracking Table

```
CREATE TABLE `Tracking` (
	sourceDatabase		VARCHAR(100) DEFAULT '',
	sourceTable		VARCHAR(100) DEFAULT '',
	columnName		VARCHAR(100) DEFAULT '',
	sequentialPosition	BIGINT DEFAULT 0,
	timestampPosition	TIMESTAMP NULL DEFAULT NULL,
	lastRun			TIMESTAMP NULL DEFAULT NULL
);
```

