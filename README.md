# MIGRATOR

[![Build Status](https://secure.travis-ci.org/jbuchbinder/migrator.png)](http://travis-ci.org/jbuchbinder/migrator)
[![Go Report Card](https://goreportcard.com/badge/github.com/jbuchbinder/migrator)](https://goreportcard.com/report/github.com/jbuchbinder/migrator)
[![GoDoc](https://godoc.org/github.com/jbuchbinder/migrator?status.png)](https://godoc.org/github.com/jbuchbinder/migrator)

ETL / data migrator.

## Parameters

| Parameter | Type | Default Value | Description |
| -- | -- | -- |
| ``BatchSize`` | integer | 1000 | Extractor: Number of rows polled from the source database at a time |
| ``InsertBatchSize`` | integer | 100 | Loader: Number of rows inserted per statement |
| ``Debug`` | bool | false | Show additional debugging information |

