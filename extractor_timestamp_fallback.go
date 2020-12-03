package migrator

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func init() {
	ExtractorMap["timestamp_fallback"] = ExtractorTimestampFallback
}

// ExtractorTimestampFallback is an Extractor instance which uses a DATETIME/TIMESTAMP
// field to determine which rows to pull from the source database table.
var ExtractorTimestampFallback = func(db *sql.DB, dbName, tableName string, ts TrackingStatus, params *Parameters) (bool, []SQLRow, TrackingStatus, error) {
	batchSize := paramInt(*params, "BatchSize", DefaultBatchSize)
	debug := paramBool(*params, "Debug", false)

	tag := fmt.Sprintf("ExtractorTimestampFallback[%s.%s]: ", dbName, tableName)

	moreData := false

	if debug {
		logger.Printf(tag+"Beginning run with params %#v", params)
	}

	data := make([]SQLRow, 0)
	var maxStamp time.Time

	tsStart := time.Now()

	// Pull column names based on backup
	colnames := strings.Split(ts.ColumnName, ",")
	if len(colnames) < 2 {
		err := fmt.Errorf("Requires two columns separated by a comma")
		logger.Printf(tag + "ERR: " + err.Error())
		return false, data, ts, err
	}

	if debug {
		logger.Printf(tag+"Query: \"SELECT * FROM `"+tableName+"` WHERE IFNULL(`"+colnames[0]+"`,`"+colnames[1]+"`) > %v LIMIT %d\"", ts.TimestampPosition, batchSize)
	}
	rows, err := db.Query("SELECT * FROM `"+tableName+"` WHERE IFNULL(`"+colnames[0]+"`,`"+colnames[1]+"`) > ? LIMIT ?", ts.TimestampPosition, batchSize)
	if err != nil {
		logger.Printf(tag + "ERR: " + err.Error())
		return false, data, ts, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return false, data, ts, err
	}
	if debug {
		logger.Printf(tag+"Columns %v", cols)
	}
	dataCount := 0
	for rows.Next() {
		dataCount++
		scanArgs := make([]interface{}, len(cols))
		values := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			logger.Printf(tag + "Scan: " + err.Error())
			return false, data, ts, err
		}

		// De-reference fields
		rowData := SQLRow{}
		rowData.Data = make(SQLUntypedRow, len(cols))
		for i := range cols {
			rowData.Data[cols[i]] = values[i]
		}
		data = append(data, rowData)

		timestamp, ok := rowData.Data[ts.ColumnName].(time.Time)
		if !ok {
			logger.Printf(tag+"ERROR: Unable to process table %s due to column %s not being a Time", dbName+"."+tableName, ts.ColumnName)
			return false, data, ts, err
		}
		maxStamp = timemax(maxStamp, timestamp)
	}

	logger.Printf(tag+"Duration to extract %d rows: %s", dataCount, time.Since(tsStart).String())

	if dataCount == 0 {
		if debug {
			logger.Printf(tag+"Batch size %d, row count %d; indicating no more data", batchSize, dataCount)
		}
		return false, data, ts, nil
	}

	if dataCount < batchSize {
		if debug {
			logger.Printf(tag+"Batch size %d, row count %d; indicating no more data", batchSize, dataCount)
		}
		moreData = false
	} else {
		if debug {
			logger.Printf(tag+"Batch size %d == row count %d; indicating more data", batchSize, dataCount)
		}
		moreData = true
	}

	if debug {
		logger.Printf(tag+"%s high timestamp value %#v", ts.ColumnName, maxStamp)
	}
	err = SetTrackingStatusTimestamp(ts.Db, dbName, tableName, maxStamp)
	// Copy old object ...
	newTs := &TrackingStatus{
		Db:             ts.Db,
		SourceDatabase: ts.SourceDatabase,
		SourceTable:    ts.SourceTable,
		ColumnName:     ts.ColumnName,
		// ... with updates
		TimestampPosition: NullTimeFromTime(maxStamp),
		LastRun:           NullTimeNow(),
	}

	(*params)["METHOD"] = "REPLACE"

	return moreData, data, *newTs, err
}
