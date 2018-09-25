package migrator

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

func init() {
	ExtractorMap["timestamp"] = ExtractorTimestamp
}

// ExtractorTimestamp is an Extractor instance which uses a DATETIME/TIMESTAMP
// field to determine which rows to pull from the source database table.
var ExtractorTimestamp = func(db *sql.DB, dbName, tableName string, ts TrackingStatus, params *Parameters) (bool, []SqlUntypedRow, TrackingStatus, error) {
	tag := fmt.Sprintf("ExtractorTimestamp[%s.%s]: ", dbName, tableName)

	moreData := false

	log.Printf(tag+"Beginning run with params %#v", params)

	data := make([]SqlUntypedRow, 0)
	var maxStamp time.Time

	batchSize := paramInt(*params, "BatchSize", DefaultBatchSize)
	debug := paramBool(*params, "Debug", false)

	tsStart := time.Now()

	rows, err := db.Query("SELECT * FROM `"+tableName+"` WHERE `"+ts.ColumnName+"` > ? LIMIT ?", ts.TimestampPosition, batchSize)
	if err != nil {
		return false, data, ts, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return false, data, ts, err
	}
	if debug {
		log.Printf(tag+"Columns %v", cols)
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
			log.Printf(tag + "Scan: " + err.Error())
			return false, data, ts, err
		}

		// De-reference fields
		rowData := make(SqlUntypedRow, len(cols))
		for i := range cols {
			rowData[cols[i]] = values[i]
		}
		data = append(data, rowData)
		maxStamp = timemax(maxStamp, rowData[ts.ColumnName].(time.Time))
	}

	log.Printf(tag+"Duration to extract %d rows: %s", dataCount, time.Since(tsStart).String())

	if dataCount == 0 {
		if debug {
			log.Printf(tag+"Batch size %d, row count %d; indicating no more data", batchSize, dataCount)
		}
		return false, data, ts, nil
	}

	if dataCount < batchSize {
		log.Printf(tag+"Batch size %d, row count %d; indicating no more data", batchSize, dataCount)
		moreData = false
	} else {
		log.Printf(tag+"Batch size %d == row count %d; indicating more data", batchSize, dataCount)
		moreData = true
	}

	log.Printf(tag+"%s high timestamp value %#v", ts.ColumnName, maxStamp)
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