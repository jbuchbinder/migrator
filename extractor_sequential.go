package migrator

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"time"
)

func init() {
	ExtractorMap["sequential"] = ExtractorSequential
}

// ExtractorSequential is an Extractor instance which uses the primary key
// sequence to determine which rows should be extracted from the source
// database table.
var ExtractorSequential = func(db *sql.DB, dbName, tableName string, ts TrackingStatus, params Parameters) (bool, []SqlUntypedRow, TrackingStatus, error) {
	tag := fmt.Sprintf("ExtractorSequential[%s.%s]: ", dbName, tableName)

	moreData := false

	log.Printf(tag+"Beginning run with params %#v", params)

	data := make([]SqlUntypedRow, 0)
	minSeq := int64(math.MaxInt64)
	var maxSeq int64

	batchSize := paramInt(params, "BatchSize", DefaultBatchSize)
	debug := paramBool(params, "Debug", false)

	tsStart := time.Now()

	rows, err := db.Query("SELECT * FROM `"+tableName+"` WHERE `"+ts.ColumnName+"` > ? LIMIT ?", ts.SequentialPosition, batchSize)
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
		minSeq = int64min(minSeq, rowData[ts.ColumnName].(int64))
		maxSeq = int64max(maxSeq, rowData[ts.ColumnName].(int64))
	}

	log.Printf(tag+"Duration to extract %d rows: %s", dataCount, time.Since(tsStart).String())

	if dataCount == 0 {
		if debug {
			log.Printf(tag+"Batch size %d, row count %d; indicating no more data", batchSize, dataCount)
		}
		return false, data, ts, nil
	}

	if dataCount < batchSize {
		if debug {
			log.Printf(tag+"Batch size %d, row count %d; indicating no more data", batchSize, dataCount)
		}
		moreData = false
	} else {
		if debug {
			log.Printf(tag+"Batch size %d == row count %d; indicating more data", batchSize, dataCount)
		}
		moreData = true
	}

	log.Printf(tag+"%s seq value range %d - %d", ts.ColumnName, minSeq, maxSeq)
	// Manually copy old tracking object ...
	newTs := &TrackingStatus{
		Db:             ts.Db,
		SourceDatabase: ts.SourceDatabase,
		SourceTable:    ts.SourceTable,
		ColumnName:     ts.ColumnName,
		// ... with updates
		SequentialPosition: maxSeq,
		LastRun:            NullTimeNow(),
	}

	return moreData, data, *newTs, nil
}
