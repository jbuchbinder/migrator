package migrator

import (
	"database/sql"
	"fmt"
	"math"
	"time"
)

func init() {
	ExtractorMap["sequential"] = ExtractorSequential
}

// ExtractorSequential is an Extractor instance which uses the primary key
// sequence to determine which rows should be extracted from the source
// database table.
var ExtractorSequential = func(db *sql.DB, dbName, tableName string, ts TrackingStatus, params *Parameters) (bool, []SQLRow, TrackingStatus, error) {
	batchSize := paramInt(*params, ParamBatchSize, DefaultBatchSize)
	sequentialReplace := paramBool(*params, ParamSequentialReplace, false)
	debug := paramBool(*params, ParamDebug, false)

	tag := fmt.Sprintf("ExtractorSequential[%s.%s]: ", dbName, tableName)

	moreData := false

	if debug {
		logger.Debugf(tag+"Beginning run with params %#v", params)
	}

	data := make([]SQLRow, 0)
	minSeq := int64(math.MaxInt64)
	var maxSeq int64

	tsStart := time.Now()

	if debug {
		logger.Debugf(tag+"Query: \"SELECT * FROM `"+tableName+"` WHERE `"+ts.ColumnName+"` > %d LIMIT %d\"", ts.SequentialPosition, batchSize)
	}
	rows, err := db.Query("SELECT * FROM `"+tableName+"` WHERE `"+ts.ColumnName+"` > ? LIMIT ?", ts.SequentialPosition, batchSize)
	if err != nil {
		logger.Errorf(tag + "ERR: " + err.Error())
		return false, data, ts, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return false, data, ts, err
	}
	if debug {
		logger.Debugf(tag+"Columns %v", cols)
	}
	dataCount := 0
	for rows.Next() {
		dataCount++
		scanArgs := make([]any, len(cols))
		values := make([]any, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			logger.Errorf(tag + "Scan: " + err.Error())
			return false, data, ts, err
		}

		// De-reference fields
		rowData := SQLRow{}
		if sequentialReplace {
			rowData.Method = "REPLACE"
		} else {
			rowData.Method = "INSERT"
		}
		rowData.Data = make(SQLUntypedRow, len(cols))
		for i := range cols {
			rowData.Data[cols[i]] = values[i]
		}
		data = append(data, rowData)

		// Sanity check the column before committing to avoid panics during casting
		seqno, ok := rowData.Data[ts.ColumnName].(int64)
		if !ok {
			logger.Errorf(tag+"ERROR: Unable to process table %s due to column %s not being an integer", dbName+"."+tableName, ts.ColumnName)
			return false, data, ts, nil
		}
		minSeq = int64min(minSeq, seqno)
		maxSeq = int64max(maxSeq, seqno)
	}

	logger.Infof(tag+"Duration to extract %d rows: %s", dataCount, time.Since(tsStart).String())

	if dataCount == 0 {
		if debug {
			logger.Debugf(tag+"Batch size %d, row count %d; indicating no more data", batchSize, dataCount)
		}
		return false, data, ts, nil
	}

	if dataCount < batchSize {
		if debug {
			logger.Debugf(tag+"Batch size %d, row count %d; indicating no more data", batchSize, dataCount)
		}
		moreData = false
	} else {
		if debug {
			logger.Debugf(tag+"Batch size %d == row count %d; indicating more data", batchSize, dataCount)
		}
		moreData = true
	}

	logger.Infof(tag+"%s seq value range %d - %d", ts.ColumnName, minSeq, maxSeq)
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

	if sequentialReplace {
		(*params)[ParamMethod] = "REPLACE"
	} else {
		(*params)[ParamMethod] = "INSERT"
	}

	return moreData, data, *newTs, nil
}
