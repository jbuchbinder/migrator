package migrator

import (
	"database/sql"
	"fmt"
	"time"
)

func init() {
	ExtractorMap["timestamp"] = ExtractorTimestamp
}

// ExtractorTimestamp is an Extractor instance which uses a DATETIME/TIMESTAMP
// field to determine which rows to pull from the source database table.
var ExtractorTimestamp = func(db *sql.DB, dbName, tableName string, ts TrackingStatus, params *Parameters) (bool, []SQLRow, TrackingStatus, error) {
	batchSize := paramInt(*params, ParamBatchSize, DefaultBatchSize)
	debug := paramBool(*params, ParamDebug, false)
	onlyPast := paramBool(*params, ParamOnlyPast, false)

	tag := fmt.Sprintf("ExtractorTimestamp[%s.%s]: ", dbName, tableName)

	moreData := false

	if debug {
		logger.Debugf(tag+"Beginning run with params %#v", params)
	}

	data := make([]SQLRow, 0)
	var maxStamp time.Time

	tsStart := time.Now()

	if debug {
		if onlyPast {
			logger.Debugf(tag+"Query: \"SELECT * FROM `"+tableName+"` WHERE `"+ts.ColumnName+"` > %v AND `"+ts.ColumnName+"` <= NOW() LIMIT %d\"", ts.TimestampPosition, batchSize)
		} else {
			logger.Debugf(tag+"Query: \"SELECT * FROM `"+tableName+"` WHERE `"+ts.ColumnName+"` > %v LIMIT %d\"", ts.TimestampPosition, batchSize)
		}
	}
	var rows *sql.Rows
	var err error
	if onlyPast {
		rows, err = db.Query("SELECT * FROM `"+tableName+"` WHERE `"+ts.ColumnName+"` > ? AND `"+ts.ColumnName+"` <= NOW() LIMIT ?", ts.TimestampPosition, batchSize)
	} else {
		rows, err = db.Query("SELECT * FROM `"+tableName+"` WHERE `"+ts.ColumnName+"` > ? LIMIT ?", ts.TimestampPosition, batchSize)
	}
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
		rowData.Data = make(SQLUntypedRow, len(cols))
		for i := range cols {
			rowData.Data[cols[i]] = values[i]
		}
		data = append(data, rowData)

		timestamp, ok := rowData.Data[ts.ColumnName].(time.Time)
		if !ok {
			logger.Errorf(tag+"ERROR: Unable to process table %s due to column %s not being a Time", dbName+"."+tableName, ts.ColumnName)
			return false, data, ts, err
		}
		maxStamp = timemax(maxStamp, timestamp)
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

	if debug {
		logger.Debugf(tag+"%s high timestamp value %#v", ts.ColumnName, maxStamp)
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

	(*params)[ParamMethod] = "REPLACE"

	return moreData, data, *newTs, err
}
