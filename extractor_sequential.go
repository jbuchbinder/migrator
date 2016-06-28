package migrator

import (
	"database/sql"
	"fmt"
	"log"
)

var ExtractorSequential = func(db *sql.DB, dbName, tableName string, ts TrackingStatus, params Parameters) (bool, []SqlUntypedRow, error) {
	tag := fmt.Sprintf("ExtractorSequential[%s.%s]: ", dbName, tableName)

	moreData := false

	log.Printf(tag+"Beginning run with params %#v", params)

	data := make([]SqlUntypedRow, 0)
	var maxSeq int64

	batchSize := paramInt(params, "BatchSize", DefaultBatchSize)

	rows, err := db.Query("SELECT * FROM `"+tableName+"` WHERE `"+ts.ColumnName+"` > ? LIMIT ?", ts.SequentialPosition, batchSize)
	if err != nil {
		return false, data, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return false, data, err
	}
	log.Printf(tag+"Columns %v", cols)
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
			return false, data, err
		}

		// De-reference fields
		rowData := make(SqlUntypedRow, len(cols))
		for i := range cols {
			rowData[cols[i]] = values[i]
		}
		data = append(data, rowData)
		maxSeq = int64max(maxSeq, rowData[ts.ColumnName].(int64))
	}

	if dataCount < batchSize {
		log.Printf(tag+"Batch size %d, row count %d; indicating no more data", batchSize, dataCount)
		moreData = false
	} else {
		log.Printf(tag+"Batch size %d == row count %d; indicating more data", batchSize, dataCount)
		moreData = true
	}

	log.Printf(tag+"%s high seq value %d", ts.ColumnName, maxSeq)
	err = SetTrackingStatusSequential(ts.Db, dbName, tableName, maxSeq)

	return moreData, data, err
}
