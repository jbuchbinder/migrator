package migrator

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

func init() {
	ExtractorMap["queue"] = ExtractorQueue
}

// ExtractorQueue is an Extractor instance which uses a table which is
// triggered by INSERT or UPDATE to notify the extractor that it needs
// to replicate a row.
var ExtractorQueue = func(db *sql.DB, dbName, tableName string, ts TrackingStatus, params *Parameters) (bool, []SQLRow, TrackingStatus, error) {
	batchSize := paramInt(*params, "BatchSize", DefaultBatchSize)
	debug := paramBool(*params, "Debug", false)

	tag := fmt.Sprintf("ExtractorQueue[%s.%s]: ", dbName, tableName)

	moreData := false

	if debug {
		log.Printf(tag+"Beginning run with params %#v", params)
	}

	data := make([]SQLRow, 0)
	//minSeq := int64(math.MaxInt64)
	//var maxSeq int64

	tsStart := time.Now()

	rowsToProcess, err := db.Query("SELECT * FROM `"+RecordQueueTable+"` WHERE sourceDatabase = ? AND sourceTable = ? ORDER BY timestampUpdated LIMIT ?",
		dbName, tableName, DefaultBatchSize)
	if err != nil {
		log.Printf(tag+"Error extracting queue rows: %s", err.Error())
		return false, data, ts, err
	}
	dataCount := 0
	for rowsToProcess.Next() {
		rq := RecordQueue{Db: db}
		err := rowsToProcess.Scan(
			&(rq.SourceDatabase),
			&(rq.SourceTable),
			&(rq.PrimaryKeyColumnName),
			&(rq.PrimaryKeyColumnValue),
			&(rq.TimestampUpdated),
			&(rq.Method),
		)
		if err != nil {
			log.Printf(tag + "Queue Scan: " + err.Error())
			return false, data, ts, err
		}

		// Handle REMOVE -- since we can't actually scan a removed item
		if rq.Method == "REMOVE" {
			if debug {
				log.Printf(tag+"Found REMOVE -- processing : %#v", rq)
			}
			rowData := SQLRow{}
			rowData.Method = "REMOVE"
			rowData.Data = SQLUntypedRow{}
			rowData.Data[rq.PrimaryKeyColumnName] = rq.PrimaryKeyColumnValue
			data = append(data, rowData)
			err = rq.Remove()
			if err != nil {
				return false, data, ts, err
			}
			continue
		}

		var rows *sql.Rows
		if strings.Index(rq.PrimaryKeyColumnName, ",") != -1 {
			// Support for multiple indices and values separated by commas
			qs := "SELECT * FROM `" + tableName + "` WHERE "
			for iter, x := range strings.Split(rq.PrimaryKeyColumnName, ",") {
				if iter != 0 {
					qs += " AND "
				}
				qs += "`" + x + "` = ? "
			}
			qs += " LIMIT 1"
			qvRaw := strings.Split(rq.PrimaryKeyColumnValue, ",")
			qv := []interface{}{}
			for _, v := range qvRaw {
				qv = append(qv, v)
			}
			rows, err = db.Query(qs, qv...)
		} else {
			rows, err = db.Query("SELECT * FROM `"+tableName+"` WHERE `"+rq.PrimaryKeyColumnName+"` = ? LIMIT 1", rq.PrimaryKeyColumnValue)
		}
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
			rowData := SQLRow{}
			rowData.Method = "REPLACE"
			rowData.Data = make(SQLUntypedRow, len(cols))
			for i := range cols {
				rowData.Data[cols[i]] = values[i]
			}
			data = append(data, rowData)
			//minSeq = int64min(minSeq, rowData.Data[ts.ColumnName].(int64))
			//maxSeq = int64max(maxSeq, rowData.Data[ts.ColumnName].(int64))
		}
		err = rq.Remove()
		if err != nil {
			log.Printf(tag+"Error removing record queue entry: %s", err.Error())
		}
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

	//log.Printf(tag+"%s seq value range %d - %d", ts.ColumnName, minSeq, maxSeq)

	// Manually copy old tracking object ...
	newTs := &TrackingStatus{
		Db:             ts.Db,
		SourceDatabase: ts.SourceDatabase,
		SourceTable:    ts.SourceTable,
		ColumnName:     ts.ColumnName,
		// ... with updates
		SequentialPosition: ts.SequentialPosition,
		LastRun:            NullTimeNow(),
	}

	(*params)["METHOD"] = "REPLACE"
	return moreData, data, *newTs, nil
}
