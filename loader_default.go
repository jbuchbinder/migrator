package migrator

import (
	"database/sql"
	"time"
)

// DefaultLoader represents a default Loader instance.
var DefaultLoader = func(db *sql.DB, tables []TableData, params *Parameters) error {
	var err error

	size := paramInt(*params, ParamInsertBatchSize, 100)
	//debug := paramBool(*params, ParamDebug, false)

	for _, table := range tables {
		tag := "DefaultLoader(" + table.DbName + "." + table.TableName + "): "
		tsStart := time.Now()

		// Batch into transaction methods
		rowsByMethod := make(map[string][]SQLUntypedRow, 0)
		for _, r := range table.Data {
			if _, ok := rowsByMethod[r.Method]; !ok {
				rowsByMethod[r.Method] = make([]SQLUntypedRow, 0)
			}
			rowsByMethod[r.Method] = append(rowsByMethod[r.Method], r.Data)
		}

		for method := range rowsByMethod {
			logger.Debugf(tag+"Beginning transaction, InsertBatchSize == %d", size)
			tx, err := db.Begin()
			if err != nil {
				logger.Errorf(tag + "Transaction start: " + err.Error())
				return err
			}
			switch method {
			case "REPLACE":
				logger.Debug(tag + "Method REPLACE")
				err = BatchedReplace(tx, table.TableName, rowsByMethod[method], size, params)

			case "INSERT":
				logger.Debug(tag + "Method INSERT")
				err = BatchedInsert(tx, table.TableName, rowsByMethod[method], size, params)

			case "REMOVE":
				logger.Debug(tag + "Method REMOVE")
				err = BatchedRemove(tx, table.TableName, rowsByMethod[method], size, params)

			default:
				logger.Debugf(tag+"Unknown method '%s' present, falling back on REPLACE", method)
				err = BatchedReplace(tx, table.TableName, rowsByMethod[method], size, params)
			}
			if err != nil {
				logger.Warnf(tag + "Rolling back transaction")
				err2 := tx.Rollback()
				if err2 != nil {
					logger.Errorf(tag + "Error during rollback: " + err2.Error())
				}
				return err
			}

			logger.Infof(tag+"Duration to insert %d rows: %s", len(table.Data), time.Since(tsStart).String())

			logger.Debugf(tag + "Committing transaction")
			err = tx.Commit()
			if err != nil {
				logger.Errorf(tag + "Error during commit: " + err.Error())
			}
		}
	}

	return err
}
