package migrator

import (
	"database/sql"
	"log"
	"time"
)

// DefaultLoader represents a default Loader instance.
var DefaultLoader = func(db *sql.DB, tables []TableData, params *Parameters) error {
	var err error

	size := paramInt(*params, "InsertBatchSize", 100)
	//debug := paramBool(*params, "Debug", false)

	for _, table := range tables {
		tag := "DefaultLoader(" + table.DbName + "." + table.TableName + "): "
		tsStart := time.Now()

		log.Printf(tag+"Beginning transaction, InsertBatchSize == %d", size)
		tx, err := db.Begin()
		if err != nil {
			log.Printf(tag + "Transaction start: " + err.Error())
			return err
		}
		if method, ok := (*params)["METHOD"].(string); ok {
			switch method {
			case "REPLACE":
				log.Printf(tag + "Method REPLACE")
				err = BatchedReplace(tx, table.TableName, table.Data, size)

			case "INSERT":
				log.Printf(tag + "Method INSERT")
				err = BatchedInsert(tx, table.TableName, table.Data, size)
				break
			default:
				log.Printf(tag+"Unknown method '%s' present, falling back on INSERT", method)
				err = BatchedInsert(tx, table.TableName, table.Data, size)
				break
			}
		} else {
			// Fall back to INSERT
			log.Printf(tag + "No method present, falling back on INSERT")
			err = BatchedInsert(tx, table.TableName, table.Data, size)
		}
		if err != nil {
			log.Printf(tag + "Rolling back transaction")
			err2 := tx.Rollback()
			if err2 != nil {
				log.Printf(tag + "Error during rollback: " + err2.Error())
			}
			return err
		}

		log.Printf(tag+"Duration to insert %d rows: %s", len(table.Data), time.Since(tsStart).String())

		log.Printf(tag + "Committing transaction")
		err = tx.Commit()
		if err != nil {
			log.Printf(tag + "Error during commit: " + err.Error())
		}
	}

	return err
}
