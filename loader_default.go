package migrator

import (
	"database/sql"
	"log"
)

// DefaultLoader
var DefaultLoader = func(db *sql.DB, dbName, tableName string, data []SqlUntypedRow, params Parameters) error {
	tag := "DefaultLoader(" + dbName + "." + tableName + "): "

	size := paramInt(params, "InsertBatchSize", 100)

	log.Printf(tag+"Beginning transaction, InsertBatchSize == %d", size)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = BatchedInsert(tx, tableName, data, size)
	if err != nil {
		err = tx.Rollback()
		if err != nil {
			log.Printf(tag + "Error during rollback: " + err.Error())
			return err
		}
	}
	log.Printf(tag + "Committing transaction")
	return tx.Commit()
}
