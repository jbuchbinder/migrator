package migrator

import (
	"database/sql"
	"log"
)

// DefaultLoader
var DefaultLoader = func(db *sql.DB, tables []TableData, params Parameters) error {
	var err error

	size := paramInt(params, "InsertBatchSize", 100)
	//debug := paramBool(params, "Debug", false)

	for _, table := range tables {
		tag := "DefaultLoader(" + table.DbName + "." + table.TableName + "): "

		log.Printf(tag+"Beginning transaction, InsertBatchSize == %d", size)
		tx, err := db.Begin()
		if err != nil {
			log.Printf(tag + "Transaction start: " + err.Error())
			return err
		}
		err = BatchedInsert(tx, table.TableName, table.Data, size)
		if err != nil {
			err = tx.Rollback()
			if err != nil {
				log.Printf(tag + "Error during rollback: " + err.Error())
				return err
			}
		}

		log.Printf(tag + "Committing transaction")
		err = tx.Commit()
		if err != nil {
			log.Printf(tag + "Error during commit: " + err.Error())
		}
	}

	return err
}
