package migrator

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"reflect"
)

// BatchedInsert takes an array of SQL data rows and creates a series of
// batched inserts to insert the data into an existing sql.Tx (transaction)
// object.
func BatchedInsert(tx *sql.Tx, table string, data []SQLUntypedRow, size int, params *Parameters) error {
	return BatchedQuery(tx, table, data, size, "INSERT", params)
}

// BatchedReplace takes an array of SQL data rows and creates a series of
// batched replaces to replace the data into an existing sql.Tx (transaction)
// object.
func BatchedReplace(tx *sql.Tx, table string, data []SQLUntypedRow, size int, params *Parameters) error {
	return BatchedQuery(tx, table, data, size, "REPLACE", params)
}

// BatchedRemove takes an array of SQL data rows and creates a series of
// DELETE FROM statements to remove the data in an existing sql.Tx (transaction)
// object.
func BatchedRemove(tx *sql.Tx, table string, data []SQLUntypedRow, size int, params *Parameters) error {
	debug := paramBool(*params, "Debug", false)

	// Pull column names from first row
	if len(data) < 1 {
		return fmt.Errorf("BatchedRemove(): [%s] no data presented", table)
	}
	keys := reflect.ValueOf(data[0]).MapKeys()
	if len(keys) < 1 {
		return fmt.Errorf("BatchedRemove(): [%s] no columns presented", table)
	}

	if size < 1 {
		size = 1
	}

	for i := 0; i < len(data); i++ {
		params := make([]interface{}, 0)

		// Header is always the same
		prepared := new(bytes.Buffer)
		prepared.WriteString("DELETE FROM")
		prepared.WriteString(" `" + table + "` WHERE ")

		for iter, k := range keys {
			if iter != 0 {
				prepared.WriteString(" AND ")
			}
			prepared.WriteString("`" + k.String() + "` = ?")
			params = append(params, data[i][keys[iter].String()])
		}
		prepared.WriteString(";")

		if debug {
			logger.Debugf("BatchedRemove(): [%s] Prepared remove: %s", table, prepared.String())
		}

		// Attempt to execute
		_, err := tx.Exec(prepared.String(), params...)
		if err != nil {
			logger.Errorf("BatchedRemove(): [%s] ERROR: %s", table, err.Error())
			return err
		}
	}

	return nil
}

// BatchedQuery takes an array of SQL data rows and creates a series of
// batched queries to insert/replace the data into an existing sql.Tx
// (transaction) object.
func BatchedQuery(tx *sql.Tx, table string, data []SQLUntypedRow, size int, op string, params *Parameters) error {
	debug := paramBool(*params, "Debug", false)
	lowLevelDebug := paramBool(*params, "LowLevelDebug", false)

	// Pull column names from first row
	if len(data) < 1 {
		return fmt.Errorf("BatchedQuery(): [%s] no data presented", table)
	}
	keys := reflect.ValueOf(data[0]).MapKeys()
	if len(keys) < 1 {
		return fmt.Errorf("BatchedQuery(): [%s] no columns presented", table)
	}

	if size < 1 {
		size = 1
	}
	batches := int(math.Ceil(float64(len(data)) / float64(size)))

	for i := 0; i < batches; i++ {
		params := make([]interface{}, 0)

		// Header is always the same
		prepared := new(bytes.Buffer)
		switch op {
		case "INSERT":
			prepared.WriteString("INSERT INTO")
		case "REPLACE":
			prepared.WriteString("REPLACE INTO")
		default:
		}
		prepared.WriteString(" `" + table + "` ( ")
		for iter, k := range keys {
			if iter != 0 {
				prepared.WriteString(", ")
			}
			prepared.WriteString("`" + k.String() + "`")
		}
		prepared.WriteString(" ) VALUES")

		// Create value clauses
		for j := i * size; j < intmin((i+1)*size, len(data)); j++ {
			if j > i*size {
				prepared.WriteString(",")
			}
			prepared.WriteString(" ( ")
			for l := 0; l < len(keys); l++ {
				if l > 0 {
					prepared.WriteString(",")
				}
				prepared.WriteString("?")
				params = append(params, data[j][keys[l].String()])
			}
			prepared.WriteString(" ) ")
		}
		prepared.WriteString(";")

		if debug {
			logger.Debugf("BatchedQuery(): [%s] Prepared %s: %s", table, op, prepared.String())
		}

		// Attempt to execute
		res, err := tx.Exec(prepared.String(), params...)
		if lowLevelDebug {
			logger.Tracef("BatchedQuery(): [%s] %s [%#v]", table, prepared.String(), params)
		}
		if debug {
			lastInsertID, _ := res.LastInsertId()
			rowsAffected, _ := res.RowsAffected()
			logger.Debugf("BatchedQuery(): [%s] last id inserted = %d, rows affected = %d", table, lastInsertID, rowsAffected)
		}
		if err != nil {
			logger.Errorf("BatchedQuery(): [%s] ERROR: %s", table, err.Error())
			return err
		}
	}

	return nil
}
