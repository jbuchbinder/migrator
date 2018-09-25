package migrator

import (
	"bytes"
	"database/sql"
	"errors"
	"math"
	"reflect"
)

// BatchedInsert takes an array of SQL data rows and creates a series of
// batched inserts to insert the data into an existing sql.Tx (transaction)
// object.
func BatchedInsert(tx *sql.Tx, table string, data []SqlUntypedRow, size int) error {
	return BatchedQuery(tx, table, data, size, "INSERT")
}

// BatchedReplace takes an array of SQL data rows and creates a series of
// batched replaces to replace the data into an existing sql.Tx (transaction)
// object.
func BatchedReplace(tx *sql.Tx, table string, data []SqlUntypedRow, size int) error {
	return BatchedQuery(tx, table, data, size, "REPLACE")
}

// BatchedQuery takes an array of SQL data rows and creates a series of
// batched queries to insert/replace the data into an existing sql.Tx
// (transaction) object.
func BatchedQuery(tx *sql.Tx, table string, data []SqlUntypedRow, size int, op string) error {
	// Pull column names from first row
	if len(data) < 1 {
		return errors.New("BatchedQuery(): no data presented")
	}
	keys := reflect.ValueOf(data[0]).MapKeys()
	if len(keys) < 1 {
		return errors.New("BatchedQuery(): no columns presented")
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

		// Attempt to execute
		_, err := tx.Exec(prepared.String(), params...)
		if err != nil {
			return err
		}
	}

	return nil
}