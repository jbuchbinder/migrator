package migrator

import (
	"database/sql"
)

var (
	DefaultBatchSize  = 1000
	TrackingTableName = "EtlPosition"
	ExtractorMap      = make(map[string]Extractor)
)

// SqlUntypedRow represents a single row of SQL data which is not strongly
// typed to a structure. This obviates the need to create Golang-level language
// structures to represent tables.
type SqlUntypedRow map[string]interface{}

type Parameters map[string]interface{}

// TableData represents identifying information and data for a table.
type TableData struct {
	DbName    string
	TableName string
	Data      []SqlUntypedRow
}

// Extractor is a callback function type
type Extractor func(*sql.DB, string, string, TrackingStatus, Parameters) (bool, []SqlUntypedRow, TrackingStatus, error)

// Transformer is a callback function type which transforms an array of untyped
// information into another array of untyped information. This is used for the
// "transform" step of the ETL process.
type Transformer func(string, string, []SqlUntypedRow, Parameters) []TableData

// Loader is a callback function type
type Loader func(*sql.DB, []TableData, Parameters) error
