package migrator

import (
	"database/sql"
)

var (
	// DefaultBatchSize represents the default size of extracted batches
	DefaultBatchSize = 1000
	// TrackingTableName represents the name of the database table used
	// to track TrackingStatus instances, and exists within the target
	// database.
	TrackingTableName = "EtlPosition"
	// TransformerMap is a map of Transformer functions which can be used
	// to instantiate a Transformer based only on a string.
	TransformerMap = make(map[string]Transformer)
	// ExtractorMap is a map of Extractor functions which can be used
	// to instantiate an Extractor based only on a string.
	ExtractorMap = make(map[string]Extractor)
	// RecordQueueTable is the table name for the non-update field
	// capable entries.
	RecordQueueTable = "MigratorRecordQueue"
)

// SqlUntypedRow represents a single row of SQL data which is not strongly
// typed to a structure. This obviates the need to create Golang-level language
// structures to represent tables.
type SQLUntypedRow map[string]interface{}

// SqlRow represents a single row of SQL data with an action associated with it
type SQLRow struct {
	Data   SQLUntypedRow
	Method string
}

// Parameters represents a series of untyped parameters which are passed to
// Extractors, Transformers, and Loaders. All stages of the ETL process
// receive the same parameters.
type Parameters map[string]interface{}

// TableData represents identifying information and data for a table.
type TableData struct {
	DbName    string
	TableName string
	Data      []SQLRow
	Method    string // only used with loader, specifies INSERT/REPLACE
}

// Extractor is a callback function type
type Extractor func(*sql.DB, string, string, TrackingStatus, *Parameters) (bool, []SQLRow, TrackingStatus, error)

// Transformer is a callback function type which transforms an array of untyped
// information into another array of untyped information. This is used for the
// "transform" step of the ETL process.
type Transformer func(string, string, []SQLRow, *Parameters) []TableData

// Loader is a callback function type
type Loader func(*sql.DB, []TableData, *Parameters) error
