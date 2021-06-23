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
	// ParamMethod is the parameter name which specifies the insert or
	// update method being used by portions of the migrator.
	ParamMethod = "METHOD"
	// ParamInsertBatchSize is the parameter used by the default loader
	// to batch queries. Int, defaults to 1000.
	ParamInsertBatchSize = "InsertBatchSize"
	// ParamDebug is the parameter used to enable basic debugging
	// code in modules. Boolean, defaults to false.
	ParamDebug = "Debug"
	// ParamLowLevelDebug is the parameter used to enable lower level
	// debugging code in modules. It is boolean and defaults to false.
	ParamLowLevelDebug = "LowLevelDebug"
	// ParamBatchSize is the parameter used to specify general batch
	// processing size for polling records from the database. Int,
	// defaults to 100.
	ParamBatchSize = "BatchSize"
	// ParamOnlyPast is the parameter for timestamp-based polling which
	// only polls for timestamps in the past. Boolean, defaults to
	// false.
	ParamOnlyPast = "OnlyPast"
	// ParamSequentialReplace is the parameter for loading which uses
	// REPLACE instead of INSERT for sequentially extracted data. Boolean,
	// defaults to false.
	ParamSequentialReplace = "SequentialReplace"
	// ParamTableName is the parameter for an adjusted table name.
	// String, defaults to "".
	ParamTableName = "TableName"
	// ParamSleepBetweenRuns is the parameter which defines the amount of
	// time between runs in seconds. Int, defaults to 5.
	ParamSleepBetweenRuns = "SleepBetweenRuns"
)

// SQLUntypedRow represents a single row of SQL data which is not strongly
// typed to a structure. This obviates the need to create Golang-level language
// structures to represent tables.
type SQLUntypedRow map[string]interface{}

// SQLRow represents a single row of SQL data with an action associated with it
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
