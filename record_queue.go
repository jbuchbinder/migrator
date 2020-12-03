package migrator

import (
	"database/sql"
	"fmt"
	"time"
)

// RecordQueue is the table definition for the tracking table which
// is used for timestamp updated tables which do not have a lastUpdated
// or equivalent field.
type RecordQueue struct {
	Db                    *sql.DB
	SourceDatabase        string    `db:"sourceDatabase"`
	SourceTable           string    `db:"sourceTable"`
	PrimaryKeyColumnName  string    `db:"pkColumn"`
	PrimaryKeyColumnValue string    `db:"pkValue"`
	SequentialPosition    int64     `db:"sequentialPosition"`
	TimestampUpdated      time.Time `db:"timestampPosition"`
	Method                string    `db:"method"`
}

// String produces a human readable representation of a TrackingStatus object.
func (t RecordQueue) String() string {
	out := "RecordQueue[" + t.SourceDatabase + "." + t.SourceTable + "]: "
	return out + fmt.Sprintf("%s = %s", t.PrimaryKeyColumnName, t.PrimaryKeyColumnValue)
}

// Remove removes an entry from the record queue
func (t RecordQueue) Remove() error {
	_, err := t.Db.Exec("DELETE FROM `"+RecordQueueTable+"` WHERE sourceDatabase = ? AND sourceTable = ? AND pkColumn = ? AND pkValue = ?",
		t.SourceDatabase, t.SourceTable, t.PrimaryKeyColumnName, t.PrimaryKeyColumnValue)
	return err
}

// RemoveRecordQueueItem removes an item from the record queue
func RemoveRecordQueueItem(db *sql.DB, sourceDatabase, sourceTable, pkColumn, pkValue string) error {
	_, err := db.Exec("DELETE FROM `"+RecordQueueTable+"` WHERE sourceDatabase = ? AND sourceTable = ? AND pkColumn = ? AND pkValue = ?",
		sourceDatabase, sourceTable, pkColumn, pkValue)
	return err
}
