package migrator

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"
)

// TrackingStatus is the table definition for the tracking table which
// maintains the ETL positioning
type TrackingStatus struct {
	Db                 *sql.DB
	SourceDatabase     string   `db:"sourceDatabase"`
	SourceTable        string   `db:"sourceTable"`
	ColumnName         string   `db:"columnName"`
	SequentialPosition int64    `db:"sequentialPosition"`
	TimestampPosition  NullTime `db:"timestampPosition"`
	LastRun            NullTime `db:"lastRun"`
}

// String produces a human readable representation of a TrackingStatus object.
func (t TrackingStatus) String() string {
	out := "TrackingStatus[" + t.SourceDatabase + "." + t.SourceTable + "]: "
	if t.TimestampPosition.Valid {
		return out + t.TimestampPosition.Time.String()
	}
	return out + fmt.Sprintf("%d", t.SequentialPosition)
}

// SerializeNewTrackingStatus serializes a TrackingStatus object to its
// database table.
func SerializeNewTrackingStatus(tt TrackingStatus) error {
	if tt.SourceDatabase == "" || tt.SourceTable == "" || tt.ColumnName == "" {
		return errors.New("SerializeNewTrackingStatus(): Unable to write incomplete record to database")
	}
	_, err := tt.Db.Exec("INSERT INTO `"+TrackingTableName+"` ( sourceDatabase, sourceTable, columnName, sequentialPosition, timestampPosition, lastRun ) VALUES ( ?, ?, ?, ?, ?, ? )", tt.SourceDatabase, tt.SourceTable, tt.ColumnName, tt.SequentialPosition, tt.TimestampPosition, tt.LastRun)
	return err
}

// GetTrackingStatus retrieves a TrackingStatus object from its underlying
// database table.
func GetTrackingStatus(db *sql.DB, sourceDatabase, sourceTable string) (TrackingStatus, error) {
	var out TrackingStatus
	err := db.QueryRow("SELECT * FROM `"+TrackingTableName+"` WHERE sourceDatabase = ? AND sourceTable = ? LIMIT 1", sourceDatabase, sourceTable).Scan(&out.SourceDatabase, &out.SourceTable, &out.ColumnName, &out.SequentialPosition, &out.TimestampPosition, &out.LastRun)
	out.Db = db
	return out, err
}

// GetTrackingStatusSequential retrieves the sequentialPosition for a
// TrackingStatus from its underlying database table.
func GetTrackingStatusSequential(db *sql.DB, sourceDatabase, sourceTable string) (int64, error) {
	var seq int64
	err := db.QueryRow("SELECT sequentialPosition FROM `"+TrackingTableName+"` WHERE sourceDatabase = ? AND sourceTable = ? LIMIT 1", sourceDatabase, sourceTable).Scan(&seq)
	if err == nil && seq == 0 {
		return 0, errors.New("GetTrackingSequenceSequential(): unable to get sequential sequence")
	}
	return seq, err
}

// GetTrackingStatusTimestamp retrieves the timestampPosition for a
// TrackingStatus from its underlying database table.
func GetTrackingStatusTimestamp(db *sql.DB, sourceDatabase, sourceTable string) (NullTime, error) {
	var seq NullTime
	err := db.QueryRow("SELECT timestampPosition FROM `"+TrackingTableName+"` WHERE sourceDatabase = ? AND sourceTable = ? LIMIT 1", sourceDatabase, sourceTable).Scan(&seq)
	if err == nil && !seq.Valid {
		return seq, errors.New("GetTrackingSequenceTimestamp(): unable to get timestamp sequence")
	}
	return seq, err

}

// SerializeTrackingStatus serializes a copy of an actively modified
// TrackingStatus to its underlying database table.
func SerializeTrackingStatus(db *sql.DB, ts TrackingStatus) error {
	log.Printf("SerializeTrackingStatus(): %s", ts)
	_, err := db.Exec("UPDATE `"+TrackingTableName+"` SET sequentialPosition = ?, timestampPosition = ?, lastRun = ? WHERE sourceDatabase = ? AND sourceTable = ?", ts.SequentialPosition, ts.TimestampPosition, ts.LastRun, ts.SourceDatabase, ts.SourceTable)
	return err
}

// SetTrackingStatusSequential updates a TrackingStatus object's
// sequentialPosition in its underlying database table.
func SetTrackingStatusSequential(db *sql.DB, sourceDatabase, sourceTable string, seq int64) error {
	_, err := db.Exec("UPDATE `"+TrackingTableName+"` SET sequentialPosition = ?, lastRun = ? WHERE sourceDatabase = ? AND sourceTable = ?", seq, time.Now(), sourceDatabase, sourceTable)
	return err
}

// SetTrackingStatusTimestamp updates a TrackingStatus object's
// timestampPosition in its underlying database table.
func SetTrackingStatusTimestamp(db *sql.DB, sourceDatabase, sourceTable string, stamp time.Time) error {
	_, err := db.Exec("UPDATE `"+TrackingTableName+"` SET timestampPosition = ?, lastRun = ? WHERE sourceDatabase = ? AND sourceTable = ?", stamp, time.Now(), sourceDatabase, sourceTable)
	return err
}
