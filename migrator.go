package migrator

import (
	"database/sql"
	"errors"
	"log"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Migrator represents an object which encompasses an entire end-to-end
// ETL process.
type Migrator struct {
	// SourceDsn represents the DSN (data source name) for the source
	// table. Format is:
	// https://github.com/go-sql-driver/mysql#dsn-data-source-name
	SourceDsn *mysql.Config

	// SourceTable defines the table name where data will be pulled
	// for the Extractor.
	SourceTable string

	// SourceKey is the key field which is used to determine position.
	// This is only specified for the creation of the tracking
	// table if necessary.
	SourceKey string

	// DestinationDsn represents the DSN (data source name) for the
	// destination table. Format is:
	// https://github.com/go-sql-driver/mysql#dsn-data-source-name
	DestinationDsn *mysql.Config

	// DestinationTable defines the table name where data will be pushed
	// by the Loader.
	DestinationTable string

	// Parameters are a map of arbitrary values / structures which are
	// passed to all of the constituent functions ( Extractor, Transformer,
	// Loader ) in the Migrator.
	Parameters *Parameters

	// Extractor represents the Extractor callback.
	Extractor Extractor

	// Transformer represents the Transformer callback. This should be,
	// at a minimum, set to DefaultTransformer if there is no conversion
	// set to take place.
	Transformer Transformer

	// Loader represents the Loader callback.
	Loader Loader

	// Internal fields

	sourceDb      *sql.DB
	destinationDb *sql.DB
	terminated    bool
	initialized   bool
}

// Init initializes the underlying MySQL database connections for the
// Migrator instance.
func (m *Migrator) Init() error {
	tag := "Migrator.Init(): "

	var err error
	log.Printf(tag + "Initializing migrator")

	if m.SourceDsn == nil || m.DestinationDsn == nil {
		return errors.New(tag + "No source or destination DSN set")
	}

	if m.SourceTable == "" || m.DestinationTable == "" {
		return errors.New(tag + "No source or destination table set")
	}

	if m.initialized {
		return errors.New(tag + "Already initialized")
	}

	// Adjust with forced params
	m.SourceDsn.ParseTime = true
	m.DestinationDsn.ParseTime = true

	log.Printf(tag+"Using source dsn: %s", m.SourceDsn.FormatDSN())
	m.sourceDb, err = sql.Open("mysql", m.SourceDsn.FormatDSN())
	if err != nil {
		return err
	}
	m.sourceDb.SetMaxIdleConns(0)
	m.sourceDb.SetMaxOpenConns(50)

	log.Printf(tag+"Using destination dsn: %s", m.DestinationDsn.FormatDSN())
	m.destinationDb, err = sql.Open("mysql", m.DestinationDsn.FormatDSN())
	if err != nil {
		return err
	}
	m.destinationDb.SetMaxIdleConns(0)
	m.destinationDb.SetMaxOpenConns(50)

	// Attempt to make sure there is a tracking table and status entry

	log.Printf(tag + "Ensuring that tracking table exists")
	err = CreateTrackingTable(m.destinationDb)
	if err != nil {
		return err
	}

	log.Printf(tag+"Getting tracking table status for %s.%s", m.SourceDsn.DBName, m.SourceTable)
	_, err = GetTrackingStatus(m.destinationDb, m.SourceDsn.DBName, m.SourceTable)
	if err != nil {
		tt := TrackingStatus{
			Db:                 m.destinationDb,
			SourceDatabase:     m.SourceDsn.DBName,
			SourceTable:        m.SourceTable,
			ColumnName:         m.SourceKey,
			SequentialPosition: 0,
			TimestampPosition:  NullTime{},
			LastRun:            NullTimeNow(),
		}
		log.Printf(tag+"Creating tracking table entry, as none exists: %#v", tt)
		err := SerializeNewTrackingStatus(tt)
		if err != nil {
			log.Printf(tag+"TrackingStatus: %#v", tt)
			return err
		}
	}

	m.initialized = true

	return nil
}

// Run spins off a goroutine with a running migrator until the corresponding
// Quit() method is called.
func (m *Migrator) Run() error {
	tag := "Migrator.Run(): "

	if !m.initialized {
		return errors.New(tag + "Not initialized")
	}

	delay := paramInt(*m.Parameters, "SleepBetweenRuns", 5)

	go func() {
		// Actual run
		ts, err := GetTrackingStatus(m.destinationDb, m.SourceDsn.DBName, m.SourceTable)
		if err != nil {
			log.Printf(tag + "GetTrackingStatus: " + err.Error())
			return
		}
		log.Printf(tag + "Entering loop")
		for {
			if m.terminated {
				log.Printf(tag + "Received quit signal")
				m.Close()
				return
			}
			log.Printf(tag+"TrackingStatus: %s", ts.String())

			more, rows, newTs, err := m.Extractor(m.sourceDb, m.SourceDsn.DBName, m.SourceTable, ts, m.Parameters)
			if err != nil {
				log.Printf(tag + "Extractor: " + err.Error())
			}
			log.Printf(tag+"Extracted %d rows", len(rows))
			err = m.Loader(m.destinationDb, m.Transformer(m.DestinationDsn.DBName, m.DestinationTable, rows, m.Parameters), m.Parameters)
			if err != nil {
				log.Printf(tag + "Loader: " + err.Error())

			}

			log.Printf(tag + "Tracking: Updating table")
			err = SerializeTrackingStatus(m.destinationDb, newTs)
			if err != nil {
				log.Printf(tag + "Tracking: " + err.Error())
			}

			ts = newTs

			if !more {
				log.Printf(tag+"No more rows detected to process, sleeping for %d sec", delay)
				time.Sleep(time.Second * time.Duration(delay))

				ts, err = GetTrackingStatus(m.destinationDb, m.SourceDsn.DBName, m.SourceTable)
				if err != nil {
					log.Printf(tag + "GetTrackingStatus: " + err.Error())
					return
				}
			}

			// Sleep for 150ms to avoid pileups
			time.Sleep(time.Millisecond * 150)
		}
	}()

	return nil
}

// Close forcibly closes the database connections for the Migrator instance
// and marks it as being uninitialized.
func (m *Migrator) Close() {
	tag := "Migrator.Close(): "

	log.Printf(tag + "Closing connections")
	if m.sourceDb != nil {
		log.Printf(tag + "Closing source db connection")
		m.sourceDb.Close()
	}
	if m.destinationDb != nil {
		log.Printf(tag + "Closing destination db connection")
		m.destinationDb.Close()
	}

	m.initialized = false
}

// Quit is the method which should be used as the "preferred method" for
// terminating a Migrator instance.
func (m *Migrator) Quit() error {
	tag := "Migrator.Quit(): "

	if !m.initialized {
		return errors.New(tag + "Not initialized")
	}

	log.Printf(tag + "Sending quit signal")

	m.terminated = true

	return nil
}
