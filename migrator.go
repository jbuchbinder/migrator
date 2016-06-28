package migrator

import (
	"database/sql"
	"errors"
	"github.com/go-sql-driver/mysql"
	"log"
	"time"
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
	Parameters Parameters

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
	quitChan      chan bool
	initialized   bool
}

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

	m.initialized = true

	return nil
}

func (m Migrator) Run() error {
	tag := "Migrator.Run(): "

	if !m.initialized {
		return errors.New(tag + "Not initialized")
	}

	go func() {
		log.Printf(tag + "Entering loop")
		for {
			select {
			case <-m.quitChan:
				log.Printf(tag + "Received quit signal")
				m.Close()
				return
			default:
				// Actual run
				ts, err := GetTrackingStatus(m.destinationDb, m.SourceDsn.DBName, m.SourceTable)
				if err != nil {
					log.Printf(tag + "GetTrackingStatus: " + err.Error())
					continue
				}

				more, rows, err := m.Extractor(m.sourceDb, m.SourceDsn.DBName, m.SourceTable, ts, m.Parameters)
				if err != nil {
					log.Printf(tag + "Extractor: " + err.Error())
				}
				log.Printf(tag+"Extracted %d rows", len(rows))
				err = m.Loader(m.destinationDb, m.DestinationDsn.DBName, m.DestinationTable, m.Transformer(rows, m.Parameters), m.Parameters)
				if err != nil {
					log.Printf(tag + "Loader: " + err.Error())
				}
				if !more {
					log.Printf(tag + "No more rows detected to process, sleeping for 5 sec")
					time.Sleep(time.Second * 5)
				}
			}

			// Sleep for 150ms to avoid pileups
			time.Sleep(time.Millisecond * 150)
		}
	}()

	return nil
}

func (m Migrator) Close() {
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

func (m Migrator) Quit() error {
	tag := "Migrator.Quit(): "

	if !m.initialized {
		return errors.New(tag + "Not initialized")
	}

	log.Printf(tag + "Sending quit signal")

	m.quitChan <- true

	return nil
}
