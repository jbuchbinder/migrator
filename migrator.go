package migrator

import (
	"database/sql"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"go.elastic.co/apm/module/apmsql"
)

var (
	// logger represents the logger used by the migrator.
	logger *log.Logger
)

// SetLogger sets a logrus Logger object used by the migrator
func SetLogger(l *log.Logger) {
	logger = l
}

// Migrator represents an object which encompasses an entire end-to-end
// ETL process.
type Migrator struct {
	// SourceDsn represents the DSN (data source name) for the source
	// table. Format is:
	// https://github.com/go-sql-driver/mysql#dsn-data-source-name
	SourceDsn *mysql.Config

	// DestinationDsn represents the DSN (data source name) for the
	// destination table. Format is:
	// https://github.com/go-sql-driver/mysql#dsn-data-source-name
	DestinationDsn *mysql.Config

	// Iterations represents all of the actual migrations being performed.
	Iterations []Iteration

	// Apm determines whether APM support will be enabled or disabled
	Apm bool

	// Parameters are a map of arbitrary values / structures which are
	// passed to all of the constituent functions except for Transformer
	// ( Extractor, Loader ) in the Migrator.
	Parameters *Parameters

	// ErrorCallback represents a logging callback for errors
	ErrorCallback func(map[string]string, error)

	// Internal fields

	sourceDb      *sql.DB
	destinationDb *sql.DB
	terminated    bool
	initialized   bool
	wg            *sync.WaitGroup
}

// Iteration defines the individual sub-migrator configuration which replicates
// a single table
type Iteration struct {
	// DestinationTable defines the table name where data will be pushed
	// by the Loader.
	DestinationTable string

	// SourceTable defines the table name where data will be pulled
	// for the Extractor.
	SourceTable string

	// SourceKey is the key field which is used to determine position.
	// This is only specified for the creation of the tracking
	// table if necessary.
	SourceKey string

	// Parameters are a map of arbitrary values / structures which are
	// passed to all of the constituent functions except for Transformer
	// ( Extractor, Loader ) in the Migrator.
	Parameters *Parameters

	// Extractor represents the Extractor callback.
	Extractor Extractor

	// ExtractorName represents the name of the extractor used
	ExtractorName string

	// Transformer represents the Transformer callback. This should be,
	// at a minimum, set to DefaultTransformer if there is no conversion
	// set to take place.
	Transformer Transformer

	// TransformerParameters are a map of arbitrary parameters specific
	// to transformers.
	TransformerParameters *Parameters

	// LoaderName represents the name of the loader used
	LoaderName string

	// Loader represents the Loader callback.
	Loader Loader
}

// SetWaitGroup sets the wait group instance being used
func (m *Migrator) SetWaitGroup(wg *sync.WaitGroup) {
	m.wg = wg
}

// SetErrorCallback sets the error callback function
func (m *Migrator) SetErrorCallback(f func(map[string]string, error)) {
	m.ErrorCallback = f
}

// GetWaitGroup returns the wait group instance being used
func (m Migrator) GetWaitGroup() sync.WaitGroup {
	return *(m.wg)
}

// Init initializes the underlying MySQL database connections for the
// Migrator instance.
func (m *Migrator) Init() error {
	tag := "Migrator.Init(): [" + m.SourceDsn.FormatDSN() + "] "

	var err error
	logger.Infof(tag + "Initializing migrator")

	if m.SourceDsn == nil || m.DestinationDsn == nil {
		return errors.New(tag + "No source or destination DSN set")
	}

	if m.initialized {
		return errors.New(tag + "Already initialized")
	}

	// Adjust with forced params
	m.SourceDsn.ParseTime = true
	m.DestinationDsn.ParseTime = true

	logger.Infof(tag+"Using source dsn: %s", m.SourceDsn.FormatDSN())
	if m.Apm {
		logger.Infof(tag+"Reporting APM stats for %s", m.SourceDsn.FormatDSN())
		m.sourceDb, err = apmsql.Open("apmmysql", m.SourceDsn.FormatDSN())
	} else {
		m.sourceDb, err = sql.Open("mysql", m.SourceDsn.FormatDSN())
	}
	if err != nil {
		return err
	}
	m.sourceDb.SetMaxIdleConns(0)
	m.sourceDb.SetMaxOpenConns(len(m.Iterations) * 3)

	logger.Infof(tag+"Using destination dsn: %s", m.DestinationDsn.FormatDSN())
	if m.Apm {
		logger.Infof(tag+"Reporting APM stats for %s", m.DestinationDsn.FormatDSN())
		m.destinationDb, err = apmsql.Open("apmmysql", m.DestinationDsn.FormatDSN())
	} else {
		m.destinationDb, err = sql.Open("mysql", m.DestinationDsn.FormatDSN())
	}
	if err != nil {
		return err
	}
	m.destinationDb.SetMaxIdleConns(0)
	m.destinationDb.SetMaxOpenConns(len(m.Iterations) * 3)

	for x := range m.Iterations {

		// Avoid NPEs and just pass basic params if there are no TransformerParameters
		if m.Iterations[x].TransformerParameters == nil {
			m.Iterations[x].TransformerParameters = m.Iterations[x].Parameters
		}

		// Attempt to make sure there is a tracking table and status entry

		logger.Infof(tag + "Ensuring that tracking table exists")
		err = CreateTrackingTable(m.destinationDb)
		if err != nil {
			return err
		}

		logger.Infof(tag+"Getting tracking table status for %s.%s", m.SourceDsn.DBName, m.Iterations[x].SourceTable)
		_, err = GetTrackingStatus(m.destinationDb, m.SourceDsn.DBName, m.Iterations[x].SourceTable)
		if err != nil {
			tt := TrackingStatus{
				Db:                 m.destinationDb,
				SourceDatabase:     m.SourceDsn.DBName,
				SourceTable:        m.Iterations[x].SourceTable,
				ColumnName:         m.Iterations[x].SourceKey,
				SequentialPosition: 0,
				TimestampPosition:  NullTime{},
				LastRun:            NullTimeNow(),
			}
			logger.Infof(tag+"Creating tracking table entry, as none exists: %#v", tt)
			err := SerializeNewTrackingStatus(tt)
			if err != nil {
				logger.Infof(tag+"TrackingStatus: %#v", tt)
				return err
			}
		}
	}

	m.initialized = true

	return nil
}

// sleepWithInterrupt allows a sleep cycle that checks for termination every second
func (m *Migrator) sleepWithInterrupt(length int) {
	for i := 0; i <= length; i++ {
		time.Sleep(time.Second)
		if m.terminated {
			return
		}
	}
}

// Run spins off a goroutine with a running migrator until the corresponding
// Quit() method is called.
func (m *Migrator) Run() error {
	debug := paramBool(*(m.Parameters), "Debug", false)
	if debug {
		//logger.Level = log.TraceLevel
	}

	tag := "Migrator.Run(): [" + m.SourceDsn.DBName + "] "

	logger.Debugf(tag + "Entry")

	if !m.initialized {
		return errors.New(tag + "Not initialized")
	}

	for x := range m.Iterations {
		delay := paramInt(*m.Iterations[x].Parameters, "SleepBetweenRuns", 5)

		m.wg.Add(1)
		go func(x int) {
			// Actual run
			var ts TrackingStatus
			var err error
			var attempt int
			for {
				ts, err = GetTrackingStatus(m.destinationDb, m.SourceDsn.DBName, m.Iterations[x].SourceTable)
				if err != nil {
					logger.Warnf(tag+"GetTrackingStatus[Attempt %d, terminated=%#v]: %s", attempt, m.terminated, err.Error())
					attempt++
					m.sleepWithInterrupt(delay)
					if m.terminated {
						logger.Infof(tag + "Received quit signal")
						m.Close()
						m.wg.Done()
						return
					}
					continue
				}
				break
			}
			logger.Debugf(tag + "Entering loop")
			for {
				if m.terminated {
					logger.Infof(tag + "Received quit signal")
					m.Close()
					m.wg.Done()
					return
				}
				logger.Debugf(tag+"TrackingStatus: %s", ts.String())

				more, rows, newTs, err := m.Iterations[x].Extractor(m.sourceDb, m.SourceDsn.DBName, m.Iterations[x].SourceTable, ts, m.Iterations[x].Parameters)
				if err != nil {
					logger.Infof(tag + "Extractor: " + err.Error())
					if m.ErrorCallback != nil {
						m.ErrorCallback(map[string]string{
							"Stage":       "Extractor",
							"SourceDb":    m.SourceDsn.DBName,
							"SourceTable": m.Iterations[x].SourceTable,
						}, err)
					}
				}
				logger.Infof(tag+"[%s.%s] Extracted %d rows", m.SourceDsn.DBName, m.Iterations[x].SourceTable, len(rows))

				logger.Debugf(tag+"Running transformer for %s.%s", m.SourceDsn.DBName, m.Iterations[x].SourceTable)
				logger.Debugf(tag+"Transformer %#v (%s,%s,%#v,%#v)", m.Iterations[x].Transformer, m.DestinationDsn.DBName, m.Iterations[x].DestinationTable, rows, m.Iterations[x].TransformerParameters)
				data := m.Iterations[x].Transformer(m.DestinationDsn.DBName, m.Iterations[x].DestinationTable, rows, m.Iterations[x].TransformerParameters)
				logger.Tracef(tag+"Transformer put out %#v for data", data)
				logger.Debugf(tag+"Running loader for %s.%s", m.SourceDsn.DBName, m.Iterations[x].SourceTable)
				err = m.Iterations[x].Loader(m.destinationDb, data, m.Iterations[x].Parameters)
				if err != nil {
					logger.Errorf(tag + "Loader: " + err.Error())
					if m.ErrorCallback != nil {
						m.ErrorCallback(map[string]string{
							"Stage":            "Loader",
							"SourceDb":         m.SourceDsn.DBName,
							"SourceTable":      m.Iterations[x].SourceTable,
							"DestinationDb":    m.DestinationDsn.DBName,
							"DestinationTable": m.Iterations[x].DestinationTable,
						}, err)
					}
				}

				logger.Debugf(tag + "Tracking: Updating table")
				err = SerializeTrackingStatus(m.destinationDb, newTs)
				if err != nil {
					logger.Errorf(tag + "Tracking: " + err.Error())
				}

				ts = newTs

				if !more {
					logger.Infof(tag+"No more rows detected to process, sleeping for %d sec + random offset", delay)
					m.sleepWithInterrupt(delay)
					time.Sleep(time.Millisecond * (time.Duration(float64(delay*1000) * rand.Float64())))

					attempt = 0
					for {
						ts, err = GetTrackingStatus(m.destinationDb, m.SourceDsn.DBName, m.Iterations[x].SourceTable)
						if err != nil {
							logger.Warnf(tag+"GetTrackingStatus[Attempt %d, terminated=%#v]: %s", attempt, m.terminated, err.Error())
							attempt++
							m.sleepWithInterrupt(delay)
							if m.terminated {
								logger.Infof(tag + "Received quit signal")
								m.Close()
								m.wg.Done()
								return
							}
							continue
						}
						break
					}

				}

				// Sleep for 150ms to avoid pileups
				time.Sleep(time.Millisecond * 150)
			}
		}(x)

	}

	return nil
}

// Close forcibly closes the database connections for the Migrator instance
// and marks it as being uninitialized.
func (m *Migrator) Close() {
	tag := "Migrator.Close(): [" + m.SourceDsn.DBName + "] "

	logger.Infof(tag + "Closing connections")
	if m.sourceDb != nil {
		logger.Infof(tag + "Closing source db connection")
		m.sourceDb.Close()
	}
	if m.destinationDb != nil {
		logger.Infof(tag + "Closing destination db connection")
		m.destinationDb.Close()
	}

	m.initialized = false
}

// Quit is the method which should be used as the "preferred method" for
// terminating a Migrator instance.
func (m *Migrator) Quit() error {
	tag := "Migrator.Quit(): "

	if !m.initialized {
		m.terminated = true
		return errors.New(tag + "Not initialized")
	}

	logger.Infof(tag + "Sending quit signal")

	m.terminated = true

	return nil
}

// GetTrackingStatus retrieves the live tracking status for an Iteration from
// the destination database tracking table
func (m *Migrator) GetTrackingStatus(iter Iteration) (TrackingStatus, error) {
	return GetTrackingStatus(m.destinationDb, m.SourceDsn.DBName, iter.SourceTable)
}

// SerializeTrackingStatus serializes a live tracking status for the current
// migrator.
func (m *Migrator) SerializeTrackingStatus(ts TrackingStatus) error {
	return SerializeTrackingStatus(m.destinationDb, ts)
}

// ParseDSN parses the given go-sql-driver/mysql datasource name.
func ParseDSN(name string) apmsql.DSNInfo {
	cfg, err := mysql.ParseDSN(name)
	if err != nil {
		// mysql.Open will fail with the same error,
		// so just return a zero value.
		return apmsql.DSNInfo{}
	}
	return apmsql.DSNInfo{
		Database: cfg.DBName,
		User:     cfg.User,
	}
}

func init() {
	apmsql.Register("apmmysql", &mysql.MySQLDriver{}, apmsql.WithDSNParser(ParseDSN))
}
