package main

import (
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jbuchbinder/migrator"
	log "github.com/sirupsen/logrus"
)

var (
	configFile = flag.String("config-file", "migrator.yml", "Configuration file (YAML)")
)

func main() {
	flag.Parse()

	logger := log.StandardLogger()

	config, err := LoadConfigWithDefaults(*configFile)
	if err != nil {
		panic(err)
	}

	stop := make(chan os.Signal)
	signal.Notify(stop, syscall.SIGTERM)
	signal.Notify(stop, syscall.SIGINT)

	migrator.TrackingTableName = config.TrackingTableName
	migrator.SetLogger(logger)

	var wg sync.WaitGroup

	migrators := make([]*migrator.Migrator, len(config.Migrations))

	for i := 0; i < len(config.Migrations); i++ {
		src, _ := mysql.ParseDSN(config.Migrations[i].SourceDsn)
		dest, _ := mysql.ParseDSN(config.Migrations[i].TargetDsn)

		migrators[i] = &migrator.Migrator{
			SourceDsn:      src,
			DestinationDsn: dest,
			Apm:            config.Migrations[i].Apm,
			Iterations:     []migrator.Iteration{},
			Parameters: &migrator.Parameters{
				migrator.ParamDebug:             config.Debug,
				migrator.ParamBatchSize:         config.Parameters.BatchSize,
				migrator.ParamInsertBatchSize:   config.Parameters.InsertBatchSize,
				migrator.ParamSequentialReplace: config.Parameters.SequentialReplace,
				migrator.ParamSleepBetweenRuns:  config.Parameters.SleepBetweenRuns,
			},
		}
		migrators[i].SetWaitGroup(&wg)

		for j := range config.Migrations[i].Iterations {
			if _, ok := migrator.ExtractorMap[config.Migrations[i].Iterations[j].Extractor]; !ok {
				logger.Printf("'%s' is not a valid type of extractor [%#v]", config.Migrations[i].Iterations[j].Extractor, config.Migrations[i].Iterations[j])
				continue
			}

			parameters := &migrator.Parameters{
				migrator.ParamDebug:             config.Debug,
				migrator.ParamBatchSize:         config.Parameters.BatchSize,
				migrator.ParamInsertBatchSize:   config.Parameters.InsertBatchSize,
				migrator.ParamSequentialReplace: config.Parameters.SequentialReplace,
				migrator.ParamSleepBetweenRuns:  config.Parameters.SleepBetweenRuns,
			}

			transformer := config.Migrations[i].Iterations[j].Transformer
			if transformer == "" {
				transformer = "default"
			}

			if _, ok := migrator.TransformerMap[transformer]; !ok {
				logger.Printf("Unable to resolve transformer '%s' for %#v", transformer, config.Migrations[i])
				panic("bailing out")
			}

			transformerParameters := config.Migrations[i].Iterations[j].TransformerParameters
			if transformerParameters == nil {
				transformerParameters = parameters
			}

			logger.Printf("Initializing with transformer parameters #%v", transformerParameters)
			iter := migrator.Iteration{
				SourceTable:           config.Migrations[i].Iterations[j].Source.Table,
				SourceKey:             config.Migrations[i].Iterations[j].Source.Key,
				DestinationTable:      config.Migrations[i].Iterations[j].Target.Table,
				Parameters:            parameters,
				Extractor:             migrator.ExtractorMap[config.Migrations[i].Iterations[j].Extractor],
				ExtractorName:         config.Migrations[i].Iterations[j].Extractor,
				Transformer:           migrator.TransformerMap[transformer],
				TransformerParameters: transformerParameters,
				Loader:                migrator.DefaultLoader,
				LoaderName:            "default",
			}
			migrators[i].Iterations = append(migrators[i].Iterations, iter)
		}
		err := migrators[i].Init()
		if err != nil {
			panic(err)
		}
		defer migrators[i].Close()
	}

	for i := range migrators {
		logger.Printf("Starting migrator #%d", i)
		err = migrators[i].Run()
		if err != nil {
			log.Print(err)
			continue
		}
	}

	if config.Timeout != 0 {
		logger.Printf("Sleeping for %d seconds waiting for runs to finish", config.Timeout)
		time.Sleep(time.Duration(config.Timeout) * time.Second)

		for i := range migrators {
			migrators[i].Quit()
		}
		return
	}

	logger.Printf("Waiting for stop signals")
	sig := <-stop
	logger.Printf("caught sig: %+v", sig)
	logger.Printf("Signalling all migrators to stop")
	for i := range migrators {
		err := migrators[i].Quit()
		if err != nil {
			logger.Printf("ERROR: %s", err.Error())
		}
	}
	logger.Printf("Wait for all threads to finish processing")
	wg.Wait()
	os.Exit(0)
}
