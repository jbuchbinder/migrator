package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jbuchbinder/migrator"
)

var (
	configFile = flag.String("config-file", "migrator.yml", "Configuration file (YAML)")
)

func main() {
	flag.Parse()

	config, err := LoadConfigWithDefaults(*configFile)
	if err != nil {
		panic(err)
	}

	stop := make(chan os.Signal)
	signal.Notify(stop, syscall.SIGTERM)
	signal.Notify(stop, syscall.SIGINT)

	migrator.TrackingTableName = config.TrackingTableName

	migrators := make([]*migrator.Migrator, len(config.Migrations))

	for i := 0; i < len(config.Migrations); i++ {
		src, _ := mysql.ParseDSN(config.Migrations[i].Source.Dsn)
		dest, _ := mysql.ParseDSN(config.Migrations[i].Target.Dsn)

		if _, ok := migrator.ExtractorMap[config.Migrations[i].Extractor]; !ok {
			log.Printf("'%s' is not a valid type of extractor", config.Migrations[i].Extractor)
			continue
		}

		parameters := &migrator.Parameters{
			"BatchSize":         config.Parameters.BatchSize,
			"InsertBatchSize":   config.Parameters.InsertBatchSize,
			"SequentialReplace": config.Parameters.SequentialReplace,
			"SleepBetweenRuns":  config.Parameters.SleepBetweenRuns,
		}

		transformer := config.Migrations[i].Transformer
		if transformer == "" {
			transformer = "default"
		}

		if _, ok := migrator.TransformerMap[transformer]; !ok {
			log.Printf("Unable to resolve transformer '%s' for %#v", transformer, config.Migrations[i])
			panic("bailing out")
		}

		transformerParameters := config.Migrations[i].TransformerParameters
		if transformerParameters == nil {
			transformerParameters = parameters
		}

		log.Printf("Initializing with transformer parameters #%v", transformerParameters)
		migrators[i] = &migrator.Migrator{
			SourceDsn:             src,
			SourceTable:           config.Migrations[i].Source.Table,
			SourceKey:             config.Migrations[i].Source.Key,
			DestinationDsn:        dest,
			DestinationTable:      config.Migrations[i].Target.Table,
			Parameters:            parameters,
			Extractor:             migrator.ExtractorMap[config.Migrations[i].Extractor],
			Transformer:           migrator.TransformerMap[transformer],
			TransformerParameters: transformerParameters,
			Loader:                migrator.DefaultLoader,
		}
		err := migrators[i].Init()
		if err != nil {
			panic(err)
		}
		defer migrators[i].Close()
	}

	for i := range migrators {
		log.Printf("Starting migrator #%d", i)
		err = migrators[i].Run()
		if err != nil {
			log.Print(err)
			continue
		}
	}

	if config.Timeout != 0 {
		log.Printf("Sleeping for %d seconds waiting for runs to finish", config.Timeout)
		time.Sleep(time.Duration(config.Timeout) * time.Second)

		for i := range migrators {
			migrators[i].Quit()
		}
		return
	}

	log.Printf("Waiting for stop signals")
	sig := <-stop
	log.Printf("caught sig: %+v", sig)
	log.Printf("Signalling all migrators to stop")
	for i := range migrators {
		err := migrators[i].Quit()
		if err != nil {
			log.Printf("ERROR: %s", err.Error())
		}
	}
	log.Printf("Wait for %d seconds to finish processing", config.Parameters.SleepBetweenRuns*2)
	time.Sleep(2 * time.Duration(config.Parameters.SleepBetweenRuns) * time.Second)
	os.Exit(0)
}
