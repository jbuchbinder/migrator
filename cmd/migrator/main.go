package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
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
		}
		migrators[i].SetWaitGroup(&wg)

		for j := range config.Migrations[i].Iterations {
			if _, ok := migrator.ExtractorMap[config.Migrations[i].Iterations[j].Extractor]; !ok {
				log.Printf("'%s' is not a valid type of extractor [%#v]", config.Migrations[i].Iterations[j].Extractor, config.Migrations[i].Iterations[j])
				continue
			}

			parameters := &migrator.Parameters{
				"Debug":             config.Debug,
				"BatchSize":         config.Parameters.BatchSize,
				"InsertBatchSize":   config.Parameters.InsertBatchSize,
				"SequentialReplace": config.Parameters.SequentialReplace,
				"SleepBetweenRuns":  config.Parameters.SleepBetweenRuns,
			}

			transformer := config.Migrations[i].Iterations[j].Transformer
			if transformer == "" {
				transformer = "default"
			}

			if _, ok := migrator.TransformerMap[transformer]; !ok {
				log.Printf("Unable to resolve transformer '%s' for %#v", transformer, config.Migrations[i])
				panic("bailing out")
			}

			transformerParameters := config.Migrations[i].Iterations[j].TransformerParameters
			if transformerParameters == nil {
				transformerParameters = parameters
			}

			log.Printf("Initializing with transformer parameters #%v", transformerParameters)
			iter := migrator.Iteration{
				SourceTable:           config.Migrations[i].Iterations[j].Source.Table,
				SourceKey:             config.Migrations[i].Iterations[j].Source.Key,
				DestinationTable:      config.Migrations[i].Iterations[j].Target.Table,
				Parameters:            parameters,
				Extractor:             migrator.ExtractorMap[config.Migrations[i].Iterations[j].Extractor],
				Transformer:           migrator.TransformerMap[transformer],
				TransformerParameters: transformerParameters,
				Loader:                migrator.DefaultLoader,
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
	log.Printf("Wait for all threads to finish processing")
	wg.Wait()
	os.Exit(0)
}
