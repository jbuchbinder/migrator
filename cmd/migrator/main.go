package main

import (
	"flag"
	"log"
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

	migrator.TrackingTableName = config.TrackingTableName

	migrators := make([]*migrator.Migrator, len(config.Migrations))

	for i := 0; i < len(config.Migrations); i++ {
		src, _ := mysql.ParseDSN(config.Migrations[i].Source.Dsn)
		dest, _ := mysql.ParseDSN(config.Migrations[i].Target.Dsn)

		if _, ok := migrator.ExtractorMap[config.Migrations[i].Extractor]; !ok {
			log.Printf("'%s' is not a valid type of extractor", config.Migrations[i].Extractor)
			continue
		}

		log.Printf("Initializing with transformer parameters #%v", config.Migrations[i].TransformerParameters)
		migrators[i] = &migrator.Migrator{
			SourceDsn:        src,
			SourceTable:      config.Migrations[i].Source.Table,
			SourceKey:        config.Migrations[i].Source.Key,
			DestinationDsn:   dest,
			DestinationTable: config.Migrations[i].Target.Table,
			Parameters: &migrator.Parameters{
				"BatchSize":         config.Parameters.BatchSize,
				"InsertBatchSize":   config.Parameters.InsertBatchSize,
				"SequentialReplace": config.Parameters.SequentialReplace,
			},
			Extractor:             migrator.ExtractorMap[config.Migrations[i].Extractor],
			Transformer:           migrator.TransformerMap[config.Migrations[i].Transformer],
			TransformerParameters: config.Migrations[i].TransformerParameters,
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

	for {
		time.Sleep(500 * time.Millisecond)
	}
}
