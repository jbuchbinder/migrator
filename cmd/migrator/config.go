package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

var (
	Config *MigratorConfig
)

type MigratorConfig struct {
	Debug             bool         `yaml:"debug"`
	Port              int          `yaml:"port"`
	Migrations        []Migrations `yaml:"migrations"`
	TrackingTableName string       `yaml:"tracking-table"`
	Parameters        struct {
		BatchSize       int `yaml:"batch-size"`
		InsertBatchSize int `yaml:"insert-batch-size"`
	} `yaml:"parameters"`
	Timeout int `yaml:"timeout"`
}

type Migrations struct {
	Source struct {
		Dsn   string `yaml:"dsn"`
		Table string `yaml:"table"`
	} `yaml:"source"`
	Target struct {
		Dsn   string `yaml:"dsn"`
		Table string `yaml:"table"`
	} `yaml:"target"`
	Extractor string
}

func (c *MigratorConfig) SetDefaults() {
	c.Debug = false
	c.Port = 3040
	c.TrackingTableName = "Tracking"
	c.Timeout = 0
}

func LoadConfigWithDefaults(configPath string) (*MigratorConfig, error) {
	c := &MigratorConfig{}
	c.SetDefaults()
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return c, err
	}
	err = yaml.Unmarshal([]byte(data), c)
	return c, err
}
