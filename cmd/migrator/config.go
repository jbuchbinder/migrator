package main

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

// MigratorConfig defines the basic structure which is deserialized from the
// migrator YAML configuration file.
type MigratorConfig struct {
	Debug             bool         `yaml:"debug"`
	Port              int          `yaml:"port"`
	Migrations        []Migrations `yaml:"migrations"`
	TrackingTableName string       `yaml:"tracking-table"`
	Parameters        struct {
		BatchSize         int  `yaml:"batch-size"`
		InsertBatchSize   int  `yaml:"insert-batch-size"`
		SequentialReplace bool `yaml:"sequential-replace"`
	} `yaml:"parameters"`
	Timeout int `yaml:"timeout"`
}

// Migrations represents a single migration coniguration instance.
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

// SetDefaults creates a series of reasonable default values for the current
// MigratorConfig instance.
func (c *MigratorConfig) SetDefaults() {
	c.Debug = false
	c.Port = 3040
	c.TrackingTableName = "Tracking"
	c.Timeout = 0
}

// LoadConfigWithDefaults loads a YAML configuration file representing a
// MigratorConfig structure.
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
