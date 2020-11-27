module github.com/jbuchbinder/migrator/cmd/migrator

go 1.15

replace github.com/jbuchbinder/migrator => ../../

require (
	github.com/go-sql-driver/mysql v1.5.0
	github.com/jbuchbinder/migrator v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.7.0
	gopkg.in/yaml.v2 v2.3.0
)
