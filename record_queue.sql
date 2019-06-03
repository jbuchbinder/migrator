
CREATE TABLE `MigratorRecordQueue` (
	sourceDatabase		VARCHAR(100) NOT NULL,
	sourceTable			VARCHAR(100) NOT NULL,
	pkColumn 			VARCHAR(100) NOT NULL,
	pkValue 			VARCHAR(100) NOT NULL,
	timestampUpdated 	TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

	KEY (timestampUpdated)
);