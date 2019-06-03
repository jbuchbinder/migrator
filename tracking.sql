
CREATE TABLE `EtlPosition` (
	sourceDatabase		VARCHAR(100) DEFAULT '',
	sourceTable		VARCHAR(100) DEFAULT '',
	columnName		VARCHAR(100) DEFAULT '',
	sequentialPosition	BIGINT DEFAULT 0,
	timestampPosition	TIMESTAMP NULL DEFAULT NULL,
	lastRun			TIMESTAMP NULL DEFAULT NULL
);
