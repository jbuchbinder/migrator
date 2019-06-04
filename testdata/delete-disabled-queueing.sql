## Database and table pair for delete-disabled queueing

DROP DATABASE IF EXISTS a;
DROP DATABASE IF EXISTS b;

CREATE DATABASE a;

USE a;

CREATE TABLE x (
	id SERIAL,
	name VARCHAR(100) NOT NULL,
	dob DATETIME,
	enabled BOOL
);


CREATE TABLE `MigratorRecordQueue` (
	sourceDatabase		VARCHAR(100) NOT NULL,
	sourceTable			VARCHAR(100) NOT NULL,
	pkColumn 			VARCHAR(100) NOT NULL,
	pkValue 			VARCHAR(100) NOT NULL,
	timestampUpdated 	TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	method 				ENUM( 'UPDATE', 'REMOVE' ) DEFAULT 'UPDATE',

	KEY (sourceDatabase, sourceTable),
	KEY (method),
	KEY (timestampUpdated)
);

CREATE DATABASE b;

USE b;

CREATE TABLE x (
	id SERIAL,
	name VARCHAR(100) NOT NULL,
	dob DATETIME,
	enabled BOOL
);

#CREATE TABLE `EtlPosition` (
#	sourceDatabase		VARCHAR(100) DEFAULT '',
#	sourceTable		VARCHAR(100) DEFAULT '',
#	columnName		VARCHAR(100) DEFAULT '',
#	sequentialPosition	BIGINT DEFAULT 0,
#	timestampPosition	TIMESTAMP NULL DEFAULT NULL,
#	lastRun			TIMESTAMP NULL DEFAULT NULL
#);

#INSERT INTO EtlPosition VALUES ( 'a', 'x', 'id', 0, NULL, NOW() );

## Set triggers for queue method

USE a;

DELIMITER $$
CREATE TRIGGER Migrator_x_Update
    AFTER UPDATE ON x
    FOR EACH ROW
BEGIN
    INSERT INTO MigratorRecordQueue (
        sourceDatabase,
        sourceTable,
        pkColumn,
        pkValue,
        timestampUpdated
    ) VALUES (
        'a',
        'x',
        'id',
        OLD.id,
        NOW()
    );
END$$

CREATE TRIGGER Migrator_x_Insert
    AFTER INSERT ON x
    FOR EACH ROW
BEGIN
    INSERT INTO MigratorRecordQueue (
        sourceDatabase,
        sourceTable,
        pkColumn,
        pkValue,
        timestampUpdated
    ) VALUES (
        'a',
        'x',
        'id',
        NEW.id,
        NOW()
    );
END$$

DELIMITER ;

## Stock some values in there

INSERT INTO x VALUES
	( 1, 'Andrew Abramson', '1930-01-02', TRUE ),
	( 2, 'Brett Baker', '1942-03-14', TRUE ),
	( 3, 'Charlie Collins', '1945-11-09', FALSE ),
	( 4, 'Dirk Delta', '1982-03-18', TRUE );

