package migrator

// DefaultTransformer by default does nothing -- the data is not transformed.
var DefaultTransformer = func(dbName, tableName string, data []SqlUntypedRow, params Parameters) []TableData {
	return []TableData{
		TableData{
			DbName:    dbName,
			TableName: tableName,
			Data:      data,
		},
	}
}
