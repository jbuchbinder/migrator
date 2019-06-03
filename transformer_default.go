package migrator

// DefaultTransformer by default does nothing -- the data is not transformed.
var DefaultTransformer = func(dbName, tableName string, data []SQLRow, params *Parameters) []TableData {
	method, ok := (*params)["METHOD"].(string)
	if !ok {
		method = ""
	}
	return []TableData{
		{
			DbName:    dbName,
			TableName: tableName,
			Data:      data,
			Method:    method,
		},
	}
}
