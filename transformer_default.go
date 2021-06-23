package migrator

func init() {
	TransformerMap["default"] = DefaultTransformer
}

// DefaultTransformer by default does nothing -- the data is not transformed.
var DefaultTransformer = func(dbName, tableName string, data []SQLRow, params *Parameters) []TableData {
	method, ok := (*params)[ParamMethod].(string)
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
