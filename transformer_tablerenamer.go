package migrator

import "log"

func init() {
	TransformerMap["tablerenamer"] = TableRenamerTransformer
}

// TableRenamerTransformer adjusts the
var TableRenamerTransformer = func(dbName, tableName string, data []SQLRow, params *Parameters) []TableData {
	method, ok := (*params)["METHOD"].(string)
	if !ok {
		method = ""
	}

	newTableName, ok := (*params)["TableName"].(string)
	if !ok {
		log.Printf("TableRenamerTransformer: parameter TableName not passed, retaining %s as name", tableName)
		newTableName = tableName
	}

	return []TableData{
		{
			DbName:    dbName,
			TableName: newTableName,
			Data:      data,
			Method:    method,
		},
	}
}
