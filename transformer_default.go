package migrator

// DefaultTransformer by default does nothing -- the data is not transformed.
var DefaultTransformer = func(data []SqlUntypedRow, params Parameters) []SqlUntypedRow {
	return data
}
