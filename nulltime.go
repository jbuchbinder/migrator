package migrator

import (
	"database/sql/driver"
	"time"
)

// NullTime represents a time.Time object which can also represent a NULL
// DATETIME / TIMESTAMP value in MySQL.
type NullTime struct {
	Time  time.Time
	Valid bool
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value any) error {
	nt.Time, nt.Valid = value.(time.Time)
	return nil
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

// Unix exposes the underlying Unix() call of the wrapped time.Time structure.
func (nt NullTime) Unix() int64 {
	if nt.Valid {
		return nt.Time.Unix()
	}
	return 0
}

// MarshalJSON implements the json.Marshaler interface for encoding/json.
func (nt NullTime) MarshalJSON() ([]byte, error) {
	if nt.Valid {
		return nt.Time.MarshalJSON()
	}
	return []byte("null"), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface for encoding/json.
func (nt *NullTime) UnmarshalJSON(data []byte) (err error) {
	if len(data) < 3 || string(data) == "null" {
		*nt = NullTime{Valid: false}
		return
	}

	t, err := time.Parse(`"`+time.RFC3339+`"`, string(data))
	*nt = NullTime{t, err == nil}
	return
}

// NullTimeNow creates a new NullTime instance representing the current time.
func NullTimeNow() NullTime {
	return NullTime{time.Now(), true}
}

// NullTimeFromTime creates a new NullTime instance with the specified time.
func NullTimeFromTime(t time.Time) NullTime {
	return NullTime{t, true}
}
