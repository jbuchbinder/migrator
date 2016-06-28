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
func (nt *NullTime) Scan(value interface{}) error {
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

func (nt NullTime) Unix() int64 {
	if nt.Valid {
		return nt.Time.Unix()
	}
	return 0
}

func (nt NullTime) MarshalJSON() ([]byte, error) {
	if nt.Valid {
		return nt.Time.MarshalJSON()
	}
	return []byte("null"), nil
}

func (nt *NullTime) UnmarshalJSON(data []byte) (err error) {
	if data == nil || len(data) < 3 {
		*nt = NullTime{Valid: false}
		return
	}

	t, err := time.Parse(`"`+time.RFC3339+`"`, string(data))
	*nt = NullTime{t, err == nil}
	return
}

func NullTimeNow() NullTime {
	return NullTime{time.Now(), true}
}

func NullTimeFromTime(t time.Time) NullTime {
	return NullTime{t, true}
}
