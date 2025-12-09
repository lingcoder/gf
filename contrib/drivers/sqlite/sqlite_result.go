// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package sqlite

import (
	"database/sql"

	"github.com/gogf/gf/v2/database/gdb"
)

// Result is the result type for SQLite database operations.
// It implements both sql.Result and gdb.ReturningResult interfaces.
type Result struct {
	sql.Result
	records gdb.Result // Records from RETURNING clause
}

// RowsAffected returns the number of rows affected by the operation.
// When RETURNING clause is used, it returns the count of returned records.
func (r Result) RowsAffected() (int64, error) {
	if r.records != nil {
		return int64(len(r.records)), nil
	}
	if r.Result != nil {
		return r.Result.RowsAffected()
	}
	return 0, nil
}

// LastInsertId returns the last inserted ID.
func (r Result) LastInsertId() (int64, error) {
	if r.Result != nil {
		return r.Result.LastInsertId()
	}
	return 0, nil
}

// GetRecords returns the records from RETURNING clause.
// Returns nil if no RETURNING clause was specified or no data was returned.
// This method implements the gdb.ReturningResult interface.
func (r Result) GetRecords() gdb.Result {
	return r.records
}
