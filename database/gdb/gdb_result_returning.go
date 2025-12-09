// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import "database/sql"

// ReturningResult is an extended sql.Result interface that includes RETURNING clause data.
// This interface is implemented by database drivers that support the RETURNING clause
// (PostgreSQL, SQLite 3.35+, SQL Server OUTPUT, MariaDB 10.5+, DaMeng, Oracle).
//
// Usage example:
//
//	result, err := db.Model("users").Data(g.Map{"name": "John"}).Returning("id", "created_at").Insert()
//	if err != nil {
//	    return err
//	}
//	// Type assertion to get RETURNING data
//	if rr, ok := result.(gdb.ReturningResult); ok {
//	    records := rr.GetRecords()
//	    // records contains the returned rows
//	}
type ReturningResult interface {
	sql.Result

	// GetRecords returns the records from RETURNING clause.
	// Returns nil if the database driver doesn't support RETURNING or if no RETURNING clause was specified.
	GetRecords() Result
}

// SqlResultToReturning attempts to convert a sql.Result to ReturningResult.
// Returns nil if the result doesn't implement ReturningResult interface.
func SqlResultToReturning(result sql.Result) ReturningResult {
	if rr, ok := result.(ReturningResult); ok {
		return rr
	}
	return nil
}
