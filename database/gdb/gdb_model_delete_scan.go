// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
)

// DeleteAndScan deletes data and scans the RETURNING clause result into the given pointer.
// The pointer can be type of *struct/**struct/*[]struct/*[]*struct.
//
// This method automatically uses RETURNING * to get all fields of the deleted record(s).
// It's useful when you need to capture data before deletion, for example for audit logs,
// undo operations, or cascading business logic.
//
// Database support:
//   - PostgreSQL: Full support. PostgreSQL 18+ supports OLD prefix for accessing old values.
//   - SQLite: Supported since version 3.35.0 (2021-03-12)
//   - SQL Server: Uses OUTPUT clause with DELETED prefix
//   - MariaDB: Supported since 10.5.0
//   - DaMeng: Full support
//   - MySQL/ClickHouse: Not supported, will return error
//
// Example:
//
//	type User struct {
//	    Id    int64  `json:"id"`
//	    Name  string `json:"name"`
//	    Email string `json:"email"`
//	}
//	var deletedUser User
//	err := db.Model("users").Where("id", 1).DeleteAndScan(&deletedUser)
//	// deletedUser now contains the data of the deleted record
//
//	// For multiple records:
//	var deletedUsers []User
//	err := db.Model("users").Where("status", "inactive").DeleteAndScan(&deletedUsers)
//	// deletedUsers contains all deleted records
func (m *Model) DeleteAndScan(pointer any, where ...any) error {
	var (
		model = m.ReturningAll() // Automatically use RETURNING *
	)

	if len(where) > 0 {
		model = model.Where(where[0], where[1:]...)
	}

	result, err := model.Delete()
	if err != nil {
		return err
	}

	// Try to get RETURNING records from result
	rr := SqlResultToReturning(result)
	if rr == nil {
		return gerror.NewCode(
			gcode.CodeNotSupported,
			"DeleteAndScan is not supported by current database driver, "+
				"it requires database support for RETURNING clause (PostgreSQL, SQLite 3.35+, SQL Server, MariaDB 10.5+, DaMeng)",
		)
	}

	records := rr.GetRecords()
	if records == nil || len(records) == 0 {
		return gerror.NewCode(
			gcode.CodeNotFound,
			"no records returned from RETURNING clause",
		)
	}

	// Scan records into pointer
	return records.Structs(pointer)
}
