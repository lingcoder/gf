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

// UpdateAndScan updates data and scans the RETURNING clause result into the given pointer.
// The pointer can be type of *struct/**struct/*[]struct/*[]*struct.
//
// This method automatically uses RETURNING * to get all fields of the updated record(s).
// It's useful when you need to get the final values after update, especially with triggers
// or default values.
//
// Database support:
//   - PostgreSQL: Full support. PostgreSQL 18+ supports OLD and NEW prefixes.
//   - SQLite: Supported since version 3.35.0 (2021-03-12)
//   - SQL Server: Uses OUTPUT clause with INSERTED/DELETED prefixes
//   - MariaDB: Not yet supported for UPDATE
//   - DaMeng: Full support
//   - MySQL/ClickHouse: Not supported, will return error
//
// Example:
//
//	type User struct {
//	    Id        int64     `json:"id"`
//	    Name      string    `json:"name"`
//	    UpdatedAt time.Time `json:"updated_at"`
//	}
//	var user User
//	err := db.Model("users").Data(g.Map{"name": "John Updated"}).Where("id", 1).UpdateAndScan(&user)
//	// user now contains the updated values including any database-generated fields
//
//	// For multiple records:
//	var users []User
//	err := db.Model("users").Data(g.Map{"status": "active"}).Where("role", "admin").UpdateAndScan(&users)
func (m *Model) UpdateAndScan(pointer any, dataAndWhere ...any) error {
	var (
		model  = m.ReturningAll() // Automatically use RETURNING *
		result = model
	)

	if len(dataAndWhere) > 0 {
		if len(dataAndWhere) > 2 {
			result = result.Data(dataAndWhere[0]).Where(dataAndWhere[1], dataAndWhere[2:]...)
		} else if len(dataAndWhere) == 2 {
			result = result.Data(dataAndWhere[0]).Where(dataAndWhere[1])
		} else {
			result = result.Data(dataAndWhere[0])
		}
	}

	sqlResult, err := result.Update()
	if err != nil {
		return err
	}

	// Try to get RETURNING records from result
	rr := SqlResultToReturning(sqlResult)
	if rr == nil {
		return gerror.NewCode(
			gcode.CodeNotSupported,
			"UpdateAndScan is not supported by current database driver, "+
				"it requires database support for RETURNING clause (PostgreSQL, SQLite 3.35+, SQL Server, DaMeng)",
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
