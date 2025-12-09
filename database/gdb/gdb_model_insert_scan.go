// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"database/sql"

	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
)

// InsertAndScan inserts data and scans the RETURNING clause result into the given pointer.
// The pointer can be type of *struct/**struct/*[]struct/*[]*struct.
//
// This method automatically uses RETURNING * to get all fields of the inserted record(s).
// It's useful when you need to get auto-generated values like auto-increment IDs, timestamps, etc.
//
// Database support:
//   - PostgreSQL: Full support
//   - SQLite: Supported since version 3.35.0 (2021-03-12)
//   - SQL Server: Uses OUTPUT clause
//   - MariaDB: Supported since 10.5.0
//   - DaMeng: Full support
//   - MySQL/ClickHouse: Not supported, will return error
//
// Example:
//
//	type User struct {
//	    Id        int64     `json:"id"`
//	    Name      string    `json:"name"`
//	    CreatedAt time.Time `json:"created_at"`
//	}
//	var user User
//	err := db.Model("users").Data(g.Map{"name": "John"}).InsertAndScan(&user)
//	// user.Id and user.CreatedAt are now populated with values from database
//
//	// For batch insert:
//	var users []User
//	err := db.Model("users").Data(g.Slice{
//	    g.Map{"name": "John"},
//	    g.Map{"name": "Jane"},
//	}).InsertAndScan(&users)
func (m *Model) InsertAndScan(pointer any, data ...any) error {
	return m.doInsertAndScan(pointer, InsertOptionDefault, data...)
}

// InsertIgnoreAndScan does "INSERT IGNORE INTO ..." statement and scans the RETURNING clause
// result into the given pointer. The pointer can be type of *struct/**struct/*[]struct/*[]*struct.
//
// For database support, see InsertAndScan.
func (m *Model) InsertIgnoreAndScan(pointer any, data ...any) error {
	return m.doInsertAndScan(pointer, InsertOptionIgnore, data...)
}

// SaveAndScan does "INSERT INTO ... ON DUPLICATE KEY UPDATE..." statement and scans the
// RETURNING clause result into the given pointer.
// The pointer can be type of *struct/**struct/*[]struct/*[]*struct.
//
// For database support, see InsertAndScan.
func (m *Model) SaveAndScan(pointer any, data ...any) error {
	return m.doInsertAndScan(pointer, InsertOptionSave, data...)
}

// ReplaceAndScan does "REPLACE INTO ..." statement and scans the RETURNING clause result
// into the given pointer. The pointer can be type of *struct/**struct/*[]struct/*[]*struct.
//
// For database support, see InsertAndScan.
func (m *Model) ReplaceAndScan(pointer any, data ...any) error {
	return m.doInsertAndScan(pointer, InsertOptionReplace, data...)
}

// doInsertAndScan performs the insert operation with RETURNING clause and scans the result.
func (m *Model) doInsertAndScan(pointer any, insertOption InsertOption, data ...any) error {
	var (
		model  = m.ReturningAll() // Automatically use RETURNING *
		result sql.Result
		err    error
	)

	if len(data) > 0 {
		model = model.Data(data...)
	}

	switch insertOption {
	case InsertOptionDefault:
		result, err = model.Insert()
	case InsertOptionIgnore:
		result, err = model.InsertIgnore()
	case InsertOptionSave:
		result, err = model.Save()
	case InsertOptionReplace:
		result, err = model.Replace()
	default:
		result, err = model.Insert()
	}

	if err != nil {
		return err
	}

	// Try to get RETURNING records from result
	rr := SqlResultToReturning(result)
	if rr == nil {
		return gerror.NewCode(
			gcode.CodeNotSupported,
			"InsertAndScan is not supported by current database driver, "+
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
