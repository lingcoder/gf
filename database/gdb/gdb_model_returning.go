// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

// Returning sets the fields to be returned after INSERT/UPDATE/DELETE operations.
// This feature is supported by PostgreSQL, SQLite (3.35.0+), SQL Server (via OUTPUT clause),
// MariaDB (10.5.0+ for INSERT/DELETE), DaMeng, and Oracle (via RETURNING INTO in PL/SQL).
//
// The fields parameter specifies which columns should be returned. Multiple fields can be provided.
// For databases that don't support this feature (MySQL, ClickHouse), the clause will be ignored.
//
// Example:
//
//	// PostgreSQL/SQLite: INSERT INTO users(name, age) VALUES('John', 18) RETURNING id, created_at
//	db.Model("users").Data(g.Map{"name": "John", "age": 18}).Returning("id", "created_at").Insert()
//
//	// SQL Server: INSERT INTO users(name, age) OUTPUT INSERTED.id, INSERTED.created_at VALUES('John', 18)
//	db.Model("users").Data(g.Map{"name": "John", "age": 18}).Returning("id", "created_at").Insert()
//
// Database Support:
//   - PostgreSQL: Full support for INSERT/UPDATE/DELETE. PostgreSQL 18+ supports OLD and NEW prefixes.
//   - SQLite: Supported since version 3.35.0 (2021-03-12) for INSERT/UPDATE/DELETE.
//   - SQL Server: Uses OUTPUT clause (available since SQL Server 2005) for INSERT/UPDATE/DELETE/MERGE.
//   - MariaDB: Supported since 10.5.0 for INSERT and DELETE. UPDATE is not yet supported.
//   - DaMeng: Supports RETURNING clause for INSERT operations.
//   - Oracle: Uses RETURNING INTO clause, only available in PL/SQL context.
//   - MySQL: Not supported, the clause will be silently ignored.
//   - ClickHouse: Not supported, the clause will be silently ignored.
func (m *Model) Returning(fields ...string) *Model {
	model := m.getModel()
	model.returning = fields
	return model
}

// ReturningAll sets all fields to be returned after INSERT/UPDATE/DELETE operations.
// This is equivalent to using RETURNING * in SQL.
//
// Example:
//
//	// PostgreSQL/SQLite: INSERT INTO users(name, age) VALUES('John', 18) RETURNING *
//	db.Model("users").Data(g.Map{"name": "John", "age": 18}).ReturningAll().Insert()
//
// For database support information, see Returning() method documentation.
func (m *Model) ReturningAll() *Model {
	model := m.getModel()
	model.returning = []string{"*"}
	return model
}
