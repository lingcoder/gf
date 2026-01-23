// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gaussdb_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/test/gtest"
	"github.com/gogf/gf/v2/text/gstr"
)

// https://github.com/gogf/gf/issues/4495
// Test Tables() returns tables from specific schema using explicit schema parameter.
func Test_Issue4495_Tables_SearchPath(t *testing.T) {
	var (
		schema1     = fmt.Sprintf("test_schema1_%d", gtime.TimestampNano())
		schema2     = fmt.Sprintf("test_schema2_%d", gtime.TimestampNano())
		table1      = fmt.Sprintf("t_only_in_schema1_%d", gtime.TimestampNano())
		table2      = fmt.Sprintf("t_only_in_schema2_%d", gtime.TimestampNano())
		tableCommon = fmt.Sprintf("t_common_%d", gtime.TimestampNano())
	)

	// Create two schemas
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema1)); err != nil {
		gtest.Fatal(err)
	}
	defer func() {
		db.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema1))
	}()

	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema2)); err != nil {
		gtest.Fatal(err)
	}
	defer func() {
		db.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema2))
	}()

	// Create table only in schema1
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.%s (id int)`, schema1, table1)); err != nil {
		gtest.Fatal(err)
	}

	// Create table only in schema2
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.%s (id int)`, schema2, table2)); err != nil {
		gtest.Fatal(err)
	}

	// Create same-name table in both schemas
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.%s (id int)`, schema1, tableCommon)); err != nil {
		gtest.Fatal(err)
	}
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.%s (id int)`, schema2, tableCommon)); err != nil {
		gtest.Fatal(err)
	}

	// Test 1: Tables() with explicit schema parameter for schema1
	gtest.C(t, func(t *gtest.T) {
		tables, err := db.Tables(ctx, schema1)
		t.AssertNil(err)

		// Should contain table1 and tableCommon from schema1
		t.Assert(gstr.InArray(tables, table1), true)
		t.Assert(gstr.InArray(tables, tableCommon), true)
		// Should NOT contain table2 (it's in schema2)
		t.Assert(gstr.InArray(tables, table2), false)
	})

	// Test 2: Tables() with explicit schema parameter for schema2
	gtest.C(t, func(t *gtest.T) {
		tables, err := db.Tables(ctx, schema2)
		t.AssertNil(err)

		// Should contain table2 and tableCommon from schema2
		t.Assert(gstr.InArray(tables, table2), true)
		t.Assert(gstr.InArray(tables, tableCommon), true)
		// Should NOT contain table1 (it's in schema1)
		t.Assert(gstr.InArray(tables, table1), false)
	})
}

// https://github.com/gogf/gf/issues/4495
// Test TableFields() returns correct field info for same-name tables in different schemas
// using schema-qualified table names like "schema.table".
func Test_Issue4495_TableFields_SchemaFilter(t *testing.T) {
	var (
		schema1   = fmt.Sprintf("test_schema1_%d", gtime.TimestampNano())
		schema2   = fmt.Sprintf("test_schema2_%d", gtime.TimestampNano())
		tableName = fmt.Sprintf("t_issue4495_%d", gtime.TimestampNano())
	)

	// Create two schemas
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema1)); err != nil {
		gtest.Fatal(err)
	}
	defer func() {
		db.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema1))
	}()

	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema2)); err != nil {
		gtest.Fatal(err)
	}
	defer func() {
		db.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema2))
	}()

	// Create same-name table in schema1 with default value 'default_from_schema1'
	if _, err := db.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.%s (
			id bigserial PRIMARY KEY,
			name varchar(100) DEFAULT 'default_from_schema1'
		)`, schema1, tableName)); err != nil {
		gtest.Fatal(err)
	}

	// Create same-name table in schema2 with DIFFERENT default value 'default_from_schema2'
	if _, err := db.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.%s (
			id bigserial PRIMARY KEY,
			name varchar(100) DEFAULT 'default_from_schema2'
		)`, schema2, tableName)); err != nil {
		gtest.Fatal(err)
	}

	// Test 1: TableFields with schema-qualified table name for schema1
	gtest.C(t, func(t *gtest.T) {
		// Query schema1's table explicitly using schema-qualified name
		fields, err := db.TableFields(ctx, fmt.Sprintf("%s.%s", schema1, tableName))
		t.AssertNil(err)

		nameField, ok := fields["name"]
		t.Assert(ok, true)

		defaultValue := nameField.Default
		t.AssertNE(defaultValue, nil)

		defaultStr := fmt.Sprintf("%v", defaultValue)
		t.Assert(strings.Contains(defaultStr, "schema1"), true)
	})

	// Test 2: TableFields with schema-qualified table name for schema2
	gtest.C(t, func(t *gtest.T) {
		// Query schema2's table explicitly using schema-qualified name
		fields, err := db.TableFields(ctx, fmt.Sprintf("%s.%s", schema2, tableName))
		t.AssertNil(err)

		nameField, ok := fields["name"]
		t.Assert(ok, true)

		defaultValue := nameField.Default
		t.AssertNE(defaultValue, nil)

		defaultStr := fmt.Sprintf("%v", defaultValue)
		t.Assert(strings.Contains(defaultStr, "schema2"), true)
	})
}

// https://github.com/gogf/gf/issues/4495
// Test that partitioned tables are handled correctly (parent table included, child partitions excluded).
// Note: GaussDB may have different partition support depending on version.
func Test_Issue4495_Tables_PartitionedTables(t *testing.T) {
	var (
		schema      = fmt.Sprintf("test_schema_%d", gtime.TimestampNano())
		parentTable = fmt.Sprintf("t_parent_%d", gtime.TimestampNano())
	)

	// Create schema
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema)); err != nil {
		gtest.Fatal(err)
	}
	defer func() {
		db.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
	}()

	// Create partitioned table (PostgreSQL 10+ syntax)
	_, err := db.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.%s (
			id int,
			created_at date
		) PARTITION BY RANGE (created_at)`, schema, parentTable))
	if err != nil {
		// Skip test if GaussDB version doesn't support this partitioning syntax
		t.Logf("Skipping partition test: %v", err)
		return
	}

	// Create partition child tables
	_, err = db.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.%s_2024 PARTITION OF %s.%s
		FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')`,
		schema, parentTable, schema, parentTable))
	if err != nil {
		gtest.Fatal(err)
	}

	_, err = db.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.%s_2025 PARTITION OF %s.%s
		FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')`,
		schema, parentTable, schema, parentTable))
	if err != nil {
		gtest.Fatal(err)
	}

	gtest.C(t, func(t *gtest.T) {
		tables, err := db.Tables(ctx, schema)
		t.AssertNil(err)

		// Should contain parent table
		t.Assert(gstr.InArray(tables, parentTable), true)

		// Should NOT contain partition child tables
		t.Assert(gstr.InArray(tables, parentTable+"_2024"), false)
		t.Assert(gstr.InArray(tables, parentTable+"_2025"), false)
	})
}

// https://github.com/gogf/gf/issues/4495
// Test cache isolation for different schemas.
func Test_Issue4495_CacheIsolation(t *testing.T) {
	var (
		schema1 = fmt.Sprintf("test_cache_schema1_%d", gtime.TimestampNano())
		schema2 = fmt.Sprintf("test_cache_schema2_%d", gtime.TimestampNano())
		table1  = fmt.Sprintf("t_cache1_%d", gtime.TimestampNano())
		table2  = fmt.Sprintf("t_cache2_%d", gtime.TimestampNano())
	)

	// Create two schemas with different tables
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema1)); err != nil {
		gtest.Fatal(err)
	}
	defer func() {
		db.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema1))
	}()

	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema2)); err != nil {
		gtest.Fatal(err)
	}
	defer func() {
		db.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema2))
	}()

	// Create different tables in each schema
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.%s (id int)`, schema1, table1)); err != nil {
		gtest.Fatal(err)
	}
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.%s (id int)`, schema2, table2)); err != nil {
		gtest.Fatal(err)
	}

	gtest.C(t, func(t *gtest.T) {
		// Get tables from schema1 using explicit schema parameter
		// Note: db.Schema() in GoFrame changes database name, not GaussDB schema.
		// For GaussDB schema support, we use db.Tables(ctx, schema) parameter.
		tables1, err := db.Tables(ctx, schema1)
		t.AssertNil(err)
		t.Assert(gstr.InArray(tables1, table1), true)
		t.Assert(gstr.InArray(tables1, table2), false)

		// Get tables from schema2 - should NOT return schema1's cached result
		tables2, err := db.Tables(ctx, schema2)
		t.AssertNil(err)
		t.Assert(gstr.InArray(tables2, table2), true)
		t.Assert(gstr.InArray(tables2, table1), false)

		// Verify they are different
		t.AssertNE(len(tables1), 0)
		t.AssertNE(len(tables2), 0)
	})
}

// https://github.com/gogf/gf/issues/4495
// Test TableFields() correctly detects primary key and unique key constraints.
func Test_Issue4495_TableFields_KeyConstraints(t *testing.T) {
	var (
		schema    = fmt.Sprintf("test_schema_%d", gtime.TimestampNano())
		tableName = fmt.Sprintf("t_keys_%d", gtime.TimestampNano())
	)

	// Create schema
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema)); err != nil {
		gtest.Fatal(err)
	}
	defer func() {
		db.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
	}()

	// Create table with primary key and unique constraint
	if _, err := db.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.%s (
			id bigserial PRIMARY KEY,
			email varchar(100) UNIQUE NOT NULL,
			name varchar(100)
		)`, schema, tableName)); err != nil {
		gtest.Fatal(err)
	}

	gtest.C(t, func(t *gtest.T) {
		fields, err := db.TableFields(ctx, fmt.Sprintf("%s.%s", schema, tableName))
		t.AssertNil(err)

		// Check primary key
		idField, ok := fields["id"]
		t.Assert(ok, true)
		t.Assert(idField.Key, "pri")

		// Check unique key
		emailField, ok := fields["email"]
		t.Assert(ok, true)
		t.Assert(emailField.Key, "uni")

		// Check normal field (no key)
		nameField, ok := fields["name"]
		t.Assert(ok, true)
		t.Assert(nameField.Key, "")
	})
}

// https://github.com/gogf/gf/issues/4495
// Test Tables() with explicit schema parameter.
// Note: In GaussDB (like PostgreSQL), db.Schema() changes the database name, not the schema.
// For GaussDB schema support, use db.Tables(ctx, schema) parameter instead.
func Test_Issue4495_Tables_WithNamespace(t *testing.T) {
	var (
		schema = fmt.Sprintf("test_ns_schema_%d", gtime.TimestampNano())
		table  = fmt.Sprintf("t_ns_table_%d", gtime.TimestampNano())
	)

	// Create schema and table
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema)); err != nil {
		gtest.Fatal(err)
	}
	defer func() {
		db.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
	}()

	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.%s (id int)`, schema, table)); err != nil {
		gtest.Fatal(err)
	}

	gtest.C(t, func(t *gtest.T) {
		// Use explicit schema parameter to get tables from specific schema
		tables, err := db.Tables(ctx, schema)
		t.AssertNil(err)
		t.Assert(gstr.InArray(tables, table), true)
	})
}

// https://github.com/gogf/gf/issues/4495
// Test TableFields() with table that doesn't exist returns error.
func Test_Issue4495_TableFields_NotExists(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		_, err := db.TableFields(ctx, "non_existent_table_12345")
		// Should return an error for non-existent table
		t.AssertNE(err, nil)
	})
}

// https://github.com/gogf/gf/issues/4495
// Test Tables() returns empty result for empty schema.
func Test_Issue4495_Tables_EmptySchema(t *testing.T) {
	var (
		schema = fmt.Sprintf("test_empty_schema_%d", gtime.TimestampNano())
	)

	// Create empty schema (no tables)
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema)); err != nil {
		gtest.Fatal(err)
	}
	defer func() {
		db.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
	}()

	gtest.C(t, func(t *gtest.T) {
		tables, err := db.Tables(ctx, schema)
		t.AssertNil(err)
		t.Assert(len(tables), 0)
	})
}
