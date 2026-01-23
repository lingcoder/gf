// Copyright 2019 gf Author(https://github.com/gogf/gf). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package dm_test

import (
	"testing"
	"time"

	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/test/gtest"
	"github.com/gogf/gf/v2/text/gstr"
)

func Test_Issue2594(t *testing.T) {
	table := "HANDLE_INFO"
	array := gstr.SplitAndTrim(gtest.DataContent(`issue`, `2594`, `sql.sql`), ";")
	for _, v := range array {
		if _, err := db.Exec(ctx, v); err != nil {
			gtest.Error(err)
		}
	}
	defer dropTable(table)

	type HandleValueMysql struct {
		Index int64  `orm:"index"`
		Type  string `orm:"type"`
		Data  []byte `orm:"data"`
	}
	type HandleInfoMysql struct {
		Id         int                `orm:"id,primary" json:"id"`
		SubPrefix  string             `orm:"sub_prefix"`
		Prefix     string             `orm:"prefix"`
		HandleName string             `orm:"handle_name"`
		CreateTime time.Time          `orm:"create_time"`
		UpdateTime time.Time          `orm:"update_time"`
		Value      []HandleValueMysql `orm:"value"`
	}

	gtest.C(t, func(t *gtest.T) {
		var h1 = HandleInfoMysql{
			SubPrefix:  "p_",
			Prefix:     "m_",
			HandleName: "name",
			CreateTime: gtime.Now().FormatTo("Y-m-d H:i:s").Time,
			UpdateTime: gtime.Now().FormatTo("Y-m-d H:i:s").Time,
			Value: []HandleValueMysql{
				{
					Index: 10,
					Type:  "t1",
					Data:  []byte("abc"),
				},
				{
					Index: 20,
					Type:  "t2",
					Data:  []byte("def"),
				},
			},
		}
		_, err := db.Model(table).OmitEmptyData().Insert(h1)
		t.AssertNil(err)

		var h2 HandleInfoMysql
		err = db.Model(table).Scan(&h2)
		t.AssertNil(err)

		h1.Id = 1
		t.Assert(h1, h2)
	})
}

// https://github.com/gogf/gf/issues/4495
// Test Tables() returns correct tables using USER_TABLES (for current user)
// and ALL_TABLES (when schema is specified).
func Test_Issue4495_Tables(t *testing.T) {
	table1 := createInitTable()
	table2 := createInitTable()
	defer dropTable(table1)
	defer dropTable(table2)

	// Test 1: Tables() returns tables for current user (using USER_TABLES)
	gtest.C(t, func(t *gtest.T) {
		tables, err := db.Tables(ctx)
		t.AssertNil(err)

		// Should contain our created tables (case-insensitive comparison for DM)
		found1 := false
		found2 := false
		for _, tbl := range tables {
			if gstr.Equal(tbl, table1) {
				found1 = true
			}
			if gstr.Equal(tbl, table2) {
				found2 = true
			}
		}
		t.Assert(found1, true)
		t.Assert(found2, true)
	})

	// Test 2: Tables() with explicit schema parameter (using ALL_TABLES with OWNER filter)
	gtest.C(t, func(t *gtest.T) {
		tables, err := db.Tables(ctx, TestDBName)
		t.AssertNil(err)

		// Should contain our created tables
		found1 := false
		found2 := false
		for _, tbl := range tables {
			if gstr.Equal(tbl, table1) {
				found1 = true
			}
			if gstr.Equal(tbl, table2) {
				found2 = true
			}
		}
		t.Assert(found1, true)
		t.Assert(found2, true)
	})
}

// https://github.com/gogf/gf/issues/4495
// Test TableFields() returns correct field info with proper schema handling.
func Test_Issue4495_TableFields(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	// Test 1: TableFields() without schema uses current schema
	gtest.C(t, func(t *gtest.T) {
		fields, err := db.TableFields(ctx, table)
		t.AssertNil(err)
		t.Assert(len(fields) > 0, true)

		// Verify key fields exist
		_, hasID := fields["ID"]
		_, hasAccountName := fields["ACCOUNT_NAME"]
		t.Assert(hasID, true)
		t.Assert(hasAccountName, true)
	})

	// Test 2: TableFields() with explicit schema parameter
	gtest.C(t, func(t *gtest.T) {
		fields, err := db.TableFields(ctx, table, TestDBName)
		t.AssertNil(err)
		t.Assert(len(fields) > 0, true)

		// Verify key fields exist
		_, hasID := fields["ID"]
		_, hasAccountName := fields["ACCOUNT_NAME"]
		t.Assert(hasID, true)
		t.Assert(hasAccountName, true)
	})

	// Test 3: TableFields() with Schema() method
	gtest.C(t, func(t *gtest.T) {
		fields, err := db.Schema(TestDBName).TableFields(ctx, table)
		t.AssertNil(err)
		t.Assert(len(fields) > 0, true)

		// Verify ID is primary key
		idField, hasID := fields["ID"]
		t.Assert(hasID, true)
		t.Assert(idField.Key, "PRI")
	})
}

// https://github.com/gogf/gf/issues/4495
// Test cache isolation for different schemas.
func Test_Issue4495_CacheIsolation(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Get tables using default schema
		tables1, err := db.Tables(ctx)
		t.AssertNil(err)

		// Get tables using explicit schema
		tables2, err := db.Schema(TestDBName).Tables(ctx)
		t.AssertNil(err)

		// Both should contain the created table
		found1 := false
		found2 := false
		for _, tbl := range tables1 {
			if gstr.Equal(tbl, table) {
				found1 = true
				break
			}
		}
		for _, tbl := range tables2 {
			if gstr.Equal(tbl, table) {
				found2 = true
				break
			}
		}
		t.Assert(found1, true)
		t.Assert(found2, true)
	})
}

// Test_MultilineSQLStatement tests that multi-line SQL statements are properly supported.
// This test verifies that newlines and tabs in SQL queries are preserved,
// which is essential for readability and proper SQL statement handling.
func Test_MultilineSQLStatement(t *testing.T) {
	table := "A_tables"
	createInitTable(table)
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Test multi-line SELECT statement with newlines and indentation
		multilineSql := `
		SELECT 
			id,
			account_name,
			attr_index
		FROM A_tables
		WHERE id = ?
		AND account_name = ?
		`
		result, err := db.GetAll(ctx, multilineSql, 1, "name_1")
		t.AssertNil(err)
		t.Assert(len(result), 1)
		t.Assert(result[0]["ID"].Int(), 1)
		t.Assert(result[0]["ACCOUNT_NAME"].String(), "name_1")
	})

	gtest.C(t, func(t *gtest.T) {
		// Test multi-line SELECT with tabs
		multilineSql := `SELECT
			id,
			account_name,
			attr_index
		FROM A_tables
		WHERE id IN (?, ?)
		ORDER BY id`
		result, err := db.GetAll(ctx, multilineSql, 2, 3)
		t.AssertNil(err)
		t.Assert(len(result), 2)
		t.Assert(result[0]["ID"].Int(), 2)
		t.Assert(result[1]["ID"].Int(), 3)
	})

	gtest.C(t, func(t *gtest.T) {
		// Test that newlines in values don't cause issues
		multilineSql := `
		SELECT * 
		FROM A_tables 
		WHERE id = ?`
		result, err := db.GetAll(ctx, multilineSql, 5)
		t.AssertNil(err)
		t.Assert(len(result), 1)
		t.Assert(result[0]["ID"].Int(), 5)
		t.Assert(result[0]["ACCOUNT_NAME"].String(), "name_5")
	})

	gtest.C(t, func(t *gtest.T) {
		// Test multi-line INSERT with newlines
		multilineSql := `
		INSERT INTO A_tables
		(ID, ACCOUNT_NAME, ATTR_INDEX, CREATED_TIME, UPDATED_TIME)
		VALUES
		(?, ?, ?, ?, ?)`
		_, err := db.Exec(ctx, multilineSql, 1001, "multiline_insert_test", 100, gtime.Now(), gtime.Now())
		t.AssertNil(err)

		// Verify the insert worked
		result, err := db.GetAll(ctx, "SELECT * FROM A_tables WHERE ID = ?", 1001)
		t.AssertNil(err)
		t.Assert(len(result), 1)
		t.Assert(result[0]["ACCOUNT_NAME"].String(), "multiline_insert_test")
	})

	gtest.C(t, func(t *gtest.T) {
		// Test multi-line UPDATE with newlines
		multilineSql := `
		UPDATE A_tables
		SET account_name = ?,
			attr_index = ?
		WHERE id = ?`
		_, err := db.Exec(ctx, multilineSql, "updated_multiline", 999, 1)
		t.AssertNil(err)

		// Verify the update worked
		result, err := db.GetAll(ctx, "SELECT * FROM A_tables WHERE ID = ?", 1)
		t.AssertNil(err)
		t.Assert(len(result), 1)
		t.Assert(result[0]["ACCOUNT_NAME"].String(), "updated_multiline")
	})
}
