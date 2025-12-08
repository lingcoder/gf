// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package dm_test

import (
	"testing"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/test/gtest"
)

// Test_Model_Insert_Returning tests the RETURNING clause for INSERT operations.
// DaMeng database supports RETURNING clause similar to PostgreSQL/Oracle.
func Test_Model_Insert_Returning(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Test basic RETURNING with specific fields
		result, err := db.Schema(TestDBName).Model(table).Data(g.Map{
			"id":           1,
			"account_name": "user1",
			"pwd_reset":    0,
			"created_time": gtime.Now(),
		}).Returning("id", "account_name").Insert()
		t.AssertNil(err)

		n, _ := result.RowsAffected()
		t.Assert(n, 1)

		// Test RETURNING all fields
		result, err = db.Schema(TestDBName).Model(table).Data(g.Map{
			"id":           2,
			"account_name": "user2",
			"pwd_reset":    0,
			"created_time": gtime.Now(),
		}).ReturningAll().Insert()
		t.AssertNil(err)

		n, _ = result.RowsAffected()
		t.Assert(n, 1)
	})
}

// Test_Model_Insert_Returning_Batch tests RETURNING clause with batch insert.
func Test_Model_Insert_Returning_Batch(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Batch insert with RETURNING
		result, err := db.Schema(TestDBName).Model(table).Data(g.Slice{
			g.Map{
				"id":           1,
				"account_name": "batch1",
				"pwd_reset":    0,
				"created_time": gtime.Now(),
			},
			g.Map{
				"id":           2,
				"account_name": "batch2",
				"pwd_reset":    0,
				"created_time": gtime.Now(),
			},
			g.Map{
				"id":           3,
				"account_name": "batch3",
				"pwd_reset":    0,
				"created_time": gtime.Now(),
			},
		}).Returning("id", "account_name").Insert()

		t.AssertNil(err)
		n, _ := result.RowsAffected()
		t.Assert(n, 3)
	})
}

// Test_Model_Update_Returning tests the RETURNING clause for UPDATE operations.
func Test_Model_Update_Returning(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Update with RETURNING
		result, err := db.Schema(TestDBName).Model(table).Data(g.Map{
			"account_name": "Updated Name",
		}).Where("id", 1).Returning("id", "account_name").Update()

		t.AssertNil(err)
		n, _ := result.RowsAffected()
		t.Assert(n, 1)

		// Verify the update
		value, err := db.Schema(TestDBName).Model(table).Where("id", 1).Value("account_name")
		t.AssertNil(err)
		t.Assert(value.String(), "Updated Name")
	})
}

// Test_Model_Update_Returning_All tests RETURNING * for UPDATE operations.
func Test_Model_Update_Returning_All(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Update with RETURNING *
		result, err := db.Schema(TestDBName).Model(table).Data(g.Map{
			"pwd_reset": 1,
		}).Where("id", 2).ReturningAll().Update()

		t.AssertNil(err)
		n, _ := result.RowsAffected()
		t.Assert(n, 1)
	})
}

// Test_Model_Delete_Returning tests the RETURNING clause for DELETE operations.
func Test_Model_Delete_Returning(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Get initial count
		count, err := db.Schema(TestDBName).Model(table).Count()
		t.AssertNil(err)

		// Delete with RETURNING
		result, err := db.Schema(TestDBName).Model(table).Where("id", 1).Returning("id", "account_name").Delete()
		t.AssertNil(err)

		n, _ := result.RowsAffected()
		t.Assert(n, 1)

		// Verify deletion
		newCount, err := db.Schema(TestDBName).Model(table).Count()
		t.AssertNil(err)
		t.Assert(newCount, count-1)

		// Verify record is deleted
		record, err := db.Schema(TestDBName).Model(table).Where("id", 1).One()
		t.AssertNil(err)
		t.Assert(record.IsEmpty(), true)
	})
}

// Test_Model_Delete_Returning_All tests RETURNING * for DELETE operations.
func Test_Model_Delete_Returning_All(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Delete with RETURNING *
		result, err := db.Schema(TestDBName).Model(table).Where("id", 3).ReturningAll().Delete()
		t.AssertNil(err)

		n, _ := result.RowsAffected()
		t.Assert(n, 1)
	})
}

// Test_Model_Returning_Multiple tests multiple RETURNING operations in sequence.
func Test_Model_Returning_Multiple(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Multiple inserts with RETURNING
		for i := 1; i <= 5; i++ {
			result, err := db.Schema(TestDBName).Model(table).Data(g.Map{
				"id":           i,
				"account_name": g.NewVar(i).String(),
				"pwd_reset":    0,
				"created_time": gtime.Now(),
			}).Returning("id").Insert()

			t.AssertNil(err)
			n, _ := result.RowsAffected()
			t.Assert(n, 1)
		}

		// Verify all records inserted
		count, err := db.Schema(TestDBName).Model(table).Count()
		t.AssertNil(err)
		t.Assert(count, 5)
	})
}

// Test_Model_Save_Returning tests RETURNING with Save (UPSERT) operations.
// DM uses MERGE statement for upsert operations.
func Test_Model_Save_Returning(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// First save (INSERT)
		result, err := db.Schema(TestDBName).Model(table).Data(g.Map{
			"id":           100,
			"account_name": "save_user",
			"pwd_reset":    0,
			"created_time": gtime.Now(),
		}).OnConflict("id").Save()

		t.AssertNil(err)
		n, _ := result.RowsAffected()
		t.Assert(n, 1)

		// Second save (UPDATE on conflict)
		result, err = db.Schema(TestDBName).Model(table).Data(g.Map{
			"id":           100,
			"account_name": "updated_save_user",
			"pwd_reset":    1,
			"created_time": gtime.Now(),
		}).OnConflict("id").Save()

		t.AssertNil(err)
		n, _ = result.RowsAffected()
		t.Assert(n, 1)

		// Verify the upsert worked
		value, err := db.Schema(TestDBName).Model(table).Where("id", 100).Value("account_name")
		t.AssertNil(err)
		t.Assert(value.String(), "updated_save_user")
	})
}
