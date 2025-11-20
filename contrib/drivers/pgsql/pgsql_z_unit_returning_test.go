// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package pgsql_test

import (
	"testing"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/test/gtest"
)

// Test_Model_Insert_Returning tests the RETURNING clause for INSERT operations.
func Test_Model_Insert_Returning(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Test basic RETURNING with specific fields
		result, err := db.Model(table).Data(g.Map{
			"passport":    "user1",
			"password":    "pass1",
			"nickname":    "User One",
			"create_time": gtime.Now().String(),
		}).Returning("id", "passport").Insert()
		t.AssertNil(err)

		n, _ := result.RowsAffected()
		t.Assert(n, 1)

		// Test RETURNING all fields
		result, err = db.Model(table).Data(g.Map{
			"passport":    "user2",
			"password":    "pass2",
			"nickname":    "User Two",
			"create_time": gtime.Now().String(),
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
		result, err := db.Model(table).Data(g.Slice{
			g.Map{
				"passport":    "batch1",
				"password":    "pass1",
				"nickname":    "Batch One",
				"create_time": gtime.Now().String(),
			},
			g.Map{
				"passport":    "batch2",
				"password":    "pass2",
				"nickname":    "Batch Two",
				"create_time": gtime.Now().String(),
			},
			g.Map{
				"passport":    "batch3",
				"password":    "pass3",
				"nickname":    "Batch Three",
				"create_time": gtime.Now().String(),
			},
		}).Returning("id", "passport").Insert()

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
		result, err := db.Model(table).Data(g.Map{
			"nickname": "Updated Nickname",
		}).Where("id", 1).Returning("id", "nickname", "passport").Update()

		t.AssertNil(err)
		n, _ := result.RowsAffected()
		t.Assert(n, 1)

		// Verify the update
		value, err := db.Model(table).Where("id", 1).Value("nickname")
		t.AssertNil(err)
		t.Assert(value.String(), "Updated Nickname")
	})
}

// Test_Model_Update_Returning_All tests RETURNING * for UPDATE operations.
func Test_Model_Update_Returning_All(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Update with RETURNING *
		result, err := db.Model(table).Data(g.Map{
			"password": "newpassword",
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
		count, err := db.Model(table).Count()
		t.AssertNil(err)

		// Delete with RETURNING
		result, err := db.Model(table).Where("id", 1).Returning("id", "passport", "nickname").Delete()
		t.AssertNil(err)

		n, _ := result.RowsAffected()
		t.Assert(n, 1)

		// Verify deletion
		newCount, err := db.Model(table).Count()
		t.AssertNil(err)
		t.Assert(newCount, count-1)

		// Verify record is deleted
		record, err := db.Model(table).Where("id", 1).One()
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
		result, err := db.Model(table).Where("id", 3).ReturningAll().Delete()
		t.AssertNil(err)

		n, _ := result.RowsAffected()
		t.Assert(n, 1)
	})
}

// Test_Model_Save_Returning tests RETURNING with Save (UPSERT) operations.
func Test_Model_Save_Returning(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// First save (INSERT)
		result, err := db.Model(table).Data(g.Map{
			"id":          100,
			"passport":    "save_user",
			"password":    "pass",
			"nickname":    "Save User",
			"create_time": gtime.Now().String(),
		}).OnConflict("id").Returning("id", "passport").Save()

		t.AssertNil(err)
		n, _ := result.RowsAffected()
		t.Assert(n, 1)

		// Second save (UPDATE on conflict)
		result, err = db.Model(table).Data(g.Map{
			"id":          100,
			"passport":    "save_user",
			"password":    "newpass",
			"nickname":    "Updated Save User",
			"create_time": gtime.Now().String(),
		}).OnConflict("id").Returning("id", "nickname").Save()

		t.AssertNil(err)
		n, _ = result.RowsAffected()
		t.Assert(n, 1)

		// Verify the upsert worked
		value, err := db.Model(table).Where("id", 100).Value("nickname")
		t.AssertNil(err)
		t.Assert(value.String(), "Updated Save User")
	})
}

// Test_Model_Returning_WithFields tests RETURNING with field selection.
func Test_Model_Returning_WithFields(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Insert with RETURNING specific fields
		result, err := db.Model(table).
			Fields("passport", "password", "nickname", "create_time").
			Data(g.Map{
				"passport":    "field_user",
				"password":    "pass",
				"nickname":    "Field User",
				"create_time": gtime.Now().String(),
				"extra":       "should_be_ignored",
			}).
			Returning("id", "passport").
			Insert()

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
			result, err := db.Model(table).Data(g.Map{
				"passport":    g.NewVar(i).String(),
				"password":    "pass",
				"nickname":    g.NewVar(i).String(),
				"create_time": gtime.Now().String(),
			}).Returning("id").Insert()

			t.AssertNil(err)
			n, _ := result.RowsAffected()
			t.Assert(n, 1)
		}

		// Verify all records inserted
		count, err := db.Model(table).Count()
		t.AssertNil(err)
		t.Assert(count, 5)
	})
}
