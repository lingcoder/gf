// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package pgsql_test

import (
	"context"
	"testing"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
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

// Test_Model_Insert_Returning_GetRecords tests getting actual RETURNING data.
func Test_Model_Insert_Returning_GetRecords(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Insert with RETURNING and get the records
		result, err := db.Model(table).Data(g.Map{
			"passport":    "returning_user",
			"password":    "pass123",
			"nickname":    "Returning User",
			"create_time": gtime.Now().String(),
		}).Returning("id", "passport", "nickname").Insert()

		t.AssertNil(err)
		n, _ := result.RowsAffected()
		t.Assert(n, 1)

		// Try to get RETURNING records using type assertion
		rr, ok := result.(gdb.ReturningResult)
		t.Assert(ok, true)
		t.AssertNE(rr, nil)

		records := rr.GetRecords()
		t.Assert(len(records), 1)
		t.Assert(records[0]["passport"].String(), "returning_user")
		t.Assert(records[0]["nickname"].String(), "Returning User")
		t.AssertGT(records[0]["id"].Int(), 0)
	})
}

// Test_Model_Insert_Returning_GetRecords_Batch tests getting RETURNING data for batch insert.
func Test_Model_Insert_Returning_GetRecords_Batch(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Batch insert with RETURNING
		result, err := db.Model(table).Data(g.Slice{
			g.Map{
				"passport":    "batch_ret_1",
				"password":    "pass1",
				"nickname":    "Batch Ret One",
				"create_time": gtime.Now().String(),
			},
			g.Map{
				"passport":    "batch_ret_2",
				"password":    "pass2",
				"nickname":    "Batch Ret Two",
				"create_time": gtime.Now().String(),
			},
		}).ReturningAll().Insert()

		t.AssertNil(err)
		n, _ := result.RowsAffected()
		t.Assert(n, 2)

		// Get RETURNING records
		rr, ok := result.(gdb.ReturningResult)
		t.Assert(ok, true)

		records := rr.GetRecords()
		t.Assert(len(records), 2)
		t.Assert(records[0]["passport"].String(), "batch_ret_1")
		t.Assert(records[1]["passport"].String(), "batch_ret_2")
	})
}

// Test_Model_Update_Returning_GetRecords tests getting RETURNING data for UPDATE.
func Test_Model_Update_Returning_GetRecords(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Update with RETURNING
		result, err := db.Model(table).Data(g.Map{
			"nickname": "Updated Ret Nick",
		}).Where("id", 1).Returning("id", "passport", "nickname").Update()

		t.AssertNil(err)
		n, _ := result.RowsAffected()
		t.Assert(n, 1)

		// Get RETURNING records
		rr, ok := result.(gdb.ReturningResult)
		t.Assert(ok, true)

		records := rr.GetRecords()
		t.Assert(len(records), 1)
		t.Assert(records[0]["id"].Int(), 1)
		t.Assert(records[0]["nickname"].String(), "Updated Ret Nick")
	})
}

// Test_Model_Delete_Returning_GetRecords tests getting RETURNING data for DELETE.
func Test_Model_Delete_Returning_GetRecords(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Get original data first
		original, err := db.Model(table).Where("id", 1).One()
		t.AssertNil(err)

		// Delete with RETURNING
		result, err := db.Model(table).Where("id", 1).Returning("id", "passport", "nickname").Delete()
		t.AssertNil(err)

		n, _ := result.RowsAffected()
		t.Assert(n, 1)

		// Get RETURNING records - should contain the deleted data
		rr, ok := result.(gdb.ReturningResult)
		t.Assert(ok, true)

		records := rr.GetRecords()
		t.Assert(len(records), 1)
		t.Assert(records[0]["id"].Int(), 1)
		t.Assert(records[0]["passport"].String(), original["passport"].String())
		t.Assert(records[0]["nickname"].String(), original["nickname"].String())
	})
}

// Test_Model_InsertAndScan tests InsertAndScan method.
func Test_Model_InsertAndScan(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		type User struct {
			Id         int    `json:"id"`
			Passport   string `json:"passport"`
			Password   string `json:"password"`
			Nickname   string `json:"nickname"`
			CreateTime string `json:"create_time"`
		}

		var user User
		err := db.Model(table).Data(g.Map{
			"passport":    "scan_user",
			"password":    "scan_pass",
			"nickname":    "Scan User",
			"create_time": gtime.Now().String(),
		}).InsertAndScan(&user)

		t.AssertNil(err)
		t.AssertGT(user.Id, 0)
		t.Assert(user.Passport, "scan_user")
		t.Assert(user.Nickname, "Scan User")
	})
}

// Test_Model_InsertAndScan_Batch tests InsertAndScan with batch insert.
func Test_Model_InsertAndScan_Batch(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		type User struct {
			Id         int    `json:"id"`
			Passport   string `json:"passport"`
			Password   string `json:"password"`
			Nickname   string `json:"nickname"`
			CreateTime string `json:"create_time"`
		}

		var users []User
		err := db.Model(table).Data(g.Slice{
			g.Map{
				"passport":    "batch_scan_1",
				"password":    "pass1",
				"nickname":    "Batch Scan One",
				"create_time": gtime.Now().String(),
			},
			g.Map{
				"passport":    "batch_scan_2",
				"password":    "pass2",
				"nickname":    "Batch Scan Two",
				"create_time": gtime.Now().String(),
			},
		}).InsertAndScan(&users)

		t.AssertNil(err)
		t.Assert(len(users), 2)
		t.Assert(users[0].Passport, "batch_scan_1")
		t.Assert(users[1].Passport, "batch_scan_2")
		t.AssertGT(users[0].Id, 0)
		t.AssertGT(users[1].Id, 0)
	})
}

// Test_Model_UpdateAndScan tests UpdateAndScan method.
func Test_Model_UpdateAndScan(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		type User struct {
			Id         int    `json:"id"`
			Passport   string `json:"passport"`
			Password   string `json:"password"`
			Nickname   string `json:"nickname"`
			CreateTime string `json:"create_time"`
		}

		var user User
		err := db.Model(table).Data(g.Map{
			"nickname": "Updated Scan Nick",
		}).Where("id", 1).UpdateAndScan(&user)

		t.AssertNil(err)
		t.Assert(user.Id, 1)
		t.Assert(user.Nickname, "Updated Scan Nick")
	})
}

// Test_Model_DeleteAndScan tests DeleteAndScan method.
func Test_Model_DeleteAndScan(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		type User struct {
			Id         int    `json:"id"`
			Passport   string `json:"passport"`
			Password   string `json:"password"`
			Nickname   string `json:"nickname"`
			CreateTime string `json:"create_time"`
		}

		// Get original data first
		original, err := db.Model(table).Where("id", 1).One()
		t.AssertNil(err)

		var deletedUser User
		err = db.Model(table).Where("id", 1).DeleteAndScan(&deletedUser)

		t.AssertNil(err)
		t.Assert(deletedUser.Id, 1)
		t.Assert(deletedUser.Passport, original["passport"].String())

		// Verify record is deleted
		record, err := db.Model(table).Where("id", 1).One()
		t.AssertNil(err)
		t.Assert(record.IsEmpty(), true)
	})
}

// Test_Model_Transaction_Returning tests RETURNING clause within a transaction.
func Test_Model_Transaction_Returning(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		err := db.Transaction(gctx.New(), func(ctx context.Context, tx gdb.TX) error {
			// Insert with RETURNING in transaction
			result, err := tx.Model(table).Ctx(ctx).Data(g.Map{
				"passport":    "tx_user",
				"password":    "tx_pass",
				"nickname":    "TX User",
				"create_time": gtime.Now().String(),
			}).Returning("id", "passport").Insert()
			if err != nil {
				return err
			}

			// Verify RETURNING data is available
			rr, ok := result.(gdb.ReturningResult)
			if !ok {
				t.Error("Expected ReturningResult interface")
				return nil
			}
			records := rr.GetRecords()
			t.Assert(len(records), 1)
			t.AssertGT(records[0]["id"].Int(), 0)
			t.Assert(records[0]["passport"].String(), "tx_user")

			// Update with RETURNING in transaction
			result, err = tx.Model(table).Ctx(ctx).Data(g.Map{
				"nickname": "TX User Updated",
			}).Where("passport", "tx_user").Returning("id", "nickname").Update()
			if err != nil {
				return err
			}

			rr, ok = result.(gdb.ReturningResult)
			if !ok {
				t.Error("Expected ReturningResult interface for update")
				return nil
			}
			records = rr.GetRecords()
			t.Assert(len(records), 1)
			t.Assert(records[0]["nickname"].String(), "TX User Updated")

			return nil
		})
		t.AssertNil(err)
	})
}

// Test_Model_Update_Returning_Multiple tests UPDATE with RETURNING for multiple rows.
func Test_Model_Update_Returning_Multiple(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Update multiple rows with RETURNING
		result, err := db.Model(table).Data(g.Map{
			"nickname": "Updated Batch",
		}).Where("id <= ?", 3).Returning("id", "passport", "nickname").Update()

		t.AssertNil(err)
		n, _ := result.RowsAffected()
		t.Assert(n, 3)

		// Get RETURNING records
		rr, ok := result.(gdb.ReturningResult)
		t.Assert(ok, true)

		records := rr.GetRecords()
		t.Assert(len(records), 3)
		// Verify all records have the updated nickname
		for _, record := range records {
			t.Assert(record["nickname"].String(), "Updated Batch")
		}
	})
}

// Test_Model_Delete_Returning_Multiple tests DELETE with RETURNING for multiple rows.
func Test_Model_Delete_Returning_Multiple(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// Get original data first
		original, err := db.Model(table).Where("id <= ?", 2).All()
		t.AssertNil(err)
		t.Assert(len(original), 2)

		// Delete multiple rows with RETURNING
		result, err := db.Model(table).Where("id <= ?", 2).Returning("id", "passport", "nickname").Delete()
		t.AssertNil(err)

		n, _ := result.RowsAffected()
		t.Assert(n, 2)

		// Get RETURNING records - should contain the deleted data
		rr, ok := result.(gdb.ReturningResult)
		t.Assert(ok, true)

		records := rr.GetRecords()
		t.Assert(len(records), 2)
	})
}
