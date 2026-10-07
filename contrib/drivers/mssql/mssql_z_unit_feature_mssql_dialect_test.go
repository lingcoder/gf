// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package mssql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/test/gtest"
)

func Test_Mssql_TableFields_View(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)
	view := table + "_view"
	_, err := db.Exec(ctx, fmt.Sprintf("CREATE VIEW [%s] AS SELECT ID, PASSPORT, NICKNAME FROM [%s]", view, table))
	gtest.AssertNil(err)
	defer db.Exec(ctx, fmt.Sprintf("DROP VIEW [%s]", view))

	gtest.C(t, func(t *gtest.T) {
		fields, err := db.TableFields(ctx, view)
		t.AssertNil(err)
		t.Assert(len(fields), 3)
		t.Assert(fields["ID"].Index, 0)
		t.Assert(fields["NICKNAME"].Type, "varchar(45)")
	})

	gtest.C(t, func(t *gtest.T) {
		result, err := db.Model(view).Data(g.Map{"nickname": "view_name"}).Where("id", 3).Update()
		t.AssertNil(err)
		n, err := result.RowsAffected()
		t.AssertNil(err)
		t.Assert(n, 1)

		value, err := db.Model(table).Where("id", 3).Value("NICKNAME")
		t.AssertNil(err)
		t.Assert(value, "view_name")
	})
}

func Test_Mssql_TableFields_NotExist(t *testing.T) {
	table := fmt.Sprintf("user_%d", gtime.TimestampNano())
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		fields, err := db.TableFields(ctx, table)
		t.AssertNE(err, nil)
		t.Assert(len(fields), 0)

		_, err = db.Model(table).Data(g.Map{"id": 1, "passport": "p1"}).Insert()
		t.AssertNE(err, nil)
	})

	gtest.C(t, func(t *gtest.T) {
		createTable(table)

		fields, err := db.TableFields(ctx, table)
		t.AssertNil(err)
		t.Assert(len(fields), 7)

		_, err = db.Model(table).Data(g.Map{"id": 1, "passport": "p1"}).Insert()
		t.AssertNil(err)
		value, err := db.Model(table).Where("id", 1).Value("PASSPORT")
		t.AssertNil(err)
		t.Assert(value, "p1")
	})
}

func Test_Mssql_Timezone_Extra(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		value, err := db.Model(table).Where("id", 1).Value("CREATE_TIME")
		t.AssertNil(err)
		t.Assert(value.GTime().Location().String(), time.Local.String())
		t.Assert(value.GTime().Format("Y-m-d H:i:s"), CreateTime)
	})

	gtest.C(t, func(t *gtest.T) {
		node := *db.GetConfig()
		node.Extra = "timezone=Asia/Tokyo"
		tokyoDb, err := gdb.New(node)
		t.AssertNil(err)
		defer tokyoDb.Close(ctx)

		value, err := tokyoDb.Model(table).Where("id", 1).Value("CREATE_TIME")
		t.AssertNil(err)
		t.Assert(value.GTime().Location().String(), "Asia/Tokyo")
		t.Assert(value.GTime().Format("Y-m-d H:i:s"), CreateTime)
	})
}

func Test_Mssql_Lock_TableHint(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		for lock, hint := range map[string]string{
			gdb.LockForUpdate:           "WITH (UPDLOCK, ROWLOCK)",
			gdb.LockForUpdateNowait:     "WITH (UPDLOCK, ROWLOCK, NOWAIT)",
			gdb.LockForUpdateSkipLocked: "WITH (UPDLOCK, ROWLOCK, READPAST)",
			gdb.LockInShareMode:         "WITH (REPEATABLEREAD, ROWLOCK)",
			gdb.LockForShare:            "WITH (REPEATABLEREAD, ROWLOCK)",
			gdb.LockForShareNowait:      "WITH (REPEATABLEREAD, ROWLOCK, NOWAIT)",
			gdb.LockForShareSkipLocked:  "WITH (REPEATABLEREAD, ROWLOCK, READPAST)",
			gdb.LockWithUpdLockHoldLock: gdb.LockWithUpdLockHoldLock,
			"for update":                "WITH (UPDLOCK, ROWLOCK)",
		} {
			err := db.Transaction(ctx, func(ctx context.Context, tx gdb.TX) error {
				sql, err := gdb.CatchSQL(ctx, func(ctx context.Context) error {
					one, err := tx.Model(table).Ctx(ctx).Lock(lock).Where("id", 1).One()
					t.Assert(one["PASSPORT"], "user_1")
					return err
				})
				t.Assert(len(sql), 1)
				unquoted := strings.ReplaceAll(sql[0], `"`, "")
				t.Assert(strings.Contains(unquoted, table+" "+hint+" WHERE"), true)
				return err
			})
			t.AssertNil(err)
		}
	})

	gtest.C(t, func(t *gtest.T) {
		err := db.Transaction(ctx, func(ctx context.Context, tx gdb.TX) error {
			sql, err := gdb.CatchSQL(ctx, func(ctx context.Context) error {
				all, err := tx.Model(table, "a").Ctx(ctx).
					LeftJoin(table, "b", "a.ID=b.ID").
					Fields("a.ID", "b.PASSPORT").
					LockUpdate().
					Where("a.ID", g.Slice{1, 2}).
					Order("a.ID").
					All()
				t.Assert(len(all), 2)
				t.Assert(all[1]["PASSPORT"], "user_2")
				return err
			})
			t.AssertGT(len(sql), 0)
			hintIndex := strings.Index(sql[len(sql)-1], "WITH (UPDLOCK, ROWLOCK)")
			joinIndex := strings.Index(sql[len(sql)-1], "LEFT JOIN")
			t.AssertGT(hintIndex, 0)
			t.AssertLT(hintIndex, joinIndex)
			return err
		})
		t.AssertNil(err)
	})
}
