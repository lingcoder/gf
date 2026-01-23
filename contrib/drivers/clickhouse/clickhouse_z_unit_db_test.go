// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package clickhouse_test

import (
	"fmt"
	"testing"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/test/gtest"
)

func Test_DB_Ping(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		err1 := db.PingMaster()
		err2 := db.PingSlave()
		t.Assert(err1, nil)
		t.Assert(err2, nil)
	})
}

func Test_DB_Query(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		_, err := db.Query(ctx, "SELECT ?", 1)
		t.AssertNil(err)

		_, err = db.Query(ctx, "SELECT ?+?", 1, 2)
		t.AssertNil(err)

		_, err = db.Query(ctx, "SELECT ?+?", g.Slice{1, 2})
		t.AssertNil(err)

		_, err = db.Query(ctx, "ERROR")
		t.AssertNE(err, nil)
	})
}

func Test_DB_Exec(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		_, err := db.Exec(ctx, fmt.Sprintf("select * from %s ", table))
		t.AssertNil(err)
	})
}

func Test_DB_Insert(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		_, err := db.Insert(ctx, table, g.Map{
			"id":          uint64(1),
			"passport":    "t1",
			"password":    "25d55ad283aa400af464c76d713c07ad",
			"nickname":    "T1",
			"create_time": gtime.Now(),
		})
		t.AssertNil(err)
		answer, err := db.GetAll(ctx, fmt.Sprintf("SELECT * FROM %s WHERE id=?", table), 1)
		t.AssertNil(err)
		t.Assert(len(answer), 1)
		t.Assert(answer[0]["passport"], "t1")
		t.Assert(answer[0]["password"], "25d55ad283aa400af464c76d713c07ad")
		t.Assert(answer[0]["nickname"], "T1")

		// normal map
		_, err = db.Insert(ctx, table, g.Map{
			"id":          uint64(2),
			"passport":    "t2",
			"password":    "25d55ad283aa400af464c76d713c07ad",
			"nickname":    "name_2",
			"create_time": gtime.Now(),
		})
		t.AssertNil(err)

		answer, err = db.GetAll(ctx, fmt.Sprintf("SELECT * FROM %s WHERE id=?", table), 2)
		t.AssertNil(err)
		t.Assert(len(answer), 1)
		t.Assert(answer[0]["passport"], "t2")
		t.Assert(answer[0]["password"], "25d55ad283aa400af464c76d713c07ad")
		t.Assert(answer[0]["nickname"], "name_2")
	})
}

func Test_DB_Save(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		createTable("t_user")
		defer dropTable("t_user")

		i := 10
		data := g.Map{
			"id":          i,
			"passport":    fmt.Sprintf(`t%d`, i),
			"password":    fmt.Sprintf(`p%d`, i),
			"nickname":    fmt.Sprintf(`T%d`, i),
			"create_time": gtime.Now(),
		}
		_, err := db.Save(ctx, "t_user", data, 10)
		gtest.AssertNE(err, nil)
	})
}

func Test_DB_Replace(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		createTable("t_user")
		defer dropTable("t_user")

		i := 10
		data := g.Map{
			"id":          i,
			"passport":    fmt.Sprintf(`t%d`, i),
			"password":    fmt.Sprintf(`p%d`, i),
			"nickname":    fmt.Sprintf(`T%d`, i),
			"create_time": gtime.Now(),
		}
		_, err := db.Replace(ctx, "t_user", data, 10)
		gtest.AssertNE(err, nil)
	})
}

func Test_DB_GetAll(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		result, err := db.GetAll(ctx, fmt.Sprintf("SELECT * FROM %s WHERE id=?", table), 1)
		t.AssertNil(err)
		t.Assert(len(result), 1)
		t.Assert(result[0]["id"].Int(), 1)
	})
	gtest.C(t, func(t *gtest.T) {
		result, err := db.GetAll(ctx, fmt.Sprintf("SELECT * FROM %s WHERE id=?", table), g.Slice{1})
		t.AssertNil(err)
		t.Assert(len(result), 1)
		t.Assert(result[0]["id"].Int(), 1)
	})
	gtest.C(t, func(t *gtest.T) {
		result, err := db.GetAll(ctx, fmt.Sprintf("SELECT * FROM %s WHERE id in(?)", table), g.Slice{1, 2, 3})
		t.AssertNil(err)
		t.Assert(len(result), 3)
		t.Assert(result[0]["id"].Int(), 1)
		t.Assert(result[1]["id"].Int(), 2)
		t.Assert(result[2]["id"].Int(), 3)
	})
	gtest.C(t, func(t *gtest.T) {
		result, err := db.GetAll(ctx, fmt.Sprintf("SELECT * FROM %s WHERE id in(?,?,?)", table), g.Slice{1, 2, 3})
		t.AssertNil(err)
		t.Assert(len(result), 3)
		t.Assert(result[0]["id"].Int(), 1)
		t.Assert(result[1]["id"].Int(), 2)
		t.Assert(result[2]["id"].Int(), 3)
	})
	gtest.C(t, func(t *gtest.T) {
		result, err := db.GetAll(ctx, fmt.Sprintf("SELECT * FROM %s WHERE id in(?,?,?)", table), g.Slice{1, 2, 3}...)
		t.AssertNil(err)
		t.Assert(len(result), 3)
		t.Assert(result[0]["id"].Int(), 1)
		t.Assert(result[1]["id"].Int(), 2)
		t.Assert(result[2]["id"].Int(), 3)
	})
	gtest.C(t, func(t *gtest.T) {
		result, err := db.GetAll(ctx, fmt.Sprintf("SELECT * FROM %s WHERE id>=? AND id <=?", table), g.Slice{1, 3})
		t.AssertNil(err)
		t.Assert(len(result), 3)
		t.Assert(result[0]["id"].Int(), 1)
		t.Assert(result[1]["id"].Int(), 2)
		t.Assert(result[2]["id"].Int(), 3)
	})
}

func Test_DB_GetOne(t *testing.T) {
	table := createTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		type User struct {
			Id         uint64
			Passport   string
			Password   string
			Nickname   string
			CreateTime *gtime.Time
		}
		data := User{
			Id:         uint64(1),
			Passport:   "user_1",
			Password:   "pass_1",
			Nickname:   "name_1",
			CreateTime: gtime.Now(),
		}
		_, err := db.Insert(ctx, table, data)
		t.AssertNil(err)

		one, err := db.GetOne(ctx, fmt.Sprintf("SELECT * FROM %s WHERE id=?", table), 1)
		t.AssertNil(err)
		t.Assert(one["passport"], data.Passport)
		t.Assert(one["create_time"], data.CreateTime)
		t.Assert(one["nickname"], data.Nickname)
	})
}

func Test_DB_GetValue(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)
	gtest.C(t, func(t *gtest.T) {
		value, err := db.GetValue(ctx, fmt.Sprintf("SELECT id FROM %s WHERE passport=?", table), "user_3")
		t.AssertNil(err)
		t.Assert(value.Int(), 3)
	})
}

func Test_DB_GetCount(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)
	gtest.C(t, func(t *gtest.T) {
		count, err := db.GetCount(ctx, fmt.Sprintf("SELECT * FROM %s", table))
		t.AssertNil(err)
		t.Assert(count, TableSize)
	})
}

func Test_DB_GetArray(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)
	gtest.C(t, func(t *gtest.T) {
		array, err := db.GetArray(ctx, fmt.Sprintf("SELECT password FROM %s", table))
		t.AssertNil(err)
		arrays := make([]string, 0)
		for i := 1; i <= TableSize; i++ {
			arrays = append(arrays, fmt.Sprintf(`pass_%d`, i))
		}
		t.Assert(array, arrays)
	})
}

func Test_DB_GetScan(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)
	gtest.C(t, func(t *gtest.T) {
		type User struct {
			Id         int
			Passport   string
			Password   string
			NickName   string
			CreateTime gtime.Time
		}
		user := new(User)
		err := db.GetScan(ctx, user, fmt.Sprintf("SELECT * FROM %s WHERE id=?", table), 3)
		t.AssertNil(err)
		t.Assert(user.NickName, "name_3")
	})
}

func Test_DB_Update(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		_, err := db.Update(ctx, table, "password='123456'", "id=3")
		t.AssertNE(err, nil)

		one, err := db.Model(table).Where("id", 3).One()
		t.AssertNil(err)
		t.AssertNE(one["password"].String(), "123456")

		t.Assert(one["id"].Int(), 3)
		t.Assert(one["passport"].String(), "user_3")
		t.Assert(one["nickname"].String(), "name_3")
	})
}

func Test_DB_Delete(t *testing.T) {
	table := createInitTable()
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		//db.SetDebug(true)
		count, err := db.Model(table).Ctx(ctx).Count()
		t.AssertNil(err)
		t.Assert(count, 10)

		result, err := db.Delete(ctx, table, "id>3")
		t.AssertNil(err)
		t.AssertNil(result)

		count, err = db.Model(table).Ctx(ctx).Count()
		t.AssertNil(err)
		t.Assert(count, 3)
	})
}

func Test_DB_Tables(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		tables := []string{"t_user1", "pop", "haha"}

		for _, v := range tables {
			createTable(v)
		}

		defer dropTable(tables...)

		result, err := db.Tables(ctx)
		gtest.AssertNil(err)

		for i := 0; i < len(tables); i++ {
			find := false
			for j := 0; j < len(result); j++ {
				if tables[i] == result[j] {
					find = true
					break
				}
			}
			gtest.AssertEQ(find, true)
		}
	})
}

func Test_DB_TableFields(t *testing.T) {
	table := createInitTable("user")
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		field, err := db.TableFields(ctx, "user")
		gtest.AssertNil(err)
		gtest.AssertEQ(len(field), 5)
		gtest.AssertNQ(field, nil)
	})
}

// https://github.com/gogf/gf/issues/4495
// Test TableFields() correctly filters by database name.
func Test_Issue4495_TableFields_DatabaseFilter(t *testing.T) {
	table := createInitTable("issue4495_test")
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// TableFields should return fields only for tables in the configured database
		fields, err := db.TableFields(ctx, "issue4495_test")
		gtest.AssertNil(err)
		gtest.AssertEQ(len(fields), 5)

		// Verify field names
		_, hasId := fields["id"]
		_, hasPassport := fields["passport"]
		_, hasPassword := fields["password"]
		_, hasNickname := fields["nickname"]
		_, hasCreateTime := fields["create_time"]
		gtest.AssertEQ(hasId, true)
		gtest.AssertEQ(hasPassport, true)
		gtest.AssertEQ(hasPassword, true)
		gtest.AssertEQ(hasNickname, true)
		gtest.AssertEQ(hasCreateTime, true)
	})
}

// https://github.com/gogf/gf/issues/4495
// Test Tables() returns tables from the configured database.
func Test_Issue4495_Tables_DatabaseFilter(t *testing.T) {
	table1 := createTable("issue4495_t1")
	table2 := createTable("issue4495_t2")
	defer dropTable(table1)
	defer dropTable(table2)

	gtest.C(t, func(t *gtest.T) {
		tables, err := db.Tables(ctx)
		gtest.AssertNil(err)

		// Should contain our created tables
		found1 := false
		found2 := false
		for _, tbl := range tables {
			if tbl == "issue4495_t1" {
				found1 = true
			}
			if tbl == "issue4495_t2" {
				found2 = true
			}
		}
		gtest.AssertEQ(found1, true)
		gtest.AssertEQ(found2, true)
	})
}

// https://github.com/gogf/gf/issues/4495
// Test cache isolation - ensure cache keys include database name.
func Test_Issue4495_CacheIsolation(t *testing.T) {
	table := createInitTable("issue4495_cache")
	defer dropTable(table)

	gtest.C(t, func(t *gtest.T) {
		// First call should cache the result
		tables1, err := db.Tables(ctx)
		gtest.AssertNil(err)

		// Second call should use cache (same database)
		tables2, err := db.Tables(ctx)
		gtest.AssertNil(err)

		// Results should be consistent
		gtest.AssertEQ(len(tables1), len(tables2))

		// Both should contain our table
		found := false
		for _, tbl := range tables1 {
			if tbl == "issue4495_cache" {
				found = true
				break
			}
		}
		gtest.AssertEQ(found, true)
	})
}
