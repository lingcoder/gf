// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package mssql

import (
	"testing"

	"github.com/gogf/gf/v2/test/gtest"
)

// Test_RewriteQuery_LockClause asserts the table hint that rewriteQuery makes of the lock clause
// the core appends to a query, placed after the first table and its alias.
func Test_RewriteQuery_LockClause(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		var cases = []struct {
			sql    string
			expect string
		}{
			{
				"SELECT * FROM t WHERE id=@p1 FOR UPDATE",
				"SELECT * FROM t WITH (UPDLOCK, ROWLOCK) WHERE id=@p1",
			},
			{
				"SELECT * FROM t WHERE id=@p1 for update nowait",
				"SELECT * FROM t WITH (UPDLOCK, ROWLOCK, NOWAIT) WHERE id=@p1",
			},
			{
				"SELECT * FROM t LOCK IN SHARE MODE",
				"SELECT * FROM t WITH (REPEATABLEREAD, ROWLOCK)",
			},
			{
				"SELECT * FROM [dbo].[t] AS a LEFT JOIN u AS b ON (a.id=b.id) WHERE a.id=@p1 FOR SHARE SKIP LOCKED",
				"SELECT * FROM [dbo].[t] AS a WITH (REPEATABLEREAD, ROWLOCK, READPAST) LEFT JOIN u AS b ON (a.id=b.id) WHERE a.id=@p1",
			},
			{
				"SELECT * FROM t a WHERE a.id=@p1 WITH (UPDLOCK, HOLDLOCK)",
				"SELECT * FROM t a WITH (UPDLOCK, HOLDLOCK) WHERE a.id=@p1",
			},
			{
				"SELECT COUNT(1) FROM (SELECT COUNT(1) AS count_value FROM t GROUP BY name FOR UPDATE) count_alias",
				"SELECT COUNT(1) FROM (SELECT COUNT(1) AS count_value FROM t WITH (UPDLOCK, ROWLOCK) GROUP BY name) count_alias",
			},
			{
				"SELECT * FROM t ORDER BY id LIMIT 2 FOR UPDATE",
				"SELECT TOP 2 * FROM t WITH (UPDLOCK, ROWLOCK) ORDER BY id",
			},
			{
				"SELECT 'FOR UPDATE' FROM t",
				"SELECT 'FOR UPDATE' FROM t",
			},
			{
				"SELECT 1 FOR UPDATE",
				"SELECT 1 FOR UPDATE",
			},
		}
		for _, c := range cases {
			t.Assert(rewriteQuery(c.sql), c.expect)
		}
	})
}

// Test_RewriteSavePoint asserts the savepoint statements of the core that rewriteSavePoint
// turns into SQL Server's syntax, and the release that isReleaseSavePoint keeps from running.
func Test_RewriteSavePoint(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		t.Assert(rewriteSavePoint("SAVEPOINT transaction1"), "SAVE TRANSACTION transaction1")
		t.Assert(rewriteSavePoint(`SAVEPOINT "p"`), `SAVE TRANSACTION "p"`)
		t.Assert(rewriteSavePoint("ROLLBACK TO SAVEPOINT transaction1"), "ROLLBACK TRANSACTION transaction1")
		t.Assert(rewriteSavePoint("SELECT 'SAVEPOINT x'"), "SELECT 'SAVEPOINT x'")
		t.Assert(isReleaseSavePoint("RELEASE SAVEPOINT transaction1"), true)
		t.Assert(isReleaseSavePoint("SAVEPOINT transaction1"), false)
	})
}
