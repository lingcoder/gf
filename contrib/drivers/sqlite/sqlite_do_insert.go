// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package sqlite

import (
	"context"
	"database/sql"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/os/gctx"
)

const (
	internalReturningInCtx gctx.StrKey = "returning_fields"
)

// DoInsert inserts or updates data for given table.
// SQLite supports RETURNING clause since version 3.35.0 (2021-03-12).
func (d *Driver) DoInsert(ctx context.Context, link gdb.Link, table string, list gdb.List, option gdb.DoInsertOption) (result sql.Result, err error) {
	// If RETURNING clause is specified, pass it through context
	if len(option.Returning) > 0 {
		ctx = context.WithValue(ctx, internalReturningInCtx, option.Returning)
	}

	return d.Core.DoInsert(ctx, link, table, list, option)
}
