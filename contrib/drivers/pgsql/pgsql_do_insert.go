// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package pgsql

import (
	"context"
	"database/sql"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
)

// DoInsert inserts or updates data for given table.
func (d *Driver) DoInsert(ctx context.Context, link gdb.Link, table string, list gdb.List, option gdb.DoInsertOption) (result sql.Result, err error) {
	switch option.InsertOption {
	case gdb.InsertOptionReplace:
		return nil, gerror.NewCode(
			gcode.CodeNotSupported,
			`Replace operation is not supported by pgsql driver`,
		)

	case gdb.InsertOptionDefault:
		// Get primary key field for automatic RETURNING if no explicit RETURNING is set
		if len(option.Returning) == 0 {
			tableFields, err := d.GetCore().GetDB().TableFields(ctx, table)
			if err == nil {
				for _, field := range tableFields {
					if field.Key == "pri" {
						pkField := *field
						ctx = context.WithValue(ctx, internalPrimaryKeyInCtx, pkField)
						break
					}
				}
			}
		}
	}

	// If RETURNING clause is specified, pass it through context
	if len(option.Returning) > 0 {
		ctx = gdb.InjectReturning(ctx, option.Returning)
	}

	return d.Core.DoInsert(ctx, link, table, list, option)
}
