// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/text/gstr"
)

// DoExec commits the sql string and its arguments to underlying driver
// through given link object and returns the execution result.
// It supports RETURNING clause for SQLite 3.35.0+ (2021-03-12).
func (d *Driver) DoExec(ctx context.Context, link gdb.Link, sql string, args ...any) (result sql.Result, err error) {
	// Check if user specified RETURNING fields (from context or DoInsertOption)
	if returningFields := gdb.GetReturningFromCtx(ctx); len(returningFields) > 0 {
		// Add RETURNING clause to SQL
		sql += " " + buildReturningClause(returningFields)
	}

	// Use default DoExec
	return d.Core.DoExec(ctx, link, sql, args...)
}

// buildReturningClause builds the RETURNING clause for SQLite.
// SQLite 3.35.0+ supports RETURNING clause with the following syntax:
// - Simple field names: RETURNING id, name
// - Wildcard: RETURNING *
func buildReturningClause(fields []string) string {
	if len(fields) == 0 {
		return ""
	}

	var quotedFields []string
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "*" {
			quotedFields = append(quotedFields, "*")
		} else {
			quotedFields = append(quotedFields, fmt.Sprintf("`%s`", field))
		}
	}

	return "RETURNING " + gstr.Join(quotedFields, ", ")
}
