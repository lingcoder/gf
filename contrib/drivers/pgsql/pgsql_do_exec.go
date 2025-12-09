// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package pgsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/text/gstr"
)

// DoExec commits the sql string and its arguments to underlying driver
// through given link object and returns the execution result.
func (d *Driver) DoExec(ctx context.Context, link gdb.Link, sql string, args ...any) (result sql.Result, err error) {
	var (
		isUseCoreDoExec bool   = false // Check whether the default method needs to be used
		isUserReturning bool   = false // User explicitly specified RETURNING fields
		primaryKey      string = ""
		pkField         gdb.TableField
	)

	// Transaction checks.
	if link == nil {
		if tx := gdb.TXFromCtx(ctx, d.GetGroup()); tx != nil {
			// Firstly, check and retrieve transaction link from context.
			link = tx
		} else if link, err = d.MasterLink(); err != nil {
			// Or else it creates one from master node.
			return nil, err
		}
	} else if !link.IsTransaction() {
		// If current link is not transaction link, it checks and retrieves transaction from context.
		if tx := gdb.TXFromCtx(ctx, d.GetGroup()); tx != nil {
			link = tx
		}
	}

	// Check if user specified RETURNING fields (from context or DoInsertOption)
	if returningFields := gdb.GetReturningFromCtx(ctx); len(returningFields) > 0 {
		// User explicitly specified RETURNING fields
		sql += " " + buildReturningClause(returningFields)
		isUseCoreDoExec = false
		isUserReturning = true
	} else if value := ctx.Value(internalPrimaryKeyInCtx); value != nil {
		// Fall back to automatic primary key RETURNING
		var ok bool
		pkField, ok = value.(gdb.TableField)
		if !ok {
			isUseCoreDoExec = true
		} else if pkField.Name != "" && strings.Contains(sql, "INSERT INTO") {
			// Automatic RETURNING for INSERT with primary key
			primaryKey = pkField.Name
			sql += fmt.Sprintf(` RETURNING "%s"`, primaryKey)
			isUseCoreDoExec = false
		} else {
			isUseCoreDoExec = true
		}
	} else {
		isUseCoreDoExec = true
	}

	if isUseCoreDoExec {
		// use default DoExec
		return d.Core.DoExec(ctx, link, sql, args...)
	}

	// Execute with RETURNING clause

	// Sql filtering.
	sql, args = d.FormatSqlBeforeExecuting(sql, args)
	sql, args, err = d.DoFilter(ctx, link, sql, args)
	if err != nil {
		return nil, err
	}

	// Link execution.
	var out gdb.DoCommitOutput
	out, err = d.DoCommit(ctx, gdb.DoCommitInput{
		Link:          link,
		Sql:           sql,
		Args:          args,
		Stmt:          nil,
		Type:          gdb.SqlTypeQueryContext,
		IsTransaction: link.IsTransaction(),
	})

	if err != nil {
		return nil, err
	}
	affected := len(out.Records)

	// If user explicitly specified RETURNING fields, return the result with records.
	// The records are available via GetRecords() method for gdb.ReturningResult interface.
	if isUserReturning {
		return Result{
			affected:     int64(affected),
			lastInsertId: 0,
			lastInsertIdError: gerror.NewCodef(
				gcode.CodeNotSupported,
				"LastInsertId is not supported when using custom RETURNING clause"),
			records: out.Records,
		}, nil
	}

	// Handle automatic primary key RETURNING for INSERT
	if affected > 0 {
		if !strings.Contains(pkField.Type, "int") {
			return Result{
				affected:     int64(affected),
				lastInsertId: 0,
				lastInsertIdError: gerror.NewCodef(
					gcode.CodeNotSupported,
					"LastInsertId is not supported by primary key type: %s", pkField.Type),
			}, nil
		}

		if out.Records[affected-1][primaryKey] != nil {
			lastInsertId := out.Records[affected-1][primaryKey].Int64()
			return Result{
				affected:     int64(affected),
				lastInsertId: lastInsertId,
			}, nil
		}
	}

	return Result{}, nil
}

// buildReturningClause builds the RETURNING clause for PostgreSQL.
// It supports:
// - Simple field names: RETURNING id, name
// - Wildcard: RETURNING *
// - PostgreSQL 18+ OLD/NEW syntax: RETURNING OLD.id, NEW.id, OLD.*, NEW.*
func buildReturningClause(fields []string) string {
	if len(fields) == 0 {
		return ""
	}

	var quotedFields []string
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "*" {
			quotedFields = append(quotedFields, "*")
		} else if strings.HasPrefix(strings.ToUpper(field), "OLD.") {
			// PostgreSQL 18+ OLD.* or OLD.field_name syntax
			parts := strings.SplitN(field, ".", 2)
			if len(parts) == 2 {
				if parts[1] == "*" {
					quotedFields = append(quotedFields, "OLD.*")
				} else {
					quotedFields = append(quotedFields, fmt.Sprintf(`OLD."%s"`, parts[1]))
				}
			} else {
				quotedFields = append(quotedFields, fmt.Sprintf(`"%s"`, field))
			}
		} else if strings.HasPrefix(strings.ToUpper(field), "NEW.") {
			// PostgreSQL 18+ NEW.* or NEW.field_name syntax
			parts := strings.SplitN(field, ".", 2)
			if len(parts) == 2 {
				if parts[1] == "*" {
					quotedFields = append(quotedFields, "NEW.*")
				} else {
					quotedFields = append(quotedFields, fmt.Sprintf(`NEW."%s"`, parts[1]))
				}
			} else {
				quotedFields = append(quotedFields, fmt.Sprintf(`"%s"`, field))
			}
		} else {
			quotedFields = append(quotedFields, fmt.Sprintf(`"%s"`, field))
		}
	}

	return "RETURNING " + gstr.Join(quotedFields, ", ")
}
