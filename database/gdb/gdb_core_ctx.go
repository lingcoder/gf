// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gdb

import (
	"context"
	"sync"

	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/os/gctx"
)

// internalCtxData stores data in ctx for internal usage purpose.
type internalCtxData struct {
	sync.Mutex
	// Used configuration node in current operation.
	ConfigNode *ConfigNode
}

// column stores column data in ctx for internal usage purpose.
type internalColumnData struct {
	// The first column in result response from database server.
	// This attribute is used for Value/Count selection statement purpose,
	// which is to avoid HOOK handler that might modify the result columns
	// that can confuse the Value/Count selection statement logic.
	FirstResultColumn string
}

const (
	internalCtxDataKeyInCtx    gctx.StrKey = "InternalCtxData"
	internalColumnDataKeyInCtx gctx.StrKey = "InternalColumnData"

	// `ignoreResultKeyInCtx` is a mark for some db drivers that do not support `RowsAffected` function,
	// for example: `clickhouse`. The `clickhouse` does not support fetching insert/update results,
	// but returns errors when execute `RowsAffected`. It here ignores the calling of `RowsAffected`
	// to avoid triggering errors, rather than ignoring errors after they are triggered.
	ignoreResultKeyInCtx gctx.StrKey = "IgnoreResult"

	// ReturningKeyInCtx is used to pass RETURNING clause fields through context.
	// This is used by INSERT/UPDATE/DELETE operations that support RETURNING clause.
	// Database drivers can retrieve this value to append RETURNING clause to SQL statements.
	ReturningKeyInCtx gctx.StrKey = "ReturningFields"
)

func (c *Core) injectInternalCtxData(ctx context.Context) context.Context {
	// If the internal data is already injected, it does nothing.
	if ctx.Value(internalCtxDataKeyInCtx) != nil {
		return ctx
	}
	return context.WithValue(ctx, internalCtxDataKeyInCtx, &internalCtxData{
		ConfigNode: c.config,
	})
}

func (c *Core) setConfigNodeToCtx(ctx context.Context, node *ConfigNode) error {
	value := ctx.Value(internalCtxDataKeyInCtx)
	if value == nil {
		return gerror.NewCode(gcode.CodeInternalError, `no internal data found in context`)
	}

	data := value.(*internalCtxData)
	data.Lock()
	defer data.Unlock()
	data.ConfigNode = node
	return nil
}

func (c *Core) getConfigNodeFromCtx(ctx context.Context) *ConfigNode {
	if value := ctx.Value(internalCtxDataKeyInCtx); value != nil {
		data := value.(*internalCtxData)
		data.Lock()
		defer data.Unlock()
		return data.ConfigNode
	}
	return nil
}

func (c *Core) injectInternalColumn(ctx context.Context) context.Context {
	return context.WithValue(ctx, internalColumnDataKeyInCtx, &internalColumnData{})
}

func (c *Core) getInternalColumnFromCtx(ctx context.Context) *internalColumnData {
	if v := ctx.Value(internalColumnDataKeyInCtx); v != nil {
		return v.(*internalColumnData)
	}
	return nil
}

func (c *Core) InjectIgnoreResult(ctx context.Context) context.Context {
	if ctx.Value(ignoreResultKeyInCtx) != nil {
		return ctx
	}
	return context.WithValue(ctx, ignoreResultKeyInCtx, true)
}

func (c *Core) GetIgnoreResultFromCtx(ctx context.Context) bool {
	return ctx.Value(ignoreResultKeyInCtx) != nil
}

// InjectReturning injects RETURNING fields into context.
// This is used by database drivers that support RETURNING clause.
func InjectReturning(ctx context.Context, fields []string) context.Context {
	if len(fields) == 0 {
		return ctx
	}
	return context.WithValue(ctx, ReturningKeyInCtx, fields)
}

// GetReturningFromCtx retrieves RETURNING fields from context.
// Returns nil if no RETURNING fields are set.
func GetReturningFromCtx(ctx context.Context) []string {
	if v := ctx.Value(ReturningKeyInCtx); v != nil {
		if fields, ok := v.([]string); ok {
			return fields
		}
	}
	return nil
}
