// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package dm

import (
	"context"
	"fmt"
	"strings"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/util/gutil"
)

const (
	// tablesSqlByUser returns tables owned by current user, similar to Oracle's USER_TABLES.
	tablesSqlByUser = `SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME`

	// tablesSqlBySchema returns tables of specified schema/owner.
	tablesSqlBySchema = `SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = '%s' ORDER BY TABLE_NAME`
)

// Tables retrieves and returns the tables of current schema.
// It's mainly used in cli tool chain for automatically generating the models.
//
// When schema is specified (via parameter or config), it queries tables from that schema.
// When schema is not specified, it queries tables owned by current user (like Oracle's USER_TABLES).
func (d *Driver) Tables(ctx context.Context, schema ...string) (tables []string, err error) {
	var (
		result     gdb.Result
		usedSchema = gutil.GetOrDefaultStr(d.GetSchema(), schema...)
	)
	// When schema is empty, return the default link
	link, err := d.SlaveLink(schema...)
	if err != nil {
		return nil, err
	}

	var query string
	if usedSchema != "" {
		// Use specified schema
		query = fmt.Sprintf(tablesSqlBySchema, strings.ToUpper(usedSchema))
	} else {
		// Use current user's tables (like Oracle's USER_TABLES)
		query = tablesSqlByUser
	}

	result, err = d.DoSelect(ctx, link, query)
	if err != nil {
		return
	}
	for _, m := range result {
		if v, ok := m["TABLE_NAME"]; ok {
			tables = append(tables, v.String())
		}
	}
	return
}
