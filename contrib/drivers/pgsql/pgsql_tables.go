// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package pgsql

import (
	"context"
	"fmt"
	"regexp"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/text/gregex"
	"github.com/gogf/gf/v2/text/gstr"
	"github.com/gogf/gf/v2/util/gutil"
)

var (
	// tablesSqlTmp queries user-accessible tables from a specific schema.
	// Filters:
	//   - relkind IN ('r','p'): ordinary tables and partitioned tables (parent)
	//   - relpartbound IS NULL: excludes partition children (PostgreSQL 10+)
	//   - NOT relispartition: alternative for PostgreSQL 10+ (more explicit)
	tablesSqlTmp = `
SELECT c.relname
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = '%s'
  AND c.relkind IN ('r', 'p')
  %s
ORDER BY c.relname`

	// tablesSqlBySearchPath queries tables from all schemas in search_path.
	// Uses DISTINCT ON + array_position for search_path priority deduplication.
	// https://github.com/gogf/gf/issues/4495
	tablesSqlBySearchPath = `
SELECT DISTINCT ON (c.relname) c.relname
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = ANY(current_schemas(false))
  AND c.relkind IN ('r', 'p')
  %s
ORDER BY c.relname, array_position(current_schemas(false), n.nspname)`

	versionRegex = regexp.MustCompile(`PostgreSQL (\d+\.\d+)`)
)

func init() {
	var err error
	tablesSqlTmp, err = gdb.FormatMultiLineSqlToSingle(tablesSqlTmp)
	if err != nil {
		panic(err)
	}
	tablesSqlBySearchPath, err = gdb.FormatMultiLineSqlToSingle(tablesSqlBySearchPath)
	if err != nil {
		panic(err)
	}
}

// Tables retrieves and returns the tables of current schema.
// It's mainly used in cli tool chain for automatically generating the models.
//
// When schema is specified (via parameter or Namespace config), it queries tables from that schema.
// When schema is not specified, it queries tables from all schemas in current search_path,
// with duplicate table names resolved by search_path priority (first schema wins).
func (d *Driver) Tables(ctx context.Context, schema ...string) (tables []string, err error) {
	var (
		result     gdb.Result
		usedSchema = gutil.GetOrDefaultStr(d.GetConfig().Namespace, schema...)
	)
	// DO NOT pass schema to SlaveLink - in PostgreSQL, schema is a namespace within
	// the database, not the database itself. Passing schema to SlaveLink would attempt
	// to connect to a different database named "schema", which is incorrect.
	link, err := d.SlaveLink()
	if err != nil {
		return nil, err
	}

	useRelpartbound := ""
	if gstr.CompareVersion(d.version(ctx, link), "10") >= 0 {
		useRelpartbound = "AND c.relpartbound IS NULL"
	}

	var query string
	if usedSchema != "" {
		// Use specified schema
		query = fmt.Sprintf(tablesSqlTmp, usedSchema, useRelpartbound)
	} else {
		// Use search_path to get tables from all accessible schemas
		query = fmt.Sprintf(tablesSqlBySearchPath, useRelpartbound)
	}

	query, _ = gregex.ReplaceString(`[\n\r\s]+`, " ", gstr.Trim(query))
	result, err = d.DoSelect(ctx, link, query)
	if err != nil {
		return
	}
	for _, m := range result {
		for _, v := range m {
			tables = append(tables, v.String())
		}
	}
	return
}

// version checks and returns the database version.
func (d *Driver) version(ctx context.Context, link gdb.Link) string {
	result, err := d.DoSelect(ctx, link, "SELECT version();")
	if err != nil {
		return ""
	}
	if len(result) > 0 {
		if v, ok := result[0]["version"]; ok {
			matches := versionRegex.FindStringSubmatch(v.String())
			if len(matches) >= 2 {
				return matches[1]
			}
		}
	}
	return ""
}
