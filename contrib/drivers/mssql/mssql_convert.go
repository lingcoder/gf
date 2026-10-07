// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package mssql

import (
	"context"
	"strings"

	"github.com/google/uuid"
	mssqldriver "github.com/microsoft/go-mssqldb"

	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/text/gregex"
)

// characterFieldTypes are the SQL Server types storing text, into which a value is bound as a string.
var characterFieldTypes = map[string]bool{
	"char":     true,
	"varchar":  true,
	"nchar":    true,
	"nvarchar": true,
	"text":     true,
	"ntext":    true,
	"xml":      true,
}

// binaryFieldTypes are the SQL Server types storing bytes, into which NULL is bound as binary.
var binaryFieldTypes = map[string]bool{
	"binary":    true,
	"varbinary": true,
	"image":     true,
}

// CheckLocalTypeForField checks and returns corresponding local Golang type for given db type.
//
// SQL Server type mapping (only types not handled by Core are listed):
//
//	| SQL Server Type   | Local Go Type |
//	|-------------------|---------------|
//	| uniqueidentifier  | uuid.UUID     |
//	| bit               | bool          |
func (d *Driver) CheckLocalTypeForField(ctx context.Context, fieldType string, fieldValue any) (gdb.LocalType, error) {
	typeName, _ := gregex.ReplaceString(`\(.+\)`, "", fieldType)
	typeName = strings.ToLower(typeName)

	switch typeName {
	case "uniqueidentifier":
		return gdb.LocalTypeUUID, nil

	case "bit":
		return gdb.LocalTypeBool, nil

	default:
		return d.Core.CheckLocalTypeForField(ctx, fieldType, fieldValue)
	}
}

// ConvertValueForField converts value to the type of the record field `fieldType`.
// A value that Core encodes as JSON bytes is bound as a string into a text column, as SQL Server
// converts bytes to text by reinterpreting them; NULL is bound as binary into a binary column, as
// SQL Server refuses to convert an untyped NULL parameter to binary.
func (d *Driver) ConvertValueForField(ctx context.Context, fieldType string, fieldValue any) (any, error) {
	convertedValue, err := d.Core.ConvertValueForField(ctx, fieldType, fieldValue)
	if err != nil {
		return nil, err
	}
	typeName := strings.ToLower(fieldType)
	if b, ok := convertedValue.([]byte); ok && characterFieldTypes[typeName] {
		return string(b), nil
	}
	if convertedValue == nil && binaryFieldTypes[typeName] {
		return []byte(nil), nil
	}
	return convertedValue, nil
}

// ConvertValueForLocal converts value to local Golang type of value according field type name from database.
//
// SQL Server stores UNIQUEIDENTIFIER on the wire as a 16-byte binary blob whose first 8 bytes
// follow the little-endian COM/Win32 GUID layout, while the remaining 8 bytes are big-endian.
// Reading the raw bytes as a string yields garbage; go-mssqldb's UniqueIdentifier.Scan
// performs the required byte-order swap so the returned [uuid.UUID] matches the canonical
// RFC 4122 form (and what tools like SSMS display).
func (d *Driver) ConvertValueForLocal(ctx context.Context, fieldType string, fieldValue any) (any, error) {
	typeName, _ := gregex.ReplaceString(`\(.+\)`, "", fieldType)
	typeName = strings.ToLower(typeName)

	switch typeName {
	case "uniqueidentifier":
		var msID mssqldriver.UniqueIdentifier
		if err := msID.Scan(fieldValue); err != nil {
			return nil, gerror.Wrapf(
				err, "convert uniqueidentifier value %v to uuid.UUID failed", fieldValue,
			)
		}
		return uuid.UUID(msID), nil

	default:
		return d.Core.ConvertValueForLocal(ctx, fieldType, fieldValue)
	}
}
