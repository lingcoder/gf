// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package mssql

import (
	"strings"

	"github.com/gogf/gf/v2/database/gdb"
)

// lockTableHints maps the lock clauses of other dialects to the SQL Server table hints
// that take the same row locks until the transaction ends.
var lockTableHints = map[string]string{
	gdb.LockForUpdate:           "(UPDLOCK, ROWLOCK)",
	gdb.LockForUpdateNowait:     "(UPDLOCK, ROWLOCK, NOWAIT)",
	gdb.LockForUpdateSkipLocked: "(UPDLOCK, ROWLOCK, READPAST)",
	gdb.LockInShareMode:         "(REPEATABLEREAD, ROWLOCK)",
	gdb.LockForShare:            "(REPEATABLEREAD, ROWLOCK)",
	gdb.LockForShareNowait:      "(REPEATABLEREAD, ROWLOCK, NOWAIT)",
	gdb.LockForShareSkipLocked:  "(REPEATABLEREAD, ROWLOCK, READPAST)",
}

// tableReferenceEndKeywords are the keywords that may follow the first table reference of a
// query, so that none of them is taken as its alias.
var tableReferenceEndKeywords = map[string]bool{
	"WHERE": true, "JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true, "FULL": true,
	"CROSS": true, "OUTER": true, "ON": true, "GROUP": true, "ORDER": true, "HAVING": true,
	"UNION": true, "EXCEPT": true, "INTERSECT": true, "FOR": true, "LIMIT": true,
	"OFFSET": true, "OPTION": true, "WITH": true, "LOCK": true,
}

// moveLockClause moves the lock clause that the core appends to the query formed by `tokens`
// into a table hint after the first table it selects from, with its alias, as SQL Server locks
// rows by table hints only. A lock clause of another dialect becomes its equivalent table hint,
// and a table hint like gdb.LockWithUpdLock is moved as it is. It returns `tokens` unchanged if
// they end with no lock clause or select from no table.
func moveLockClause(tokens []sqlToken) []sqlToken {
	hint, begin := trailingLockClause(tokens)
	if begin < 0 {
		return tokens
	}
	at := tableHintPosition(tokens[:begin])
	if at < 0 {
		return tokens
	}
	result := make([]sqlToken, 0, begin+2)
	result = append(result, tokens[:at]...)
	result = append(result, sqlToken{space: " ", text: "WITH"}, sqlToken{space: " ", text: hint, group: true})
	return append(result, tokens[at:begin]...)
}

// trailingLockClause returns the table hint, like "(UPDLOCK, ROWLOCK)", of the lock clause that
// `tokens` end with, and the index of its first token, or -1 if they end with none.
func trailingLockClause(tokens []sqlToken) (hint string, begin int) {
	n := len(tokens)
	if n >= 2 && tokens[n-2].is("WITH") && tokens[n-1].group {
		return tokens[n-1].text, n - 2
	}
	for lock, hint := range lockTableHints {
		words := strings.Fields(lock)
		if len(words) > n {
			continue
		}
		matched := true
		for i, word := range words {
			if !tokens[n-len(words)+i].is(word) {
				matched = false
				break
			}
		}
		if matched {
			return hint, n - len(words)
		}
	}
	return "", -1
}

// tableHintPosition returns the index in `tokens` following the first table reference after
// FROM and its alias, where the table hint of the reference goes, or -1 if there is no FROM.
func tableHintPosition(tokens []sqlToken) int {
	var i int
	for i < len(tokens) && !tokens[i].is("FROM") {
		i++
	}
	if i += 2; i > len(tokens) {
		return -1
	}
	for i < len(tokens) && tokens[i].space == "" && tokens[i].text != "," {
		i++
	}
	switch {
	case i+1 < len(tokens) && tokens[i].is("AS"):
		i += 2
	case i < len(tokens) && !tokens[i].group && tokens[i].text != "," &&
		!tableReferenceEndKeywords[strings.ToUpper(tokens[i].text)]:
		i++
	}
	return i
}
