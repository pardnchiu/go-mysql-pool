package goMysql

import (
	"database/sql"
	"fmt"
	"strings"
)

func (b *builder) Get() (*sql.Rows, error) {
	if b.table == nil {
		return nil, b.logger.Error(nil, "Table is required")
	}

	fieldNames := make([]string, len(b.selectList))
	for i, field := range b.selectList {
		switch {
		case field == "*":
			fieldNames[i] = "*"
		case strings.ContainsAny(field, ".()"):
			fieldNames[i] = field
		default:
			fieldNames[i] = fmt.Sprintf("`%s`", field)
		}
	}

	query := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(fieldNames, ", "), *b.table)

	if len(b.joinList) > 0 {
		query += " " + strings.Join(b.joinList, " ")
	}

	if len(b.whereList) > 0 {
		query += " WHERE " + strings.Join(b.whereList, " AND ")
	}

	if b.withTotal {
		query = fmt.Sprintf("SELECT COUNT(*) OVER() AS total, data.* FROM (%s) AS data", query)
	}

	if len(b.orderList) > 0 {
		query += " ORDER BY " + strings.Join(b.orderList, ", ")
	}

	if b.limit != nil {
		query += fmt.Sprintf(" LIMIT %d", *b.limit)
	}

	if b.offset != nil {
		query += fmt.Sprintf(" OFFSET %d", *b.offset)
	}

	return b.query(query, b.bindingList...)
}
