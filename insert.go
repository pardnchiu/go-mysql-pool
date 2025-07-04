package goMysql

import (
	"fmt"
	"strings"
)

func (b *builder) Insert(data map[string]interface{}) (int64, error) {
	if b.table == nil {
		return 0, b.logger.Error(nil, "Table is required")
	}

	columns := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))
	placeholders := make([]string, 0, len(data))

	for column, value := range data {
		columns = append(columns, fmt.Sprintf("`%s`", column))
		values = append(values, value)
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		*b.table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	result, err := b.exec(query, values...)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}
