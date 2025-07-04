package goMysql

import (
	"database/sql"
	"fmt"
	"strings"
)

func (b *builder) Update(data ...map[string]interface{}) (sql.Result, error) {
	if b.table == nil {
		return nil, b.logger.Error(nil, "Table is required")
	}

	values := []interface{}{}

	if len(data) > 0 {
		for column, value := range data[0] {
			columnName := column
			if !strings.Contains(column, ".") {
				columnName = fmt.Sprintf("`%s`", column)
			}

			if str, ok := value.(string); ok && contains(supportFunction, strings.ToUpper(str)) {
				b.setList = append(b.setList, fmt.Sprintf("%s = %s", columnName, str))
			} else {
				b.setList = append(b.setList, fmt.Sprintf("%s = ?", columnName))
				values = append(values, value)
			}
		}
	}

	query := fmt.Sprintf("UPDATE `%s` SET %s", *b.table, strings.Join(b.setList, ", "))

	if len(b.whereList) > 0 {
		query += " WHERE " + strings.Join(b.whereList, " AND ")
	}

	allValues := append(values, b.bindingList...)
	return b.exec(query, allValues...)
}
