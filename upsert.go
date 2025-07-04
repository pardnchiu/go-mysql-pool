package goMysql

import (
	"fmt"
	"strings"
)

func (b *builder) Upsert(data map[string]interface{}, updateData ...interface{}) (int64, error) {
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

	updateClause := ""
	updateValues := []interface{}{}

	if len(updateData) > 0 {
		switch v := updateData[0].(type) {
		case string:
			updateClause = fmt.Sprintf(" ON DUPLICATE KEY UPDATE %s", v)
		case map[string]interface{}:
			updateParts := []string{}
			for column, value := range v {
				columnName := column
				if !strings.Contains(column, ".") {
					columnName = fmt.Sprintf("`%s`", column)
				}

				if str, ok := value.(string); ok && contains(supportFunction, strings.ToUpper(str)) {
					updateParts = append(updateParts, fmt.Sprintf("%s = %s", columnName, str))
				} else {
					updateParts = append(updateParts, fmt.Sprintf("%s = ?", columnName))
					updateValues = append(updateValues, value)
				}
			}
			updateClause = fmt.Sprintf(" ON DUPLICATE KEY UPDATE %s", strings.Join(updateParts, ", "))
		}
	} else {
		defaultUpdateParts := make([]string, len(columns))
		for i, column := range columns {
			defaultUpdateParts[i] = fmt.Sprintf("%s = VALUES(%s)", column, column)
		}
		updateClause = fmt.Sprintf(" ON DUPLICATE KEY UPDATE %s", strings.Join(defaultUpdateParts, ", "))
	}

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)%s",
		*b.table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		updateClause)

	allValues := append(values, updateValues...)
	result, err := b.exec(query, allValues...)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}
