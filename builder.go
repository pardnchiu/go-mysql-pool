package goMysql

import (
	"fmt"
	"log"
	"strings"
)

var (
	supportFunction = []string{
		"NOW()", "CURRENT_TIMESTAMP", "UUID()", "RAND()", "CURDATE()",
		"CURTIME()", "UNIX_TIMESTAMP()", "UTC_TIMESTAMP()", "SYSDATE()",
		"LOCALTIME()", "LOCALTIMESTAMP()", "PI()", "DATABASE()", "USER()",
		"VERSION()",
	}
)

func (p *Pool) DB(dbName string) *builder {
	_, err := p.db.Exec(fmt.Sprintf("USE `%s`", dbName))
	if err != nil {
		p.logger.Error(err, "Failed to switch to database "+dbName)
	}

	return &builder{
		db:         p.db,
		dbName:     &dbName,
		selectList: []string{"*"},
		logger:     p.logger,
	}
}

func (b *builder) Table(tableName string) *builder {
	b.table = &tableName
	return b
}

func (b *builder) Select(fields ...string) *builder {
	if len(fields) > 0 {
		b.selectList = fields
	}
	return b
}

func (b *builder) Total() *builder {
	b.withTotal = true
	return b
}

func (b *builder) InnerJoin(table, first, operator string, second ...string) *builder {
	return b.join("INNER", table, first, operator, second...)
}

func (b *builder) LeftJoin(table, first, operator string, second ...string) *builder {
	return b.join("LEFT", table, first, operator, second...)
}

func (b *builder) RightJoin(table, first, operator string, second ...string) *builder {
	return b.join("RIGHT", table, first, operator, second...)
}

// * private method
func (b *builder) join(joinType, table, first, operator string, second ...string) *builder {
	var secondField string
	if len(second) > 0 {
		secondField = second[0]
	} else {
		secondField = operator
		operator = "="
	}

	if !strings.Contains(first, ".") {
		first = fmt.Sprintf("`%s`", first)
	}
	if !strings.Contains(secondField, ".") {
		secondField = fmt.Sprintf("`%s`", secondField)
	}

	joinClause := fmt.Sprintf("%s JOIN `%s` ON %s %s %s", joinType, table, first, operator, secondField)
	b.joinList = append(b.joinList, joinClause)
	return b
}

func (b *builder) Where(column string, operator interface{}, value ...interface{}) *builder {
	var targetValue interface{}
	var targetOperator string

	if len(value) == 0 {
		targetValue = operator
		targetOperator = "="
	} else {
		targetOperator = fmt.Sprintf("%v", operator)
		targetValue = value[0]
	}

	if targetOperator == "LIKE" {
		if str, ok := targetValue.(string); ok {
			targetValue = fmt.Sprintf("%%%s%%", str)
		}
	}

	if !strings.Contains(column, "(") && !strings.Contains(column, ".") {
		column = fmt.Sprintf("`%s`", column)
	}

	placeholder := "?"
	if targetOperator == "IN" {
		placeholder = "(?)"
	}

	whereClause := fmt.Sprintf("%s %s %s", column, targetOperator, placeholder)
	b.whereList = append(b.whereList, whereClause)
	b.bindingList = append(b.bindingList, targetValue)

	return b
}

func (b *builder) OrderBy(column string, direction ...string) *builder {
	dir := "ASC"
	if len(direction) > 0 {
		dir = strings.ToUpper(direction[0])
	}

	if dir != "ASC" && dir != "DESC" {
		log.Printf("Invalid order direction: %s", dir)
		return b
	}

	if !strings.Contains(column, ".") {
		column = fmt.Sprintf("`%s`", column)
	}

	orderClause := fmt.Sprintf("%s %s", column, dir)
	b.orderList = append(b.orderList, orderClause)
	return b
}

func (b *builder) Limit(num int) *builder {
	b.limit = &num
	return b
}

func (b *builder) Offset(num int) *builder {
	b.offset = &num
	return b
}

func (b *builder) Increase(target string, number ...int) *builder {
	num := 1
	if len(number) > 0 {
		num = number[0]
	}

	setClause := fmt.Sprintf("%s = %s + %d", target, target, num)
	b.setList = append(b.setList, setClause)
	return b
}

// * private method
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
