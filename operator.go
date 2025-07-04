package goMysql

import (
	"database/sql"
	"fmt"
	"time"
)

func (p *Pool) Query(query string, params ...interface{}) (*sql.Rows, error) {
	if p.db == nil {
		return nil, p.logger.Error(nil, "Database connection is not available")
	}

	startTime := time.Now()
	rows, err := p.db.Query(query, params...)
	duration := time.Since(startTime)

	if duration > 20*time.Millisecond {
		p.logger.Info(fmt.Sprintf("Slow Query %s", duration))
	}

	return rows, err
}

func (p *Pool) Exec(query string, params ...interface{}) (sql.Result, error) {
	if p.db == nil {
		return nil, p.logger.Error(nil, "Database connection is not available")
	}

	startTime := time.Now()
	result, err := p.db.Exec(query, params...)
	duration := time.Since(startTime)

	if duration > 20*time.Millisecond {
		p.logger.Debug(fmt.Sprintf("Slow Query %s", duration))
	}

	return result, err
}

func (b *builder) query(query string, params ...interface{}) (*sql.Rows, error) {
	if b.db == nil {
		return nil, b.logger.Error(nil, "Database connection is not available")
	}

	startTime := time.Now()
	rows, err := b.db.Query(query, params...)
	duration := time.Since(startTime)

	if duration > 20*time.Millisecond {
		b.logger.Debug(fmt.Sprintf("Slow Query %s", duration))
	}

	return rows, err
}

func (b *builder) exec(query string, params ...interface{}) (sql.Result, error) {
	if b.db == nil {
		return nil, b.logger.Error(nil, "Database connection is not available")
	}

	startTime := time.Now()
	result, err := b.db.Exec(query, params...)
	duration := time.Since(startTime)

	if duration > 20*time.Millisecond {
		b.logger.Info(fmt.Sprintf("Slow Query %s", duration))
	}

	return result, err
}
