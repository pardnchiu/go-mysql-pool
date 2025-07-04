package goMysql

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	goLogger "github.com/pardnchiu/go-logger"
)

func New(c Config) (*PoolList, error) {
	c.Log = validLoggerConfig(c)

	logger, err := goLogger.New(c.Log)
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize `pardnchiu/go-logger`: %w", err)
	}

	var pool = &PoolList{
		Read:   nil,
		Write:  nil,
		logger: logger,
	}

	readConfig := c.Read

	if readConfig.Host == "" {
		readConfig.Host = "localhost"
	}

	if readConfig.Port == 0 {
		readConfig.Port = 3306
	}

	if readConfig.User == "" {
		readConfig.User = "root"
	}

	if readConfig.Password == "" {
		readConfig.Password = ""
	}

	if readConfig.Charset == "" {
		readConfig.Charset = "utf8mb4"
	}

	if readConfig.Connection == 0 {
		readConfig.Connection = 4
	}

	read, err := sql.Open("mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/?charset=%s&parseTime=true",
			readConfig.User,
			readConfig.Password,
			readConfig.Host,
			readConfig.Port,
			readConfig.Charset,
		),
	)
	if err != nil {
		return nil, logger.Error(err, "Failed to create read pool")
	}

	read.SetMaxOpenConns(readConfig.Connection)
	read.SetMaxIdleConns(readConfig.Connection / 2)
	read.SetConnMaxLifetime(time.Hour)

	if err := read.Ping(); err != nil {
		return nil, logger.Error(err, "Failed to connect read pool")
	}

	pool.Read = &Pool{db: read}

	writeConfig := c.Write
	if writeConfig == nil {
		writeConfig = readConfig
	}

	writeDB, err := sql.Open(
		"mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/?charset=%s&parseTime=true",
			writeConfig.User,
			writeConfig.Password,
			writeConfig.Host,
			writeConfig.Port,
			writeConfig.Charset,
		),
	)
	if err != nil {
		return nil, logger.Error(err, "Failed to create write pool")
	}

	writeDB.SetMaxOpenConns(writeConfig.Connection)
	writeDB.SetMaxIdleConns(writeConfig.Connection / 2)
	writeDB.SetConnMaxLifetime(time.Hour)

	if err := writeDB.Ping(); err != nil {
		return nil, logger.Error(err, "Failed to connect write pool")
	}

	pool.Write = &Pool{db: writeDB}

	pool.listenShutdownSignal()
	pool.Write.logger = logger
	pool.Read.logger = logger
	return pool, nil
}

func (p *PoolList) Close() error {
	var readErr, writeErr error

	if p.Read != nil && p.Read.db != nil {
		readErr = p.Read.db.Close()
		p.Read = nil
	}

	if p.Write != nil && p.Write.db != nil {
		writeErr = p.Write.db.Close()
		p.Write = nil
	}

	if readErr != nil {
		return p.Write.logger.Error(readErr, "Failed to close read pool")
	}
	if writeErr != nil {
		return p.Write.logger.Error(writeErr, "Failed to close write pool")
	}

	return nil
}

func (p *PoolList) listenShutdownSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		_ = p.Close()
		os.Exit(0)
	}()
}

func validLoggerConfig(c Config) *Log {
	if c.Log == nil {
		c.Log = &Log{
			Path:    defaultLogPath,
			Stdout:  false,
			MaxSize: defaultLogMaxSize,
		}
	}
	if c.Log.Path == "" {
		c.Log.Path = defaultLogPath
	}
	if c.Log.MaxSize <= 0 {
		c.Log.MaxSize = defaultLogMaxSize
	}
	if c.Log.MaxBackup <= 0 {
		c.Log.MaxBackup = defaultLogMaxBackup
	}
	return c.Log
}
