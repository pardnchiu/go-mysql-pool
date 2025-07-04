# MySQL 連線池 (Golang)
> 一個支持鏈式呼叫的 Golang MySQL 包裝器，具備讀寫分離設置、查詢建構器等功能，提供完整的連線管理。<br>
>> Node.js 版本可在[這裡](https://github.com/pardnchiu/node-mysql-pool)取得<br>
>> PHP 版本可在[這裡](https://github.com/pardnchiu/php-mysql-pool)取得

[![license](https://img.shields.io/github/license/pardnchiu/go-mysql)](LICENSE) 
[![version](https://img.shields.io/github/v/tag/pardnchiu/go-mysql)](https://github.com/pardnchiu/go-mysql/releases) 
[![readme](https://img.shields.io/badge/readme-English-blue)](README.md) 

## 三大主軸

### 讀寫分離配置
支援讀寫連線池配置，增加資料庫預連接，提高連接效率

### 查詢建構器
支持鏈式語法的 SQL 查詢建構介面，防止 SQL 注入攻擊

### CRUD 操作
完整的新增、查詢、更新、刪除操作支援

## 依賴套件

- [`github.com/go-sql-driver/mysql`](https://github.com/go-sql-driver/mysql)
- [`github.com/pardnchiu/go-logger`](https://github.com/pardnchiu/go-logger)

## 使用方法

### 安裝
```bash
go get github.com/pardnchiu/go-mysql
```

### 初始化
```go
package main

import (
  "fmt"
  "log"
  
  mp "github.com/pardnchiu/go-mysql"
)

func main() {
  config := mp.Config{
    Read: &mp.DBConfig{
      Host:       "localhost",
      Port:       3306,
      User:       "root",
      Password:   "password",
      Charset:    "utf8mb4",
      Connection: 10,
    },
    Write: &mp.DBConfig{
      Host:       "localhost",
      Port:       3306,
      User:       "root",
      Password:   "password",
      Charset:    "utf8mb4",
      Connection: 5,
    },
  }
  
  // 初始化
  pool, err := mp.New(config)
  if err != nil {
    log.Fatal(err)
  }
  defer pool.Close()
  
  // 插入資料
  userData := map[string]interface{}{
    "name":  "John Doe",
    "email": "john@example.com",
    "age":   30,
  }
  
  lastID, err := pool.Write.
    DB("myapp").
    Table("users").
    Insert(userData)
  if err != nil {
    log.Fatal(err)
  }
  
  fmt.Printf("插入使用者 ID: %d\n", lastID)
  
  // 查詢資料
  rows, err := pool.Read.
    DB("myapp").
    Table("users").
    Select("id", "name", "email").
    Where("age", ">", 18).
    OrderBy("created_at", "DESC").
    Limit(10).
    Get()
  if err != nil {
    log.Fatal(err)
  }
  defer rows.Close()
  
  for rows.Next() {
    var id int
    var name, email string
    err := rows.Scan(&id, &name, &email)
    if err != nil {
      log.Fatal(err)
    }
    fmt.Printf("使用者: %s (%s)\n", name, email)
  }
}
```

## 配置介紹

```go
type Config struct {
  Read  *DBConfig
  Write *DBConfig
  Log   *Log
}

type DBConfig struct {
  Host       string // 資料庫主機位址
  Port       int    // 資料庫連接埠
  User       string // 資料庫使用者名稱
  Password   string // 資料庫密碼
  Charset    string // 字元集 (預設: utf8mb4)
  Connection int    // 最大連線數
}

type Log struct {
  Path      string // 日誌目錄路徑 (預設: ./logs/goMysql)
  Stdout    bool   // 啟用控制台輸出 (預設: false)
  MaxSize   int64  // 檔案輪轉前的最大大小 (預設: 16*1024*1024)
  MaxBackup int    // 保留的日誌檔案數量 (預設: 5)
  Type      string // 輸出格式："json" 為 slog 標準，"text" 為樹狀格式（預設："text"）
}
```

## 支持操作

### 查詢

```go
// 基本查詢
rows, err := pool.Read.
  DB("database_name").
  Table("users").
  Select("id", "name", "email").
  Where("status", "active").
  Get()

// 複雜條件查詢
rows, err := pool.Read.
  DB("database_name").
  Table("users").
  Select("*").
  Where("age", ">", 18).
  Where("status", "active").
  Where("name", "LIKE", "John").
  OrderBy("created_at", "DESC").
  Limit(10).
  Offset(20).
  Get()

// JOIN 查詢
rows, err := pool.Read.
  DB("database_name").
  Table("users").
  Select("users.name", "profiles.bio").
  LeftJoin("profiles", "users.id", "profiles.user_id").
  Where("users.status", "active").
  Get()

// 計算總數
rows, err := pool.Read.
  DB("database_name").
  Table("users").
  Select("id", "name").
  Where("status", "active").
  Total().
  Limit(10).
  Get()
```

### CRUD 操作

```go
// 插入資料
data := map[string]interface{}{
  "name":  "Jane Doe",
  "email": "jane@example.com",
  "age":   25,
}

lastID, err := pool.Write.
  DB("database_name").
  Table("users").
  Insert(data)

// 更新資料
updateData := map[string]interface{}{
  "age":    26,
  "status": "updated",
}

result, err := pool.Write.
  DB("database_name").
  Table("users").
  Where("id", 1).
  Update(updateData)

// Upsert 操作
data := map[string]interface{}{
  "email": "unique@example.com",
  "name":  "New User",
}

updateData := map[string]interface{}{
  "name": "Updated User",
  "last_login": "NOW()",
}

lastID, err := pool.Write.
  DB("database_name").
  Table("users").
  Upsert(data, updateData)

// 遞增值
result, err := pool.Write.
  DB("database_name").
  Table("users").
  Where("id", 1).
  Increase("view_count", 1).
  Update()
```

### SQL 操作

```go
// 直接查詢
rows, err := pool.Read.Query("SELECT * FROM users WHERE age > ?", 18)

// 直接執行
result, err := pool.Write.Exec("UPDATE users SET last_login = NOW() WHERE id = ?", userID)
```

## 可用函式

### 連線池管理

- **New** - 建立新的連線池
  ```go
  pool, err := mp.New(config)
  ```
  - 初始化讀寫分離的連線池
  - 驗證資料庫連線可用性

- **Close** - 關閉連線池
  ```go
  err := pool.Close()
  ```
  - 關閉所有連線
  - 等待進行中的查詢完成
  - 釋放系統資源

### 查詢建構

- **DB** - 指定資料庫
  ```go
  builder := pool.Read.DB("database_name")
  ```

- **Table** - 指定資料表
  ```go
  builder := builder.Table("table_name")
  ```

- **Select** - 選擇欄位
  ```go
  builder := builder.Select("col1", "col2", "col3")
  ```

- **Where** - 篩選條件
  ```go
  builder := builder.Where("column", "value")
  builder := builder.Where("column", ">", "value")
  builder := builder.Where("column", "LIKE", "pattern")
  ```

- **Join** - 資料表聯結
  ```go
  builder := builder.LeftJoin("table2", "table1.id", "table2.foreign_id")
  builder := builder.RightJoin("table2", "table1.id", "table2.foreign_id")
  builder := builder.InnerJoin("table2", "table1.id", "table2.foreign_id")
  ```

### 資料操作

- **Insert** - 插入資料
  ```go
  lastID, err := builder.Insert(data)
  ```

- **Update** - 更新資料
  ```go
  result, err := builder.Update(data)
  ```

- **Upsert** - 插入或更新
  ```go
  lastID, err := builder.Upsert(insertData, updateData)
  ```

## 授權條款

此原始碼專案採用 [MIT](LICENSE) 授權條款。

## 作者

<img src="https://avatars.githubusercontent.com/u/25631760" align="left" width="96" height="96" style="margin-right: 0.5rem;">

<h4 style="padding-top: 0">邱敬幃 Pardn Chiu</h4>

<a href="mailto:dev@pardn.io" target="_blank">
  <img src="https://pardn.io/image/email.svg" width="48" height="48">
</a> <a href="https://linkedin.com/in/pardnchiu" target="_blank">
  <img src="https://pardn.io/image/linkedin.svg" width="48" height="48">
</a>

***

©️ 2025 [邱敬幃 Pardn Chiu](https://pardn.io)
