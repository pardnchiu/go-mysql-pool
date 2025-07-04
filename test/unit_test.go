package main

import (
	"fmt"
	"log"
	"testing"

	goMysql "github.com/pardnchiu/golang-mysql-pool"
)

var pool *goMysql.PoolList

func init() {
	// 初始化連接池配置
	config := goMysql.Config{
		Read: &goMysql.DBConfig{
			Host:       "localhost",
			Port:       3306,
			User:       "root",
			Password:   "password",
			Charset:    "utf8mb4",
			Connection: 10,
		},
		Write: &goMysql.DBConfig{
			Host:       "localhost",
			Port:       3306,
			User:       "root",
			Password:   "password",
			Charset:    "utf8mb4",
			Connection: 5,
		},
		Log: &goMysql.Log{
			Path: "./logs/mysql-pool-test",
		},
	}

	var err error
	pool, err = goMysql.New(config)
	if err != nil {
		log.Fatal("Failed to initialize pool:", err)
	}
}

func TestPoolInitialization(t *testing.T) {
	if pool == nil {
		t.Fatal("Pool should not be nil")
	}

	if pool.Read == nil {
		t.Fatal("Read pool should not be nil")
	}

	if pool.Write == nil {
		t.Fatal("Write pool should not be nil")
	}

	t.Log("Pool initialization successful")
}

func TestCreateTestTable(t *testing.T) {
	// 創建測試用的資料庫和表格
	_, err := pool.Write.Exec(`
        CREATE DATABASE IF NOT EXISTS test_db 
        CHARACTER SET utf8mb4 
        COLLATE utf8mb4_unicode_ci
    `)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	_, err = pool.Write.Exec("USE test_db")
	if err != nil {
		t.Fatalf("Failed to use test database: %v", err)
	}

	_, err = pool.Write.Exec(`
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            age INT,
            status VARCHAR(20) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    `)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	_, err = pool.Write.Exec(`
        CREATE TABLE IF NOT EXISTS profiles (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT,
            bio TEXT,
            is_public BOOLEAN DEFAULT TRUE,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    `)
	if err != nil {
		t.Fatalf("Failed to create profiles table: %v", err)
	}

	t.Log("Test tables created successfully")
}

func TestInsertData(t *testing.T) {
	data := map[string]interface{}{
		"name":   "John Doe",
		"email":  "john@example.com",
		"age":    30,
		"status": "active",
	}

	lastID, err := pool.Write.
		DB("test_db").
		Table("users").
		Insert(data)

	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if lastID <= 0 {
		t.Fatalf("Expected positive last insert ID, got: %d", lastID)
	}

	t.Logf("Inserted user with ID: %d", lastID)
}

func TestInsertMultipleUsers(t *testing.T) {
	users := []map[string]interface{}{
		{
			"name":   "Jane Smith",
			"email":  "jane@example.com",
			"age":    25,
			"status": "active",
		},
		{
			"name":   "Bob Johnson",
			"email":  "bob@example.com",
			"age":    35,
			"status": "inactive",
		},
		{
			"name":   "Alice Brown",
			"email":  "alice@example.com",
			"age":    28,
			"status": "active",
		},
	}

	for i, userData := range users {
		lastID, err := pool.Write.
			DB("test_db").
			Table("users").
			Insert(userData)

		if err != nil {
			t.Fatalf("Insert user %d failed: %v", i+1, err)
		}

		t.Logf("Inserted user %d with ID: %d", i+1, lastID)
	}
}

func TestSelectData(t *testing.T) {
	rows, err := pool.Read.
		DB("test_db").
		Table("users").
		Select("id", "name", "email", "age").
		Where("status", "active").
		OrderBy("created_at", "DESC").
		Get()

	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, age int
		var name, email string
		err := rows.Scan(&id, &name, &email, &age)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("User: ID=%d, Name=%s, Email=%s, Age=%d", id, name, email, age)
		count++
	}

	if count == 0 {
		t.Fatal("Expected to find active users, but got none")
	}

	t.Logf("Found %d active users", count)
}

func TestSelectWithWhere(t *testing.T) {
	rows, err := pool.Read.
		DB("test_db").
		Table("users").
		Select("*").
		Where("age", ">", 25).
		Where("status", "active").
		OrderBy("age", "ASC").
		Limit(2).
		Get()

	if err != nil {
		t.Fatalf("Select with WHERE failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		// 這裡可以掃描所有欄位，但為了簡化只計數
	}

	t.Logf("Found %d users with age > 25 and status = active", count)
}

func TestSelectWithLike(t *testing.T) {
	rows, err := pool.Read.
		DB("test_db").
		Table("users").
		Select("name", "email").
		Where("name", "LIKE", "J").
		Get()

	if err != nil {
		t.Fatalf("Select with LIKE failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var name, email string
		err := rows.Scan(&name, &email)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("User with 'J' in name: %s (%s)", name, email)
		count++
	}

	t.Logf("Found %d users with 'J' in name", count)
}

func TestUpdateData(t *testing.T) {
	updateData := map[string]interface{}{
		"age":    31,
		"status": "updated",
	}

	result, err := pool.Write.
		DB("test_db").
		Table("users").
		Where("email", "john@example.com").
		Update(updateData)

	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}

	if rowsAffected == 0 {
		t.Fatal("Expected to update at least 1 row, but 0 rows were affected")
	}

	t.Logf("Updated %d rows", rowsAffected)
}

func TestUpsertData(t *testing.T) {
	// 測試插入新資料
	data := map[string]interface{}{
		"name":   "Charlie Wilson",
		"email":  "charlie@example.com",
		"age":    40,
		"status": "active",
	}

	updateData := map[string]interface{}{
		"age":    41,
		"status": "updated",
	}

	lastID, err := pool.Write.
		DB("test_db").
		Table("users").
		Upsert(data, updateData)

	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	t.Logf("Upserted user with ID: %d", lastID)

	// 測試更新現有資料
	data["age"] = 42
	lastID2, err := pool.Write.
		DB("test_db").
		Table("users").
		Upsert(data, updateData)

	if err != nil {
		t.Fatalf("Second upsert failed: %v", err)
	}

	t.Logf("Second upsert returned ID: %d", lastID2)
}

func TestJoinQuery(t *testing.T) {
	// 先插入一些 profile 資料
	profiles := []map[string]interface{}{
		{
			"user_id":   1,
			"bio":       "Software Engineer",
			"is_public": true,
		},
		{
			"user_id":   2,
			"bio":       "Designer",
			"is_public": true,
		},
	}

	for _, profile := range profiles {
		_, err := pool.Write.
			DB("test_db").
			Table("profiles").
			Insert(profile)
		if err != nil {
			t.Logf("Profile insert warning: %v", err)
		}
	}

	// 執行 JOIN 查詢
	rows, err := pool.Read.
		DB("test_db").
		Table("users").
		Select("users.name", "users.email", "profiles.bio").
		LeftJoin("profiles", "users.id", "profiles.user_id").
		Where("users.status", "active").
		OrderBy("users.name", "ASC").
		Get()

	if err != nil {
		t.Fatalf("JOIN query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var name, email string
		var bio *string // 使用指標因為可能是 NULL
		err := rows.Scan(&name, &email, &bio)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		bioStr := "No bio"
		if bio != nil {
			bioStr = *bio
		}

		t.Logf("User: %s (%s) - Bio: %s", name, email, bioStr)
		count++
	}

	t.Logf("Found %d users with profiles", count)
}

func TestSelectWithTotal(t *testing.T) {
	rows, err := pool.Read.
		DB("test_db").
		Table("users").
		Select("id", "name", "email").
		Where("status", "active").
		Total().
		Limit(2).
		Get()

	if err != nil {
		t.Fatalf("Select with total failed: %v", err)
	}
	defer rows.Close()

	var total int
	count := 0
	for rows.Next() {
		var id int
		var name, email string
		err := rows.Scan(&total, &id, &name, &email)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("User: ID=%d, Name=%s, Email=%s (Total: %d)", id, name, email, total)
		count++
	}

	t.Logf("Retrieved %d users out of %d total", count, total)
}

func TestDirectQuery(t *testing.T) {
	// 測試直接 SQL 查詢
	rows, err := pool.Read.Query("SELECT COUNT(*) as user_count FROM test_db.users WHERE status = ?", "active")
	if err != nil {
		t.Fatalf("Direct query failed: %v", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
	}

	t.Logf("Active users count: %d", count)
}

func TestDirectExec(t *testing.T) {
	// 測試直接 SQL 執行
	result, err := pool.Write.Exec("UPDATE test_db.users SET status = ? WHERE age > ?", "senior", 35)
	if err != nil {
		t.Fatalf("Direct exec failed: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}

	t.Logf("Marked %d users as senior", rowsAffected)
}

func TestIncreaseMethod(t *testing.T) {
	// 測試 Increase 方法
	result, err := pool.Write.
		DB("test_db").
		Table("users").
		Where("email", "john@example.com").
		Increase("age", 1).
		Update()

	if err != nil {
		t.Fatalf("Increase failed: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}

	t.Logf("Increased age for %d users", rowsAffected)
}

func TestSlowQueryLogging(t *testing.T) {
	// 模擬慢查詢
	rows, err := pool.Read.Query("SELECT SLEEP(0.1), 'slow query test'")
	if err != nil {
		t.Fatalf("Slow query test failed: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var sleepResult int
		var message string
		err := rows.Scan(&sleepResult, &message)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("Slow query result: %d, %s", sleepResult, message)
	}

	t.Log("Slow query test completed (check logs for slow query warning)")
}

func TestCleanup(t *testing.T) {
	// 清理測試資料
	_, err := pool.Write.Exec("DROP TABLE IF EXISTS test_db.profiles")
	if err != nil {
		t.Logf("Warning: Failed to drop profiles table: %v", err)
	}

	_, err = pool.Write.Exec("DROP TABLE IF EXISTS test_db.users")
	if err != nil {
		t.Logf("Warning: Failed to drop users table: %v", err)
	}

	_, err = pool.Write.Exec("DROP DATABASE IF EXISTS test_db")
	if err != nil {
		t.Logf("Warning: Failed to drop test database: %v", err)
	}

	t.Log("Cleanup completed")
}

func TestPoolClose(t *testing.T) {
	// 測試連接池關閉
	if pool != nil {
		err := pool.Close()
		if err != nil {
			t.Fatalf("Failed to close pool: %v", err)
		}
		t.Log("Pool closed successfully")
	}
}

// 效能測試
func BenchmarkInsert(b *testing.B) {
	// 重新初始化連接池
	config := goMysql.Config{
		Read: &goMysql.DBConfig{
			Host:       "localhost",
			Port:       3306,
			User:       "root",
			Password:   "password",
			Charset:    "utf8mb4",
			Connection: 10,
		},
		Write: &goMysql.DBConfig{
			Host:       "localhost",
			Port:       3306,
			User:       "root",
			Password:   "password",
			Charset:    "utf8mb4",
			Connection: 5,
		},
		Log: &goMysql.Log{
			Path: "./logs/mysql-pool-test",
		},
	}

	benchPool, err := goMysql.New(config)
	if err != nil {
		b.Fatal("Failed to initialize benchmark pool:", err)
	}
	defer benchPool.Close()

	// 創建測試表
	_, err = benchPool.Write.Exec(`
        CREATE DATABASE IF NOT EXISTS bench_db 
        CHARACTER SET utf8mb4 
        COLLATE utf8mb4_unicode_ci
    `)
	if err != nil {
		b.Fatal("Failed to create benchmark database:", err)
	}

	_, err = benchPool.Write.Exec(`
        CREATE TABLE IF NOT EXISTS bench_db.bench_users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)
	if err != nil {
		b.Fatal("Failed to create benchmark table:", err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			data := map[string]interface{}{
				"name":  fmt.Sprintf("BenchUser_%d_%d", b.N, i),
				"email": fmt.Sprintf("bench_%d_%d@example.com", b.N, i),
			}

			_, err := benchPool.Write.
				DB("bench_db").
				Table("bench_users").
				Insert(data)

			if err != nil {
				b.Errorf("Benchmark insert failed: %v", err)
			}
			i++
		}
	})

	// 清理
	benchPool.Write.Exec("DROP TABLE IF EXISTS bench_db.bench_users")
	benchPool.Write.Exec("DROP DATABASE IF EXISTS bench_db")
}

func main() {
	fmt.Println("Running MySQL Pool tests...")
	fmt.Println("Make sure you have MySQL running with the correct credentials!")
	fmt.Println("Run with: go test -v")
}
