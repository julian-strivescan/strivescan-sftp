package database

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
)

// ConnectDB establishes a connection pool to the MySQL database.
func ConnectDB() (*sql.DB, error) {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		return nil, fmt.Errorf("DB_DSN environment variable not set")
	}

	// Append parseTime=true to handle TIME, DATE, DATETIME, TIMESTAMP correctly.
	// Adjust other parameters (timeout, etc.) as needed.
	db, err := sql.Open("mysql", dsn+"?parseTime=true")
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Configure connection pool settings (optional but recommended)
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	// Verify the connection is alive
	err = db.Ping()
	if err != nil {
		// Close the pool if ping fails to prevent resource leaks
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	fmt.Println("Successfully connected to the database.")
	return db, nil
}
