package processor

import (
	"database/sql" // Placeholder for DB connection
)

// Config holds the parameters for data processing.
type Config struct {
	Days   int
	TeamID int // 0 means not specified
	Force  bool
	Type   string // "scans" or "connections"
}

// DataProcessor defines the interface for processing different data types.
type DataProcessor interface {
	// FetchData retrieves data from the database based on the config.
	FetchData(db *sql.DB, config Config) (interface{}, error) // We'll refine the return type later
	// TransformData converts the fetched data into the desired CSV format.
	TransformData(data interface{}) ([][]string, error) // Returns slice of string slices for CSV rows
	// WriteCSV saves the transformed data to a CSV file.
	WriteCSV(data [][]string, config Config) (string, error) // Returns the path to the generated file
}
