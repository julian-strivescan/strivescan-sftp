package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os" // For os.Exit

	figure "github.com/common-nighthawk/go-figure"
	"github.com/fatih/color"
	"github.com/joho/godotenv"                                // Added godotenv
	"github.com/strivescan/strivescan-sftp/internal/database" // Added database import
	proc "github.com/strivescan/strivescan-sftp/internal/processor"
)

func main() {
	// Load .env file. Ignore error if it doesn't exist.
	err := godotenv.Load()
	if err != nil && !os.IsNotExist(err) {
		// Only log fatal if it's an error other than file not existing
		log.Fatalf("Error loading .env file: %v", err)
	}

	// --- Banner ---
	myFigure := figure.NewFigure("StriveScan SFTP", "", true)
	cyan := color.New(color.FgCyan).SprintFunc()
	fmt.Println(cyan(myFigure.String()))

	// --- Flags ---
	dataType := flag.String("type", "scans", "Type of data to process (scans or connections)")
	scanType := flag.String("scan-type", "all", "Type of scan to process (student, professional, or all)")
	days := flag.Int("days", 3, "Number of days back to process data for")
	teamID := flag.Int("team", 0, "Specific team ID to process (optional)") // Use 0 as a sentinel for 'not set'
	force := flag.Bool("force", false, "Force reprocessing even if data seems up-to-date")

	flag.Parse() // Parse the flags

	// --- Database Connection ---
	fmt.Println("\nConnecting to database...")
	db, err := database.ConnectDB()
	if err != nil {
		color.Red("Database connection failed: %v", err) // Use color for errors
		os.Exit(1)                                       // Exit if DB connection fails
	}
	defer db.Close() // Ensure DB pool is closed when main exits

	// --- Print Parsed Flags ---
	fmt.Println("\n--- Configuration ---")
	fmt.Printf("Type: %s\n", *dataType)
	if *dataType == "scans" {
		fmt.Printf("Scan Type: %s\n", *scanType)
	}
	fmt.Printf("Days: %d\n", *days)
	if *teamID != 0 {
		fmt.Printf("Team ID: %d\n", *teamID)
	} else {
		fmt.Println("Team ID: (Not specified)")
	}
	fmt.Printf("Force: %t\n", *force)
	fmt.Println("---------------------")

	// --- Determine Action based on Type ---
	fmt.Printf("\nProcessing data type: %s\n", *dataType)

	if *dataType == "scans" {
		config := proc.Config{
			Days:   *days,
			TeamID: *teamID,
			Force:  *force,
			Type:   *dataType,
		}

		// Process scans based on scan type
		switch *scanType {
		case "student":
			processScans(proc.NewStudentScanProcessor(), config, db)
		case "professional":
			processScans(proc.NewProfessionalScanProcessor(), config, db)
		case "all":
			fmt.Println("\nProcessing student scans...")
			processScans(proc.NewStudentScanProcessor(), config, db)
			fmt.Println("\nProcessing professional scans...")
			processScans(proc.NewProfessionalScanProcessor(), config, db)
		default:
			color.Red("Invalid scan type specified: %s. Use 'student', 'professional', or 'all'.", *scanType)
			os.Exit(1)
		}
	} else if *dataType == "connections" {
		color.Yellow("Connection processing not yet implemented.")
		// TODO: Implement connection processing similar to scans
	} else {
		color.Red("Invalid data type specified: %s. Use 'scans' or 'connections'.", *dataType)
		os.Exit(1)
	}

	color.Green("\nProcessing complete.")
}

// processScans handles the common processing logic for both student and professional scans
func processScans(processor proc.DataProcessor, config proc.Config, db *sql.DB) {
	// Fetch data
	rawData, err := processor.FetchData(db, config)
	if err != nil {
		color.Red("Error fetching data: %v", err)
		os.Exit(1)
	}

	// Transform data into grouped map
	groupedCsvData, err := processor.TransformData(rawData)
	if err != nil {
		color.Red("Error transforming data: %v", err)
		os.Exit(1)
	}

	// Write CSV files per team
	createdFilePaths, err := processor.WriteCSV(groupedCsvData, config)
	if err != nil {
		color.Red("Error writing CSV files: %v", err)
		os.Exit(1)
	}

	if len(createdFilePaths) > 0 {
		color.Green("CSV files successfully generated:")
		for _, fp := range createdFilePaths {
			color.Green("- %s", fp)
		}
	} else {
		color.Yellow("No data found for the specified criteria, no CSV files generated.")
	}
}
