package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os" // For os.Exit
	"path/filepath"

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
	scanType := flag.String("scan-type", "student", "Type of scan to process (student, professional, cis, or all)")
	days := flag.Int("days", 3, "Number of days back to process data for")
	teamID := flag.Int("team", 0, "Specific team ID to process (optional)") // Use 0 as a sentinel for 'not set'
	force := flag.Bool("force", false, "Force reprocessing even if data seems up-to-date")
	debug := flag.Bool("debug", false, "Enable debug mode for query logging")

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
	if *force {
		fmt.Println("Force: true")
	}
	if *debug {
		fmt.Println("Debug: true")
	}
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
		case "cis":
			processScans(proc.NewCISScanProcessor(), config, db)
		case "global":
			processScans(proc.NewGlobalScanProcessor(), config, db)
		case "parent":
			processScans(proc.NewParentScanProcessor(), config, db)
		case "ontario-student":
			processScans(proc.NewOntarioStudentScanProcessor(), config, db)
		case "ontario-parent":
			processScans(proc.NewOntarioParentScanProcessor(), config, db)
		case "ontario-counsellor":
			processScans(proc.NewOntarioCounsellorScanProcessor(), config, db)
		case "all":
			// fmt.Println("\nProcessing student scans...")
			// processScans(proc.NewStudentScanProcessor(), config, db)
			// fmt.Println("\nProcessing CIS scans...")
			// processScans(proc.NewCISScanProcessor(), config, db)
			// processScans(proc.NewLindenScanProcessor(), config, db)
			// processScans(proc.NewLindenBoardingScanProcessor(), config, db)
			// fmt.Println("\nProcessing Global scans...")
			// processScans(proc.NewGlobalScanProcessor(), config, db)
			// fmt.Println("\nProcessing Professional scans...")
			// processScans(proc.NewProfessionalScanProcessor(), config, db)
			// fmt.Println("\nProcessing Parent scans...")
			// processScans(proc.NewParentScanProcessor(), config, db)
			// fmt.Println("\nProcessing Ontario student scans...")
			// processScans(proc.NewOntarioStudentScanProcessor(), config, db)
			// fmt.Println("\nProcessing Ontario parent scans...")
			// processScans(proc.NewOntarioParentScanProcessor(), config, db)
			// processScans(proc.NewOntarioCounsellorScanProcessor(), config, db)
		default:
			color.Red("Invalid scan type specified: %s. Use 'student', 'professional', 'cis', 'ontario_student', 'ontario_parent', 'global', or 'all'.", *scanType)
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

	// Process files with SFTP processor
	sftpProcessor := proc.NewSFTPProcessor(db)
	// Get all files in output directory
	createdFilePaths := []string{}
	err = filepath.Walk("output", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			createdFilePaths = append(createdFilePaths, path)
		}
		return nil
	})

	if err != nil {
		color.Red("Error walking output directory: %v", err)
		os.Exit(1)
	}

	if len(createdFilePaths) == 0 {
		color.Yellow("No files found in output directory")
		os.Exit(0)
	}

	if err := sftpProcessor.Run(createdFilePaths, len(createdFilePaths)); err != nil {
		color.Red("Error uploading files via SFTP: %v", err)
		os.Exit(1)
	}
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
