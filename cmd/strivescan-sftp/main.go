package main

import (
	"flag"
	"fmt"

	figure "github.com/common-nighthawk/go-figure"
	"github.com/fatih/color"
	_ "github.com/strivescan/strivescan-sftp/internal/processor" // Import for side effects or future use
)

func main() {
	// --- Banner ---
	myFigure := figure.NewFigure("StriveScan SFTP", "", true)
	cyan := color.New(color.FgCyan).SprintFunc()
	fmt.Println(cyan(myFigure.String()))

	// --- Flags ---
	dataType := flag.String("type", "scans", "Type of data to process (scans or connections)")
	days := flag.Int("days", 3, "Number of days back to process data for")
	teamID := flag.Int("team", 0, "Specific team ID to process (optional)") // Use 0 as a sentinel for 'not set'
	force := flag.Bool("force", false, "Force reprocessing even if data seems up-to-date")

	flag.Parse() // Parse the flags

	// --- Print Parsed Flags ---
	fmt.Println("\n--- Configuration ---")
	fmt.Printf("Type: %s\n", *dataType)
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

	// TODO: Call appropriate function from processor package based on *dataType

	fmt.Println("\nProcessing complete.")
}
