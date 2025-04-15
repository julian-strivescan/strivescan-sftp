package main

import (
	"fmt"

	figure "github.com/common-nighthawk/go-figure"
	"github.com/fatih/color"
)

func main() {
	myFigure := figure.NewFigure("StriveScan SFTP", "", true)
	cyan := color.New(color.FgCyan).SprintFunc()
	fmt.Println(cyan(myFigure.String()))

	fmt.Println("\nStarting MySQL to CSV processing...")
}
