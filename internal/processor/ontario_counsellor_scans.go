package processor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/strivescan/strivescan-sftp/internal/models"
)

// OntarioCounsellorScanProcessor handles processing of Ontario Counsellor scan data (type 10).
type OntarioCounsellorScanProcessor struct {
	*BaseProcessor
}

// NewOntarioCounsellorScanProcessor creates a new OntarioCounsellorScanProcessor
func NewOntarioCounsellorScanProcessor() *OntarioCounsellorScanProcessor {
	return &OntarioCounsellorScanProcessor{
		BaseProcessor: NewBaseProcessor(10),
	}
}

func (ocp *OntarioCounsellorScanProcessor) GetCSVHeader() []string {
	return []string{
		"Event Name",
		"Internal Event ID",
		"First Name",
		"Last Name",
		"Email",
		"Address City",
		"Address Province",
		"Address Postal Code",
		"Address Country",
		"School",
		"School City",
		"School Province",
		"CEEB Code",
		"Organization",
		"Professional Type",
		"Job Title",
		"Rating",
		"Notes",
		"Follow Up",
		"Registration Language",
		"Scan Time",
		"Scan Rep",
		"Event Guide",
		"Updated Time",
	}
}

// FetchData retrieves Ontario Counsellor scan data (type 10) from the database.
func (ocp *OntarioCounsellorScanProcessor) FetchData(db *sql.DB, config Config) (interface{}, error) {
	fmt.Println("Fetching Ontario Counsellor scan data (type 10)...")

	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	var query strings.Builder
	args := []interface{}{config.Days, ocp.scanTypeID}

	query.WriteString(ocp.GetScanQuery())

	if config.TeamID != 0 {
		query.WriteString(" AND t.id = ?")
		args = append(args, config.TeamID)
	}

	query.WriteString(ocp.GetScanQueryGroupBy())

	finalQuery := query.String()
	fmt.Printf("Executing Query:\n%s\nArgs: %v\n", finalQuery, args)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	rows, err := db.QueryContext(ctx, finalQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("database query failed: %w", err)
	}
	defer rows.Close()

	results := []models.StudentScanData{}
	for rows.Next() {
		var scanData models.StudentScanData
		err := rows.Scan(
			&scanData.TeamID,
			&scanData.TeamName,
			&scanData.InternalEventID,
			&scanData.FairID,
			&scanData.FairName,
			&scanData.FairDate,
			&scanData.StudentID,
			&scanData.FirstName,
			&scanData.LastName,
			&scanData.Email,
			&scanData.Phone,
			&scanData.PhoneNumber,
			&scanData.AddressLine1,
			&scanData.AddressLine2,
			&scanData.AddressCity,
			&scanData.AddressState,
			&scanData.AddressZipcode,
			&scanData.AddressCountryCode,
			&scanData.Locale,
			&scanData.ScanTime,
			&scanData.UpdatedTime,
			&scanData.ParentEncountered,
			&scanData.HighSchool,
			&scanData.GraduationYear,
			&scanData.GPA,
			&scanData.AreaOfInterest1,
			&scanData.AreaOfInterest2,
			&scanData.AreaOfInterest3,
			&scanData.Birthdate,
			&scanData.SatScore,
			&scanData.ActScore,
			&scanData.TextPermission,
			&scanData.HighSchoolCity,
			&scanData.HighSchoolRegion,
			&scanData.CollegeStartSemester,
			&scanData.GPAMax,
			&scanData.GradType,
			&scanData.CEEB,
			&scanData.HasHispanicLatinoOrigin,
			&scanData.CurrentYearClass,
			&scanData.HighSchoolCountry,
			&scanData.CountryOfCitizenship1,
			&scanData.CountryOfCitizenship2,
			&scanData.CountryOfCitizenship3,
			&scanData.Gender,
			&scanData.GuidanceCounselorFirstName,
			&scanData.GuidanceCounselorLastName,
			&scanData.GuidanceCounselorEmail,
			&scanData.CountryOfInterest1,
			&scanData.CountryOfInterest2,
			&scanData.CountryOfInterest3,
			&scanData.AuthorizeCIS,
			&scanData.TOEFLScore,
			&scanData.IELTSScore,
			&scanData.SSATScore,
			&scanData.ProfessionalType,
			&scanData.PreferredName,
			&scanData.Pronouns,
			&scanData.JobTitle,
			&scanData.WorkPhone,
			&scanData.WorkPhoneExt,
			&scanData.WorkPhoneCountryCode,
			&scanData.Organization,
			&scanData.AdditionalData1,
			&scanData.AdditionalData2,
			&scanData.AdditionalData3,
			&scanData.AdditionalData4,
			&scanData.AdditionalData5,
			&scanData.AdditionalData6,
			&scanData.AdditionalData7,
			&scanData.AdditionalData8,
			&scanData.AdditionalData9,
			&scanData.AdditionalData10,
			&scanData.ParentFirstName,
			&scanData.ParentLastName,
			&scanData.ParentPhone,
			&scanData.ParentPhoneCountryCode,
			&scanData.ParentEmail,
			&scanData.ParentRelationship,
			&scanData.EthnicityCuban,
			&scanData.EthnicityMexican,
			&scanData.EthnicityPuertoRican,
			&scanData.EthnicityOtherHispanicLatinoOrSpanish,
			&scanData.EthnicityNonHispanicLatinoOrSpanish,
			&scanData.RaceAmericanIndianOrAlaskanNative,
			&scanData.RaceAsian,
			&scanData.RaceBlackOrAfricanAmerican,
			&scanData.RaceNativeHawaiianOrOtherPacificIslander,
			&scanData.RaceWhite,
			&scanData.ScanRep,
			&scanData.Notes,
			&scanData.Rating,
			&scanData.FollowUp,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		results = append(results, scanData)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	fmt.Printf("--- Fetched %d records from database ---\n", len(results))
	return results, nil
}

// TransformData groups Ontario Counsellor scan data by TeamID and prepares it for CSV.
func (ocp *OntarioCounsellorScanProcessor) TransformData(data interface{}) (map[int64][][]string, error) {
	fmt.Println("Transforming and grouping Ontario Counsellor scan data by TeamID...")
	scans, ok := data.([]models.StudentScanData)
	if !ok {
		return nil, fmt.Errorf("invalid data type for Ontario Counsellor scan transformation, expected []models.StudentScanData")
	}

	// Map to hold data grouped by team ID
	groupedData := make(map[int64][][]string)

	// Convert scans to string slices and group by TeamID
	for _, scan := range scans {
		// Get the data slice for the current team, initializing if needed
		teamData, exists := groupedData[scan.TeamID]
		if !exists {
			// Initialize with the header row
			teamData = [][]string{ocp.GetCSVHeader()}
		}
		// Append the current row
		teamData = append(teamData, ocp.TransformScanToRow(scan))
		groupedData[scan.TeamID] = teamData
	}

	fmt.Printf("Data grouped into %d teams.\n", len(groupedData))
	return groupedData, nil
}

func (ocp *OntarioCounsellorScanProcessor) TransformScanToRow(scan models.StudentScanData) []string {
	return []string{
		scan.FairName,
		ocp.nullStr(scan.InternalEventID),
		ocp.nullStr(scan.FirstName),
		ocp.nullStr(scan.LastName),
		ocp.nullStr(scan.Email),
		ocp.nullStr(scan.AddressCity),
		ocp.nullStr(scan.AddressState),
		ocp.nullStr(scan.AddressZipcode),
		ocp.nullStr(scan.AddressCountryCode),
		ocp.nullStr(scan.HighSchool),
		ocp.nullStr(scan.HighSchoolCity),
		ocp.nullStr(scan.HighSchoolRegion),
		ocp.nullStr(scan.CEEB),
		ocp.nullStr(scan.Organization),
		ocp.nullStr(scan.ProfessionalType),
		ocp.nullStr(scan.JobTitle),
		ocp.nullInt(scan.Rating),
		ocp.nullStr(scan.Notes),
		ocp.nullBool(scan.FollowUp),
		ocp.nullStr(scan.Locale),
		ocp.nullTime(scan.ScanTime, "2006-01-02 15:04:05"),
		ocp.nullStr(scan.ScanRep),
		ocp.nullStr(scan.EventGuideFavourite),
		ocp.nullTime(scan.UpdatedTime, "2006-01-02 15:04:05"),
	}
}

// WriteCSV saves the grouped Ontario Counsellor scan data to team-specific CSV files.
func (ocp *OntarioCounsellorScanProcessor) WriteCSV(groupedData map[int64][][]string, config Config) ([]string, error) {
	fmt.Println("Writing Ontario Counsellor scan data to team-specific CSV files...")
	if len(groupedData) == 0 {
		fmt.Println("No data groups to write.")
		return []string{}, nil
	}

	createdFiles := []string{}
	baseOutputDir := "output"
	timestamp := time.Now().Format("20060102_150405")

	for teamID, teamData := range groupedData {
		fp, err := ocp.WriteCSVFile(teamID, teamData, baseOutputDir, timestamp)
		if err != nil {
			return createdFiles, err
		}
		fmt.Printf("Successfully wrote %d data rows for Team %d to: %s\n", len(teamData)-1, teamID, fp)
		createdFiles = append(createdFiles, fp)
	}

	return createdFiles, nil
}
