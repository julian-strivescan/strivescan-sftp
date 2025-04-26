package processor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/strivescan/strivescan-sftp/internal/models"
)

// OntarioStudentScanProcessor handles processing of Ontario student scan data (type 4).
type OntarioStudentScanProcessor struct {
	*BaseProcessor
}

// NewOntarioStudentScanProcessor creates a new OntarioStudentScanProcessor
func NewOntarioStudentScanProcessor() *OntarioStudentScanProcessor {
	return &OntarioStudentScanProcessor{
		BaseProcessor: NewBaseProcessor(4),
	}
}

func (osp *OntarioStudentScanProcessor) GetCSVHeader() []string {
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
		"Birthdate",
		"University Start",
		"Rating",
		"Notes",
		"Follow Up",
		"Parent or Student",
		"Registration Language",
		"Scan Time",
		"Scan Rep",
		"Event Guide",
		"Updated Time",
	}
}

// FetchData retrieves Ontario student scan data (type 4) from the database.
func (osp *OntarioStudentScanProcessor) FetchData(db *sql.DB, config Config) (interface{}, error) {
	fmt.Println("Fetching Ontario student scan data (type 4)...")

	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	var query strings.Builder
	args := []interface{}{config.Days, osp.scanTypeID}

	query.WriteString(osp.GetScanQuery())

	if config.TeamID != 0 {
		query.WriteString(" AND t.id = ?")
		args = append(args, config.TeamID)
	}

	query.WriteString(osp.GetScanQueryGroupBy())

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
			&scanData.PhoneNumberFormatted,
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

// TransformData groups Ontario student scan data by TeamID and prepares it for CSV.
func (osp *OntarioStudentScanProcessor) TransformData(data interface{}) (map[int64][][]string, error) {
	fmt.Println("Transforming and grouping Ontario student scan data by TeamID...")
	scans, ok := data.([]models.StudentScanData)
	if !ok {
		return nil, fmt.Errorf("invalid data type for Ontario student scan transformation, expected []models.StudentScanData")
	}

	// Map to hold data grouped by team ID
	groupedData := make(map[int64][][]string)

	// Convert scans to string slices and group by TeamID
	for _, scan := range scans {
		// Get the data slice for the current team, initializing if needed
		teamData, exists := groupedData[scan.TeamID]
		if !exists {
			// Initialize with the header row
			teamData = [][]string{osp.GetCSVHeader()}
		}
		// Append the current row
		teamData = append(teamData, osp.TransformScanToRow(scan))
		groupedData[scan.TeamID] = teamData
	}

	fmt.Printf("Data grouped into %d teams.\n", len(groupedData))
	return groupedData, nil
}

func (osp *OntarioStudentScanProcessor) TransformScanToRow(scan models.StudentScanData) []string {
	return []string{
		scan.FairName,
		osp.nullStr(scan.InternalEventID),
		osp.nullStr(scan.FirstName),
		osp.nullStr(scan.LastName),
		osp.nullStr(scan.Email),
		osp.nullStr(scan.AddressCity),
		osp.nullStr(scan.AddressState),
		osp.nullStr(scan.AddressZipcode),
		osp.nullStr(scan.AddressCountryCode),
		osp.nullStr(scan.Birthdate),
		osp.nullStr(scan.CollegeStartSemester),
		osp.nullInt(scan.Rating),
		osp.nullStr(scan.Notes),
		osp.nullBool(scan.FollowUp),
		func() string {
			if scan.ParentEncountered.Valid && scan.ParentEncountered.Bool {
				return "Parent"
			}
			return "Student"
		}(),
		osp.nullStr(scan.Locale),
		osp.nullTime(scan.ScanTime, "2006-01-02 15:04:05"),
		osp.nullStr(scan.ScanRep),
		osp.nullStr(scan.EventGuideFavourite),
		osp.nullTime(scan.UpdatedTime, "2006-01-02 15:04:05"),
	}
}

// WriteCSV saves the grouped Ontario student scan data to team-specific CSV files.
func (osp *OntarioStudentScanProcessor) WriteCSV(groupedData map[int64][][]string, config Config) ([]string, error) {
	fmt.Println("Writing Ontario student scan data to team-specific CSV files...")
	if len(groupedData) == 0 {
		fmt.Println("No data groups to write.")
		return []string{}, nil
	}

	createdFiles := []string{}
	baseOutputDir := "output"
	timestamp := time.Now().Format("20060102_150405")

	for teamID, teamData := range groupedData {
		fp, err := osp.WriteCSVFile(teamID, teamData, baseOutputDir, timestamp)
		if err != nil {
			return createdFiles, err
		}
		fmt.Printf("Successfully wrote %d data rows for Team %d to: %s\n", len(teamData)-1, teamID, fp)
		createdFiles = append(createdFiles, fp)
	}

	return createdFiles, nil
}
