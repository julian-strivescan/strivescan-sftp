package processor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/strivescan/strivescan-sftp/internal/models"
)

// ParentScanProcessor handles processing of parent scan data (type 3).
type ParentScanProcessor struct {
	*BaseProcessor
}

// NewParentScanProcessor creates a new ParentScanProcessor
func NewParentScanProcessor() *ParentScanProcessor {
	return &ParentScanProcessor{
		BaseProcessor: NewBaseProcessor(3),
	}
}

func (pp *ParentScanProcessor) GetCSVHeader() []string {
	return []string{
		"Fair Name",
		"Internal Event ID",
		"Relationship to Student",
		"Parent First Name",
		"Parent Last Name",
		"Parent Email Address",
		"Phone",
		"Text Permission",
		"Student First Name",
		"Student Last Name",
		"Student Email Address",
		"Birthdate",
		"High School",
		"High School City",
		"High School State",
		"CEEB Code",
		"Graduation Year",
		"College Start",
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

// FetchData retrieves parent scan data (type 3) from the database.
func (pp *ParentScanProcessor) FetchData(db *sql.DB, config Config) (interface{}, error) {
	fmt.Println("Fetching parent scan data (type 3)...")

	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	var query strings.Builder
	args := []interface{}{config.Days, pp.scanTypeID}

	query.WriteString(pp.GetScanQuery())

	if config.TeamID != 0 {
		query.WriteString(" AND t.id = ?")
		args = append(args, config.TeamID)
	}

	query.WriteString(pp.GetScanQueryGroupBy())

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
			&scanData.EventGuideFavourite,
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

// TransformData groups parent scan data by TeamID and prepares it for CSV.
func (pp *ParentScanProcessor) TransformData(data interface{}) (map[int64][][]string, error) {
	fmt.Println("Transforming and grouping parent scan data by TeamID...")
	scans, ok := data.([]models.StudentScanData)
	if !ok {
		return nil, fmt.Errorf("invalid data type for parent scan transformation, expected []models.StudentScanData")
	}

	// Map to hold data grouped by team ID
	groupedData := make(map[int64][][]string)

	// Convert scans to string slices and group by TeamID
	for _, scan := range scans {
		// Get the data slice for the current team, initializing if needed
		teamData, exists := groupedData[scan.TeamID]
		if !exists {
			// Initialize with the header row
			teamData = [][]string{pp.GetCSVHeader()}
		}
		// Append the current row
		teamData = append(teamData, pp.TransformScanToRow(scan))
		groupedData[scan.TeamID] = teamData
	}

	fmt.Printf("Data grouped into %d teams.\n", len(groupedData))
	return groupedData, nil
}

func (pp *ParentScanProcessor) TransformScanToRow(scan models.StudentScanData) []string {
	return []string{
		scan.FairName,
		pp.nullStr(scan.InternalEventID),
		pp.nullStr(scan.ParentRelationship),
		pp.nullStr(scan.ParentFirstName),
		pp.nullStr(scan.ParentLastName),
		pp.nullStr(scan.ParentEmail),
		pp.nullStr(scan.Phone),
		func() string {
			if scan.TextPermission.Valid && scan.TextPermission.String == "1" {
				return "Yes"
			}
			return "No"
		}(),
		pp.nullStr(scan.FirstName),
		pp.nullStr(scan.LastName),
		pp.nullStr(scan.Email),
		pp.nullStr(scan.Birthdate),
		pp.nullStr(scan.HighSchool),
		pp.nullStr(scan.HighSchoolCity),
		pp.nullStr(scan.HighSchoolRegion),
		pp.nullStr(scan.CEEB),
		pp.nullStr(scan.GraduationYear),
		pp.nullStr(scan.CollegeStartSemester),
		pp.nullInt(scan.Rating),
		pp.nullStr(scan.Notes),
		pp.nullBool(scan.FollowUp),
		func() string {
			if scan.Locale.Valid {
				return scan.Locale.String
			}
			return "en"
		}(),
		pp.nullTime(scan.ScanTime, "2006-01-02 15:04:05"),
		pp.nullStr(scan.ScanRep),
		pp.nullStr(scan.EventGuideFavourite),
		pp.nullTime(scan.UpdatedTime, "2006-01-02 15:04:05"),
	}
}

// WriteCSV saves the grouped parent scan data to team-specific CSV files.
func (pp *ParentScanProcessor) WriteCSV(groupedData map[int64][][]string, config Config) ([]string, error) {
	fmt.Println("Writing parent scan data to team-specific CSV files...")
	if len(groupedData) == 0 {
		fmt.Println("No data groups to write.")
		return []string{}, nil
	}

	createdFiles := []string{}
	baseOutputDir := "output"
	timestamp := time.Now().Format("20060102_150405")

	for teamID, teamData := range groupedData {
		fp, err := pp.WriteCSVFile(teamID, teamData, baseOutputDir, timestamp)
		if err != nil {
			return createdFiles, err
		}
		fmt.Printf("Successfully wrote %d data rows for Team %d to: %s\n", len(teamData)-1, teamID, fp)
		createdFiles = append(createdFiles, fp)
	}

	return createdFiles, nil
}
