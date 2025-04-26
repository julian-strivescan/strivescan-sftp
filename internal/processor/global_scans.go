package processor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/strivescan/strivescan-sftp/internal/models"
)

// GlobalScanProcessor handles processing of global scan data (type 2).
type GlobalScanProcessor struct {
	*BaseProcessor
}

// NewGlobalScanProcessor creates a new GlobalScanProcessor
func NewGlobalScanProcessor() *GlobalScanProcessor {
	return &GlobalScanProcessor{
		BaseProcessor: NewBaseProcessor(2),
	}
}

func (gp *GlobalScanProcessor) GetCSVHeader() []string {
	return []string{
		"Event",
		"Internal Event ID",
		"First Name",
		"Last Name",
		"Email",
		"Phone",
		"Formatted Phone",
		"Text Permission",
		"Address 1",
		"Address 2",
		"Address Municipality",
		"Address Locality",
		"Address Region",
		"Address Postal Code",
		"Address Country",
		"Birthdate",
		"High School",
		"High School City",
		"High School Region",
		"High School Country",
		"CEEB Code",
		"Graduation Year",
		"University Start",
		"GPA",
		"GPA Max",
		"SAT",
		"ACT",
		"TOEFL",
		"IELTS",
		"Area of Interest 1",
		"Area of Interest 2",
		"Area of Interest 3",
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

// FetchData retrieves global scan data (type 2) from the database.
func (gp *GlobalScanProcessor) FetchData(db *sql.DB, config Config) (interface{}, error) {
	fmt.Println("Fetching global scan data (type 2)...")

	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	var query strings.Builder
	args := []interface{}{config.Days, gp.scanTypeID}

	query.WriteString(gp.GetScanQuery())

	if config.TeamID != 0 {
		query.WriteString(" AND t.id = ?")
		args = append(args, config.TeamID)
	}

	query.WriteString(gp.GetScanQueryGroupBy())

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

func (gp *GlobalScanProcessor) TransformScanToRow(scan models.StudentScanData) []string {
	return []string{
		scan.FairName,
		gp.nullStr(scan.InternalEventID),
		gp.nullStr(scan.FirstName),
		gp.nullStr(scan.LastName),
		gp.nullStr(scan.Email),
		gp.nullStr(scan.PhoneNumber),
		gp.nullStr(scan.PhoneNumber),
		func() string {
			if scan.TextPermission.Valid && scan.TextPermission.String == "1" {
				return "Yes"
			}
			return "No"
		}(),
		gp.nullStr(scan.AddressLine1),
		gp.nullStr(scan.AddressLine2),
		gp.nullStr(scan.AddressCity),
		gp.nullStr(scan.AddressState),
		gp.nullStr(scan.AddressState),
		gp.nullStr(scan.AddressZipcode),
		gp.nullStr(scan.AddressCountryCode),
		gp.nullStr(scan.Birthdate),
		gp.nullStr(scan.HighSchool),
		gp.nullStr(scan.HighSchoolCity),
		gp.nullStr(scan.HighSchoolRegion),
		gp.nullStr(scan.HighSchoolCountry),
		gp.nullStr(scan.CEEB),
		gp.nullStr(scan.GraduationYear),
		gp.nullStr(scan.CollegeStartSemester),
		gp.nullStr(scan.GPA),
		gp.nullStr(scan.GPAMax),
		gp.nullStr(scan.SatScore),
		gp.nullStr(scan.ActScore),
		gp.nullStr(scan.TOEFLScore),
		gp.nullStr(scan.IELTSScore),
		gp.nullStr(scan.AreaOfInterest1),
		gp.nullStr(scan.AreaOfInterest2),
		gp.nullStr(scan.AreaOfInterest3),
		gp.nullInt(scan.Rating),
		gp.nullStr(scan.Notes),
		gp.nullBool(scan.FollowUp),
		func() string {
			if scan.ParentEncountered.Valid && scan.ParentEncountered.Bool {
				return "Parent"
			}
			return "Student"
		}(),
		gp.nullStr(scan.Locale),
		gp.nullTime(scan.ScanTime, "2006-01-02 15:04:05"),
		gp.nullStr(scan.ScanRep),
		gp.nullStr(scan.EventGuideFavourite),
		gp.nullTime(scan.UpdatedTime, "2006-01-02 15:04:05"),
	}
}

// TransformData groups global scan data by TeamID and prepares it for CSV.
func (gp *GlobalScanProcessor) TransformData(data interface{}) (map[int64][][]string, error) {
	fmt.Println("Transforming and grouping global scan data by TeamID...")
	scans, ok := data.([]models.StudentScanData)
	if !ok {
		return nil, fmt.Errorf("invalid data type for global scan transformation, expected []models.StudentScanData")
	}

	// Map to hold data grouped by team ID
	groupedData := make(map[int64][][]string)

	// Convert scans to string slices and group by TeamID
	for _, scan := range scans {
		// Get the data slice for the current team, initializing if needed
		teamData, exists := groupedData[scan.TeamID]
		if !exists {
			// Initialize with the header row
			teamData = [][]string{gp.GetCSVHeader()}
		}
		// Append the current row
		teamData = append(teamData, gp.TransformScanToRow(scan))
		groupedData[scan.TeamID] = teamData
	}

	fmt.Printf("Data grouped into %d teams.\n", len(groupedData))
	return groupedData, nil
}

// WriteCSV saves the grouped global scan data to team-specific CSV files.
func (gp *GlobalScanProcessor) WriteCSV(groupedData map[int64][][]string, config Config) ([]string, error) {
	fmt.Println("Writing global scan data to team-specific CSV files...")
	if len(groupedData) == 0 {
		fmt.Println("No data groups to write.")
		return []string{}, nil
	}

	createdFiles := []string{}
	baseOutputDir := "output"
	timestamp := time.Now().Format("20060102_150405")

	for teamID, teamData := range groupedData {
		fp, err := gp.WriteCSVFile(teamID, teamData, baseOutputDir, timestamp)
		if err != nil {
			return createdFiles, err
		}
		fmt.Printf("Successfully wrote %d data rows for Team %d to: %s\n", len(teamData)-1, teamID, fp)
		createdFiles = append(createdFiles, fp)
	}

	return createdFiles, nil
}
