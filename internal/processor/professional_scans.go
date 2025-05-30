package processor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/strivescan/strivescan-sftp/internal/models"
)

// ProfessionalScanProcessor handles processing of professional scan data (type 6).
type ProfessionalScanProcessor struct {
	*BaseProcessor
}

// NewProfessionalScanProcessor creates a new ProfessionalScanProcessor
func NewProfessionalScanProcessor() *ProfessionalScanProcessor {
	return &ProfessionalScanProcessor{
		BaseProcessor: NewBaseProcessor(6),
	}
}

const professionalScanQueryBase = `
SELECT 
    t.id AS team_id,
    t.name AS team_name,
	ft.guid_id as internal_event_id,
    f.id AS fair_id,
    f.name AS fair_name,
    f.starts_at AS fair_date,
    s.id AS student_id,
    s.first_name,
    s.last_name,
    s.email,
    s.phone,
    pn.number,
    a.line1 as address_line_1,
    a.line2 as address_line_2,
    a.municipality as address_city,
    a.region as address_state,
    a.postal_code as address_zipcode,
    a.country_code as address_country_code,
	s.locale as locale,
	ufs.created_at as scan_time,
	ufs.updated_at as updated_time,
	ufs.parent_encountered as parent_encountered,
    COALESCE(s.high_school, 
             MAX(CASE WHEN sa.name = 'high_school' THEN sav.value ELSE NULL END)) AS high_school,
    COALESCE(s.graduation_year, 
             MAX(CASE WHEN sa.name = 'graduation_year' THEN sav.value ELSE NULL END)) AS graduation_year,
    COALESCE(s.gpa, 
             MAX(CASE WHEN sa.name = 'gpa' THEN sav.value ELSE NULL END)) AS gpa,
    COALESCE(s.area_of_interest_1, 
             MAX(CASE WHEN sa.name = 'area_of_interest_1' THEN sav.value ELSE NULL END)) AS area_of_interest_1,
    COALESCE(s.area_of_interest_2, 
             MAX(CASE WHEN sa.name = 'area_of_interest_2' THEN sav.value ELSE NULL END)) AS area_of_interest_2,
    COALESCE(s.area_of_interest_3, 
             MAX(CASE WHEN sa.name = 'area_of_interest_3' THEN sav.value ELSE NULL END)) AS area_of_interest_3,
    MAX(CASE WHEN sa.name = 'birthdate' THEN sav.value ELSE NULL END) AS birthdate,
    MAX(CASE WHEN sa.name = 'sat_score' THEN sav.value ELSE NULL END) AS sat_score,
    MAX(CASE WHEN sa.name = 'act_score' THEN sav.value ELSE NULL END) AS act_score,
    MAX(CASE WHEN sa.name = 'text_permission' THEN sav.value ELSE NULL END) AS text_permission,
    MAX(CASE WHEN sa.name = 'high_school_city' THEN sav.value ELSE NULL END) AS high_school_city,
    MAX(CASE WHEN sa.name = 'high_school_region' THEN sav.value ELSE NULL END) AS high_school_region,
    MAX(CASE WHEN sa.name = 'college_start_semester' THEN sav.value ELSE NULL END) AS college_start_semester,
    MAX(CASE WHEN sa.name = 'gpa_max' THEN sav.value ELSE NULL END) AS gpa_max,
    MAX(CASE WHEN sa.name = 'grad_type' THEN sav.value ELSE NULL END) AS grad_type,
    MAX(CASE WHEN sa.name = 'CEEB' THEN sav.value ELSE NULL END) AS CEEB,
    MAX(CASE WHEN sa.name = 'has_hispanic_latino_or_spanish_origin' THEN sav.value ELSE NULL END) AS has_hispanic_latino_origin,
    MAX(CASE WHEN sa.name = 'current_year_class' THEN sav.value ELSE NULL END) AS current_year_class,
    MAX(CASE WHEN sa.name = 'high_school_country' THEN sav.value ELSE NULL END) AS high_school_country,
    MAX(CASE WHEN sa.name = 'country_of_citizenship_1' THEN sav.value ELSE NULL END) AS country_of_citizenship_1,
    MAX(CASE WHEN sa.name = 'country_of_citizenship_2' THEN sav.value ELSE NULL END) AS country_of_citizenship_2,
    MAX(CASE WHEN sa.name = 'country_of_citizenship_3' THEN sav.value ELSE NULL END) AS country_of_citizenship_3,
    MAX(CASE WHEN sa.name = 'gender' THEN sav.value ELSE NULL END) AS gender,
    MAX(CASE WHEN sa.name = 'guidance_counselor_first_name' THEN sav.value ELSE NULL END) AS guidance_counselor_first_name,
    MAX(CASE WHEN sa.name = 'guidance_counselor_last_name' THEN sav.value ELSE NULL END) AS guidance_counselor_last_name,
    MAX(CASE WHEN sa.name = 'guidance_counselor_email' THEN sav.value ELSE NULL END) AS guidance_counselor_email,
    MAX(CASE WHEN sa.name = 'country_of_interest_1' THEN sav.value ELSE NULL END) AS country_of_interest_1,
    MAX(CASE WHEN sa.name = 'country_of_interest_2' THEN sav.value ELSE NULL END) AS country_of_interest_2,
    MAX(CASE WHEN sa.name = 'country_of_interest_3' THEN sav.value ELSE NULL END) AS country_of_interest_3,
    MAX(CASE WHEN sa.name = 'authorize_cis' THEN sav.value ELSE NULL END) AS authorize_cis,
    MAX(CASE WHEN sa.name = 'toefl_score' THEN sav.value ELSE NULL END) AS toefl_score,
    MAX(CASE WHEN sa.name = 'ielts_score' THEN sav.value ELSE NULL END) AS ielts_score,
    MAX(CASE WHEN sa.name = 'ssat_score' THEN sav.value ELSE NULL END) AS ssat_score,
    MAX(CASE WHEN sa.name = 'professional_type' THEN sav.value ELSE NULL END) AS professional_type,
    MAX(CASE WHEN sa.name = 'preferred_name' THEN sav.value ELSE NULL END) AS preferred_name,
    MAX(CASE WHEN sa.name = 'pronouns' THEN sav.value ELSE NULL END) AS pronouns,
    MAX(CASE WHEN sa.name = 'job_title' THEN sav.value ELSE NULL END) AS job_title,
    MAX(CASE WHEN sa.name = 'work_phone' THEN sav.value ELSE NULL END) AS work_phone,
    MAX(CASE WHEN sa.name = 'work_phone_ext' THEN sav.value ELSE NULL END) AS work_phone_ext,
    MAX(CASE WHEN sa.name = 'work_phone_country_code' THEN sav.value ELSE NULL END) AS work_phone_country_code,
    MAX(CASE WHEN sa.name = 'organization' THEN sav.value ELSE NULL END) AS organization,
    MAX(CASE WHEN sa.name = 'additional_data_1' THEN sav.value ELSE NULL END) AS additional_data_1,
    MAX(CASE WHEN sa.name = 'additional_data_2' THEN sav.value ELSE NULL END) AS additional_data_2,
    MAX(CASE WHEN sa.name = 'additional_data_3' THEN sav.value ELSE NULL END) AS additional_data_3,
    MAX(CASE WHEN sa.name = 'additional_data_4' THEN sav.value ELSE NULL END) AS additional_data_4,
    MAX(CASE WHEN sa.name = 'additional_data_5' THEN sav.value ELSE NULL END) AS additional_data_5,
    MAX(CASE WHEN sa.name = 'additional_data_6' THEN sav.value ELSE NULL END) AS additional_data_6,
    MAX(CASE WHEN sa.name = 'additional_data_7' THEN sav.value ELSE NULL END) AS additional_data_7,
    MAX(CASE WHEN sa.name = 'additional_data_8' THEN sav.value ELSE NULL END) AS additional_data_8,
    MAX(CASE WHEN sa.name = 'additional_data_9' THEN sav.value ELSE NULL END) AS additional_data_9,
    MAX(CASE WHEN sa.name = 'additional_data_10' THEN sav.value ELSE NULL END) AS additional_data_10,
    MAX(CASE WHEN sa.name = 'parent_first_name' THEN sav.value ELSE NULL END) AS parent_first_name,
    MAX(CASE WHEN sa.name = 'parent_last_name' THEN sav.value ELSE NULL END) AS parent_last_name,
    MAX(CASE WHEN sa.name = 'parent_phone' THEN sav.value ELSE NULL END) AS parent_phone,
    MAX(CASE WHEN sa.name = 'parent_phone_country_code' THEN sav.value ELSE NULL END) AS parent_phone_country_code,
    MAX(CASE WHEN sa.name = 'parent_email' THEN sav.value ELSE NULL END) AS parent_email,
    MAX(CASE WHEN sa.name = 'parent_relationship' THEN sav.value ELSE NULL END) AS parent_relationship,
	MAX(CASE WHEN es.ethnicity_id = '1' THEN 'Y' ELSE 'N' END) AS ethnicity_cuban,
	MAX(CASE WHEN es.ethnicity_id = '4' THEN 'Y' ELSE 'N' END) AS ethnicity_mexican,
	MAX(CASE WHEN es.ethnicity_id = '3' THEN 'Y' ELSE 'N' END) AS ethnicity_puerto_rican,
	MAX(CASE WHEN es.ethnicity_id = '2' THEN 'Y' ELSE 'N' END) AS ethnicity_other_hispanic_latino_or_spanish,
	MAX(CASE WHEN es.ethnicity_id = '5' THEN 'Y' ELSE 'N' END) AS ethnicity_non_hispanic_latino_or_spanish,
	MAX(CASE WHEN rs.race_id = '4' THEN 'Y' ELSE 'N' END) AS race_american_indian_or_alaskan_native,
	MAX(CASE WHEN rs.race_id = '3' THEN 'Y' ELSE 'N' END) AS race_asian,
	MAX(CASE WHEN rs.race_id = '1' THEN 'Y' ELSE 'N' END) AS race_black_or_african_american,
	MAX(CASE WHEN rs.race_id = '5' THEN 'Y' ELSE 'N' END) AS race_native_hawaiian_or_other_pacific_islander,
	MAX(CASE WHEN rs.race_id = '2' THEN 'Y' ELSE 'N' END) AS race_white,
    CONCAT(u.first_name, ' ', u.last_name) AS scan_rep,
    ufs.notes,
    ufs.rating,
    ufs.follow_up
FROM user_fair_students ufs
JOIN students s ON ufs.student_id = s.id
JOIN fairs f ON ufs.fair_id = f.id
JOIN teams t ON ufs.current_team_id = t.id
LEFT JOIN addresses a ON s.address_id = a.id
LEFT JOIN phone_numbers pn ON s.phone_number_id = pn.id
LEFT JOIN student_attribute_values sav ON s.id = sav.student_id
LEFT JOIN student_attributes sa ON sav.student_attribute_id = sa.id
LEFT JOIN fair_team ft ON f.id = ft.fair_id
LEFT JOIN ethnicity_student es on es.student_id = s.id
LEFT JOIN race_student rs on rs.student_id = s.id
LEFT JOIN users u on u.id = ufs.user_id
WHERE 
    -- Find fairs that ended within the last ? days in Chicago time
    CONVERT_TZ(f.ends_at, f.ends_at_timezone, 'America/Chicago') >= DATE_SUB(CONVERT_TZ(NOW(), 'UTC', 'America/Chicago'), INTERVAL ? DAY)
    AND CONVERT_TZ(f.ends_at, f.ends_at_timezone, 'America/Chicago') <= CONVERT_TZ(NOW(), 'UTC', 'America/Chicago')
    AND ufs.sftp_update_id IS NULL
    AND s.student_type_id = ?`

const professionalScanQueryGroupBy = `
GROUP BY 
    t.id, t.name, f.id, f.name, f.starts_at, s.id, s.first_name, s.last_name, 
    s.email, s.phone, pn.number, a.line1, a.line2, a.municipality, a.region, a.postal_code, a.country_code,
    s.high_school, s.graduation_year, s.gpa, 
    s.area_of_interest_1, s.area_of_interest_2, s.area_of_interest_3, 
    ufs.notes, ufs.rating, ufs.follow_up, ft.guid_id, ufs.created_at, ufs.updated_at, ufs.parent_encountered,
    u.first_name, u.last_name
ORDER BY t.id;`

func (pp *ProfessionalScanProcessor) GetCSVHeader() []string {
	return []string{
		"Fair Name",
		"Internal Event ID",
		"First Name",
		"Last Name",
		"Email",
		"Phone",
		"Text Permission",
		"Address 1",
		"Address 2",
		"Address City",
		"Address State",
		"Address ZIP",
		"High School",
		"High School City",
		"High School State",
		"CEEB Code",
		"Organization",
		"Professional Type",
		"Preferred Name",
		"Pronouns",
		"Job Title",
		"Work Phone",
		"Rating",
		"Notes",
		"Follow Up",
		"Scan Time",
		"Scan Rep",
		"Additional Data 1",
		"Additional Data 2",
		"Additional Data 3",
		"Additional Data 4",
		"Additional Data 5",
		"Additional Data 6",
		"Additional Data 7",
		"Additional Data 8",
		"Additional Data 9",
		"Additional Data 10",
		"Registration Language",
		"Event Guide",
		"Updated Time",
	}
}

// FetchData retrieves professional scan data (type 6) from the database.
func (pp *ProfessionalScanProcessor) FetchData(db *sql.DB, config Config) (interface{}, error) {
	fmt.Println("Fetching professional scan data (type 6)...")

	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	var query strings.Builder
	args := []interface{}{config.Days, pp.scanTypeID} // Add scan type ID as the last argument

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

// TransformData groups professional scan data by TeamID and prepares it for CSV.
func (pp *ProfessionalScanProcessor) TransformData(data interface{}) (map[int64][][]string, error) {
	fmt.Println("Transforming and grouping professional scan data by TeamID...")
	scans, ok := data.([]models.StudentScanData)
	if !ok {
		return nil, fmt.Errorf("invalid data type for professional scan transformation, expected []models.StudentScanData")
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

func (pp *ProfessionalScanProcessor) TransformScanToRow(scan models.StudentScanData) []string {
	return []string{
		scan.FairName,
		pp.nullStr(scan.InternalEventID),
		pp.nullStr(scan.FirstName),
		pp.nullStr(scan.LastName),
		pp.nullStr(scan.Email),
		pp.nullStr(scan.PhoneNumber),
		func() string {
			if scan.TextPermission.Valid && scan.TextPermission.String == "1" {
				return "Yes"
			}
			return "No"
		}(),
		pp.nullStr(scan.AddressLine1),
		pp.nullStr(scan.AddressLine2),
		pp.nullStr(scan.AddressCity),
		pp.nullStr(scan.AddressState),
		pp.nullStr(scan.AddressZipcode),
		pp.nullStr(scan.Birthdate),
		pp.nullStr(scan.HighSchool),
		pp.nullStr(scan.HighSchoolCity),
		pp.nullStr(scan.HighSchoolRegion),
		pp.nullStr(scan.CEEB),
		pp.nullStr(scan.Organization),
		pp.nullStr(scan.ProfessionalType),
		pp.nullStr(scan.PreferredName),
		pp.nullStr(scan.Pronouns),
		pp.nullStr(scan.JobTitle),
		pp.nullStr(scan.WorkPhone),
		pp.nullInt(scan.Rating),
		pp.nullStr(scan.Notes),
		pp.nullBool(scan.FollowUp),
		pp.nullTime(scan.ScanTime, "2006-01-02 15:04:05"),
		pp.nullStr(scan.ScanRep),
		pp.nullStr(scan.AdditionalData1),
		pp.nullStr(scan.AdditionalData2),
		pp.nullStr(scan.AdditionalData3),
		pp.nullStr(scan.AdditionalData4),
		pp.nullStr(scan.AdditionalData5),
		pp.nullStr(scan.AdditionalData6),
		pp.nullStr(scan.AdditionalData7),
		pp.nullStr(scan.AdditionalData8),
		pp.nullStr(scan.AdditionalData9),
		pp.nullStr(scan.AdditionalData10),
		func() string {
			if scan.Locale.Valid {
				return scan.Locale.String
			}
			return "en"
		}(),
		pp.nullStr(scan.EventGuideFavourite),
		pp.nullTime(scan.UpdatedTime, "2006-01-02 15:04:05"),
	}
}

// WriteCSV saves the grouped professional scan data to team-specific CSV files.
func (pp *ProfessionalScanProcessor) WriteCSV(groupedData map[int64][][]string, config Config) ([]string, error) {
	fmt.Println("Writing professional scan data to team-specific CSV files...")
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
