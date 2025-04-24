package processor

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/strivescan/strivescan-sftp/internal/models"
)

// BaseProcessor contains shared functionality between different scan processors
type BaseProcessor struct {
	scanTypeID int
	debug      bool
}

// NewBaseProcessor creates a new base processor with the specified scan type ID
func NewBaseProcessor(scanTypeID int) *BaseProcessor {
	return &BaseProcessor{
		scanTypeID: scanTypeID,
		debug:      false, // Debug mode off by default
	}
}

// SetDebug enables or disables debug mode for query logging.
func (bp *BaseProcessor) SetDebug(enabled bool) {
	bp.debug = enabled
}

// LogDebug prints a message only if debug mode is enabled.
func (bp *BaseProcessor) LogDebug(format string, args ...interface{}) {
	if bp.debug {
		fmt.Printf(format, args...)
	}
}

// GetScanQuery returns the base SQL query for fetching scan data
func (bp *BaseProcessor) GetScanQuery() string {
	return `
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
	MAX(CASE WHEN es.ethnicity_id = '1' THEN 'Y' ELSE '' END) AS ethnicity_cuban,
	MAX(CASE WHEN es.ethnicity_id = '4' THEN 'Y' ELSE '' END) AS ethnicity_mexican,
	MAX(CASE WHEN es.ethnicity_id = '3' THEN 'Y' ELSE '' END) AS ethnicity_puerto_rican,
	MAX(CASE WHEN es.ethnicity_id = '2' THEN 'Y' ELSE '' END) AS ethnicity_other_hispanic_latino_or_spanish,
	MAX(CASE WHEN es.ethnicity_id = '5' THEN 'Y' ELSE '' END) AS ethnicity_non_hispanic_latino_or_spanish,
	MAX(CASE WHEN rs.race_id = '4' THEN 'Y' ELSE '' END) AS race_american_indian_or_alaskan_native,
	MAX(CASE WHEN rs.race_id = '3' THEN 'Y' ELSE '' END) AS race_asian,
	MAX(CASE WHEN rs.race_id = '1' THEN 'Y' ELSE '' END) AS race_black_or_african_american,
	MAX(CASE WHEN rs.race_id = '5' THEN 'Y' ELSE '' END) AS race_native_hawaiian_or_other_pacific_islander,
	MAX(CASE WHEN rs.race_id = '2' THEN 'Y' ELSE '' END) AS race_white,
    CONCAT(u.first_name, ' ', u.last_name) AS scan_rep,
    ufs.notes,
    ufs.rating,
    ufs.follow_up,
	(CASE WHEN EXISTS (
        SELECT 1
        FROM fair_participant_student
        JOIN fair_participants ON fair_participants.id = fair_participant_student.fair_participant_id 
          AND fair_participants.deleted_at IS NULL
        WHERE fair_participant_student.student_id = s.id
          AND fair_participants.fair_id = f.id
          AND fair_participants.team_id = t.id
          AND fair_participant_student.is_favorite = 1
    ) THEN 'Event Guide Favorite' ELSE '' END) as event_guide_favourite
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
}

// GetScanQueryGroupBy returns the GROUP BY clause for the scan query
func (bp *BaseProcessor) GetScanQueryGroupBy() string {
	return `
GROUP BY 
    t.id, t.name, f.id, f.name, f.starts_at, s.id, s.first_name, s.last_name, 
    s.email, s.phone, pn.number, a.line1, a.line2, a.municipality, a.region, a.postal_code, a.country_code,
    s.high_school, s.graduation_year, s.gpa, 
    s.area_of_interest_1, s.area_of_interest_2, s.area_of_interest_3, 
    ufs.notes, ufs.rating, ufs.follow_up, ft.guid_id, ufs.created_at, ufs.updated_at, ufs.parent_encountered,
    u.first_name, u.last_name
ORDER BY t.id;`
}

// GetCSVHeader returns the standard CSV header for scan data
func (bp *BaseProcessor) GetCSVHeader() []string {
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
		"Birthdate",
		"High School",
		"High School City",
		"High School State",
		"CEEB Code",
		"Graduation Year",
		"College Start",
		"GPA",
		"GPA Max",
		"SAT",
		"ACT",
		"Area of Interest 1",
		"Area of Interest 2",
		"Area of Interest 3",
		"Ethnicity Cuban",
		"Ethnicity Mexican",
		"Ethnicity Puerto Rican",
		"Ethnicity Other Hispanic, Latino, or Spanish",
		"Ethnicity Non-Hispanic, Latino, or Spanish",
		"Race American Indian or Alaskan Native",
		"Race Asian",
		"Race Black or African American",
		"Race Native Hawaiian or Other Pacific Islander",
		"Race White",
		"Rating",
		"Notes",
		"Follow Up",
		"Parent or Student",
		"Scan Time",
		"Scan Rep",
		"Registration Language",
		"Event Guide",
		"Updated Time",
	}
}

// Helper functions for handling nullable types
func (bp *BaseProcessor) nullStr(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func (bp *BaseProcessor) nullInt(ni sql.NullInt64) string {
	if ni.Valid {
		return strconv.FormatInt(ni.Int64, 10)
	}
	return ""
}

func (bp *BaseProcessor) nullBool(nb sql.NullBool) string {
	if nb.Valid {
		return strconv.FormatBool(nb.Bool)
	}
	return ""
}

func (bp *BaseProcessor) nullTime(nt sql.NullTime, format string) string {
	if nt.Valid {
		return nt.Time.Format(format)
	}
	return ""
}

// TransformScanToRow converts a scan data record to a CSV row
func (bp *BaseProcessor) TransformScanToRow(scan models.StudentScanData) []string {
	return []string{
		scan.FairName,
		bp.nullStr(scan.InternalEventID),
		bp.nullStr(scan.FirstName),
		bp.nullStr(scan.LastName),
		bp.nullStr(scan.Email),
		bp.nullStr(scan.PhoneNumber),
		func() string {
			if scan.TextPermission.Valid && scan.TextPermission.String == "1" {
				return "Yes"
			}
			return "No"
		}(),
		bp.nullStr(scan.AddressLine1),
		bp.nullStr(scan.AddressLine2),
		bp.nullStr(scan.AddressCity),
		bp.nullStr(scan.AddressState),
		bp.nullStr(scan.AddressZipcode),
		bp.nullStr(scan.Birthdate),
		bp.nullStr(scan.HighSchool),
		bp.nullStr(scan.HighSchoolCity),
		bp.nullStr(scan.HighSchoolRegion),
		bp.nullStr(scan.CEEB),
		bp.nullStr(scan.GraduationYear),
		bp.nullStr(scan.CollegeStartSemester),
		bp.nullStr(scan.GPA),
		bp.nullStr(scan.GPAMax),
		bp.nullStr(scan.SatScore),
		bp.nullStr(scan.ActScore),
		bp.nullStr(scan.AreaOfInterest1),
		bp.nullStr(scan.AreaOfInterest2),
		bp.nullStr(scan.AreaOfInterest3),
		bp.nullStr(scan.EthnicityCuban),
		bp.nullStr(scan.EthnicityMexican),
		bp.nullStr(scan.EthnicityPuertoRican),
		bp.nullStr(scan.EthnicityOtherHispanicLatinoOrSpanish),
		bp.nullStr(scan.EthnicityNonHispanicLatinoOrSpanish),
		bp.nullStr(scan.RaceAmericanIndianOrAlaskanNative),
		bp.nullStr(scan.RaceAsian),
		bp.nullStr(scan.RaceBlackOrAfricanAmerican),
		bp.nullStr(scan.RaceNativeHawaiianOrOtherPacificIslander),
		bp.nullStr(scan.RaceWhite),
		bp.nullInt(scan.Rating),
		bp.nullStr(scan.Notes),
		bp.nullBool(scan.FollowUp),
		func() string {
			if scan.ParentEncountered.Valid && scan.ParentEncountered.Bool {
				return "Parent"
			}
			return "Student"
		}(),
		bp.nullTime(scan.ScanTime, "2006-01-02 15:04:05"),
		bp.nullStr(scan.ScanRep),
		func() string {
			if scan.Locale.Valid {
				return scan.Locale.String
			}
			return "en"
		}(),
		bp.nullStr(scan.EventGuideFavourite),
		bp.nullTime(scan.UpdatedTime, "2006-01-02 15:04:05"),
	}
}

// WriteCSVFile writes the CSV data to a file
func (bp *BaseProcessor) WriteCSVFile(teamID int64, teamData [][]string, baseOutputDir string, timestamp string) (string, error) {
	if len(teamData) <= 1 { // Skip teams with only a header row
		return "", fmt.Errorf("no data rows for team %d", teamID)
	}

	// Create team-specific directory path
	teamDir := filepath.Join(baseOutputDir, strconv.FormatInt(teamID, 10))
	filename := fmt.Sprintf("scans_%s_%s.csv", bp.getScanTypeName(), timestamp)
	fp := filepath.Join(teamDir, filename)

	// Create the team-specific output directory if it doesn't exist
	if err := os.MkdirAll(teamDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory '%s' for team %d: %w", teamDir, teamID, err)
	}

	// Create and open the file
	file, err := os.Create(fp)
	if err != nil {
		return "", fmt.Errorf("failed to create CSV file '%s' for team %d: %w", fp, teamID, err)
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write all data for this team
	if err := writer.WriteAll(teamData); err != nil {
		return "", fmt.Errorf("failed to write CSV data for team %d to '%s': %w", teamID, fp, err)
	}

	return fp, nil
}

// getScanTypeName returns a string representation of the scan type
func (bp *BaseProcessor) getScanTypeName() string {
	switch bp.scanTypeID {
	case 1:
		return "students"
	case 2:
		return "cis"
	case 6:
		return "professionals"
	default:
		return "unknown"
	}
}
