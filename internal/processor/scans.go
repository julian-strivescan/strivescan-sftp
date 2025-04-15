package processor

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/strivescan/strivescan-sftp/internal/models"
)

// StudentScanProcessor handles processing of student scan data (type 1).
type StudentScanProcessor struct{}

const studentScanQueryBase = `
SELECT 
    t.id AS team_id,
    t.name AS team_name,
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
WHERE 
    -- Find fairs that ended within the last ? days in Chicago time
    CONVERT_TZ(f.ends_at, f.ends_at_timezone, 'America/Chicago') >= DATE_SUB(CONVERT_TZ(NOW(), 'UTC', 'America/Chicago'), INTERVAL ? DAY)
    AND CONVERT_TZ(f.ends_at, f.ends_at_timezone, 'America/Chicago') <= CONVERT_TZ(NOW(), 'UTC', 'America/Chicago')
    AND ufs.sftp_update_id IS NULL
    AND s.student_type_id = 1`

const studentScanQueryGroupBy = `
GROUP BY 
    t.id, t.name, f.id, f.name, f.starts_at, s.id, s.first_name, s.last_name, 
    s.email, s.phone, pn.number, a.line1, a.line2, a.municipality, a.region, a.postal_code, a.country_code,
    s.high_school, s.graduation_year, s.gpa, 
    s.area_of_interest_1, s.area_of_interest_2, s.area_of_interest_3, 
    ufs.notes, ufs.rating, ufs.follow_up
ORDER BY t.id;`

// FetchData retrieves student scan data (type 1) from the database.
func (sp *StudentScanProcessor) FetchData(db *sql.DB, config Config) (interface{}, error) {
	fmt.Println("Fetching student scan data (type 1)...")

	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	var query strings.Builder
	args := []interface{}{config.Days} // Start with the days argument for INTERVAL

	query.WriteString(studentScanQueryBase)

	if config.TeamID != 0 {
		query.WriteString(" AND t.id = ?")
		args = append(args, config.TeamID)
	}

	query.WriteString(studentScanQueryGroupBy)

	finalQuery := query.String()
	fmt.Printf("Executing Query:\n%s\nArgs: %v\n", finalQuery, args)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Example timeout
	defer cancel()

	rows, err := db.QueryContext(ctx, finalQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("database query failed: %w", err)
	}
	defer rows.Close() // Ensure rows are closed

	results := []models.StudentScanData{}
	for rows.Next() {
		var scanData models.StudentScanData
		// Ensure the order matches the SELECT statement *exactly*
		err := rows.Scan(
			&scanData.TeamID,
			&scanData.TeamName,
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
			&scanData.Notes,
			&scanData.Rating,
			&scanData.FollowUp,
		)
		if err != nil {
			// Consider logging this error instead of returning immediately
			// depending on whether partial results are acceptable.
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		results = append(results, scanData)
	}

	// Check for errors during row iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	fmt.Printf("--- Fetched %d records from database ---\n", len(results))
	return results, nil
}

// TransformData converts student scan data to CSV format.
func (sp *StudentScanProcessor) TransformData(data interface{}) ([][]string, error) {
	fmt.Println("Transforming student scan data...")
	scans, ok := data.([]models.StudentScanData)
	if !ok {
		return nil, fmt.Errorf("invalid data type for student scan transformation, expected []models.StudentScanData")
	}

	// Define CSV header based on the updated query fields
	header := []string{
		"team_id", "team_name", "fair_id", "fair_name", "fair_date",
		"student_id", "first_name", "last_name", "email", "phone", "phone_number",
		"address_line_1", "address_line_2", "address_city", "address_state", "address_zipcode", "address_country_code",
		"high_school", "graduation_year", "gpa", "area_of_interest_1", "area_of_interest_2", "area_of_interest_3",
		"birthdate", "sat_score", "act_score", "text_permission", "high_school_city", "high_school_region",
		"college_start_semester", "gpa_max", "grad_type", "CEEB", "has_hispanic_latino_origin",
		"current_year_class", "high_school_country", "country_of_citizenship_1", "country_of_citizenship_2", "country_of_citizenship_3",
		"gender", "guidance_counselor_first_name", "guidance_counselor_last_name", "guidance_counselor_email",
		"country_of_interest_1", "country_of_interest_2", "country_of_interest_3", "authorize_cis",
		"toefl_score", "ielts_score", "ssat_score", "professional_type", "preferred_name", "pronouns",
		"job_title", "work_phone", "work_phone_ext", "work_phone_country_code", "organization",
		"additional_data_1", "additional_data_2", "additional_data_3", "additional_data_4", "additional_data_5",
		"additional_data_6", "additional_data_7", "additional_data_8", "additional_data_9", "additional_data_10",
		"parent_first_name", "parent_last_name", "parent_phone", "parent_phone_country_code", "parent_email", "parent_relationship",
		"notes", "rating", "follow_up",
	}
	csvData := [][]string{header}

	// Helper function to safely get string from nullable types
	nullStr := func(ns sql.NullString) string {
		if ns.Valid {
			return ns.String
		}
		return ""
	}
	nullInt := func(ni sql.NullInt64) string {
		if ni.Valid {
			return strconv.FormatInt(ni.Int64, 10)
		}
		return ""
	}
	nullBool := func(nb sql.NullBool) string {
		if nb.Valid {
			return strconv.FormatBool(nb.Bool)
		}
		return ""
	}
	nullTime := func(nt sql.NullTime, format string) string {
		if nt.Valid {
			return nt.Time.Format(format)
		}
		return ""
	}

	// Convert scans to string slices, including new fields
	for _, scan := range scans {
		row := []string{
			strconv.FormatInt(scan.TeamID, 10),
			scan.TeamName,
			strconv.FormatInt(scan.FairID, 10),
			scan.FairName,
			nullTime(scan.FairDate, "2006-01-02"),
			strconv.FormatInt(scan.StudentID, 10),
			nullStr(scan.FirstName),
			nullStr(scan.LastName),
			nullStr(scan.Email),
			nullStr(scan.Phone),
			nullStr(scan.PhoneNumber),
			nullStr(scan.AddressLine1),
			nullStr(scan.AddressLine2),
			nullStr(scan.AddressCity),
			nullStr(scan.AddressState),
			nullStr(scan.AddressZipcode),
			nullStr(scan.AddressCountryCode),
			nullStr(scan.HighSchool),
			nullStr(scan.GraduationYear),
			nullStr(scan.GPA),
			nullStr(scan.AreaOfInterest1),
			nullStr(scan.AreaOfInterest2),
			nullStr(scan.AreaOfInterest3),
			nullStr(scan.Birthdate),
			nullStr(scan.SatScore),
			nullStr(scan.ActScore),
			nullStr(scan.TextPermission),
			nullStr(scan.HighSchoolCity),
			nullStr(scan.HighSchoolRegion),
			nullStr(scan.CollegeStartSemester),
			nullStr(scan.GPAMax),
			nullStr(scan.GradType),
			nullStr(scan.CEEB),
			nullStr(scan.HasHispanicLatinoOrigin),
			nullStr(scan.CurrentYearClass),
			nullStr(scan.HighSchoolCountry),
			nullStr(scan.CountryOfCitizenship1),
			nullStr(scan.CountryOfCitizenship2),
			nullStr(scan.CountryOfCitizenship3),
			nullStr(scan.Gender),
			nullStr(scan.GuidanceCounselorFirstName),
			nullStr(scan.GuidanceCounselorLastName),
			nullStr(scan.GuidanceCounselorEmail),
			nullStr(scan.CountryOfInterest1),
			nullStr(scan.CountryOfInterest2),
			nullStr(scan.CountryOfInterest3),
			nullStr(scan.AuthorizeCIS),
			nullStr(scan.TOEFLScore),
			nullStr(scan.IELTSScore),
			nullStr(scan.SSATScore),
			nullStr(scan.ProfessionalType),
			nullStr(scan.PreferredName),
			nullStr(scan.Pronouns),
			nullStr(scan.JobTitle),
			nullStr(scan.WorkPhone),
			nullStr(scan.WorkPhoneExt),
			nullStr(scan.WorkPhoneCountryCode),
			nullStr(scan.Organization),
			nullStr(scan.AdditionalData1),
			nullStr(scan.AdditionalData2),
			nullStr(scan.AdditionalData3),
			nullStr(scan.AdditionalData4),
			nullStr(scan.AdditionalData5),
			nullStr(scan.AdditionalData6),
			nullStr(scan.AdditionalData7),
			nullStr(scan.AdditionalData8),
			nullStr(scan.AdditionalData9),
			nullStr(scan.AdditionalData10),
			nullStr(scan.ParentFirstName),
			nullStr(scan.ParentLastName),
			nullStr(scan.ParentPhone),
			nullStr(scan.ParentPhoneCountryCode),
			nullStr(scan.ParentEmail),
			nullStr(scan.ParentRelationship),
			nullStr(scan.Notes),
			nullInt(scan.Rating),
			nullBool(scan.FollowUp),
		}
		csvData = append(csvData, row)
	}

	return csvData, nil
}

// WriteCSV saves the student scan data to a CSV file in the output directory.
func (sp *StudentScanProcessor) WriteCSV(data [][]string, config Config) (string, error) {
	fmt.Println("Writing student scan data to CSV...")
	if len(data) <= 1 { // Only header present (or just header)
		fmt.Println("No student scan data to write.")
		return "", nil // Indicate no file was written, not necessarily an error
	}

	// Create a unique filename specific to student scans
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("scans_students_%s.csv", timestamp)
	if config.TeamID != 0 {
		filename = fmt.Sprintf("scans_students_team%d_%s.csv", config.TeamID, timestamp)
	}
	fp := filepath.Join("output", filename)

	// Create the output directory if it doesn't exist
	outDir := filepath.Dir(fp)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory '%s': %w", outDir, err)
	}

	// Create and open the file
	file, err := os.Create(fp)
	if err != nil {
		return "", fmt.Errorf("failed to create CSV file '%s': %w", fp, err)
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)

	// Write all data (header and rows) to the CSV file
	err = writer.WriteAll(data)
	if err != nil {
		return "", fmt.Errorf("failed to write data to CSV file '%s': %w", fp, err)
	}

	// Flush ensures all buffered data is written to the file
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", fmt.Errorf("error flushing CSV writer for '%s': %w", fp, err)
	}

	fmt.Printf("Successfully wrote %d data rows to: %s\n", len(data)-1, fp)
	return fp, nil // Return the path to the created file
}
