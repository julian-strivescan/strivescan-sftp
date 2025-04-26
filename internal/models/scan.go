package models

import (
	"database/sql"
)

// StudentScanData holds the detailed data fetched for student scans (type 1).
type StudentScanData struct {
	TeamID                                   int64          // t.id
	TeamName                                 string         // t.name
	InternalEventID                          sql.NullString // ft.guid_id
	FairID                                   int64          // f.id
	FairName                                 string         // f.name
	FairDate                                 sql.NullTime   // f.starts_at
	StudentID                                int64          // s.id
	FirstName                                sql.NullString // s.first_name
	LastName                                 sql.NullString // s.last_name
	Email                                    sql.NullString // s.email
	Phone                                    sql.NullString // s.phone
	PhoneNumber                              sql.NullString // pn.number
	PhoneNumberFormatted                     sql.NullString // pn.formatted_number
	AddressLine1                             sql.NullString // a.line1
	AddressLine2                             sql.NullString // a.line2
	AddressCity                              sql.NullString // a.municipality
	AddressState                             sql.NullString // a.region
	AddressZipcode                           sql.NullString // a.postal_code
	AddressCountryCode                       sql.NullString // a.country_code
	HighSchool                               sql.NullString // COALESCE(s.high_school, ...)
	GraduationYear                           sql.NullString // COALESCE(s.graduation_year, ...)
	GPA                                      sql.NullString // COALESCE(s.gpa, ...)
	AreaOfInterest1                          sql.NullString // COALESCE(s.area_of_interest_1, ...)
	AreaOfInterest2                          sql.NullString // COALESCE(s.area_of_interest_2, ...)
	AreaOfInterest3                          sql.NullString // COALESCE(s.area_of_interest_3, ...)
	Birthdate                                sql.NullString // MAX(CASE WHEN sa.name = 'birthdate' ...)
	SatScore                                 sql.NullString // MAX(CASE WHEN sa.name = 'sat_score' ...)
	ActScore                                 sql.NullString // MAX(CASE WHEN sa.name = 'act_score' ...)
	TextPermission                           sql.NullString // MAX(CASE WHEN sa.name = 'text_permission' ...)
	HighSchoolCity                           sql.NullString // MAX(CASE WHEN sa.name = 'high_school_city' ...)
	HighSchoolRegion                         sql.NullString // MAX(CASE WHEN sa.name = 'high_school_region' ...)
	CollegeStartSemester                     sql.NullString // MAX(CASE WHEN sa.name = 'college_start_semester' ...)
	GPAMax                                   sql.NullString // MAX(CASE WHEN sa.name = 'gpa_max' ...)
	GradType                                 sql.NullString // MAX(CASE WHEN sa.name = 'grad_type' ...)
	CEEB                                     sql.NullString // MAX(CASE WHEN sa.name = 'CEEB' ...)
	HasHispanicLatinoOrigin                  sql.NullString // MAX(CASE WHEN sa.name = 'has_hispanic_latino_or_spanish_origin' ...)
	CurrentYearClass                         sql.NullString // MAX(CASE WHEN sa.name = 'current_year_class' ...)
	HighSchoolCountry                        sql.NullString // MAX(CASE WHEN sa.name = 'high_school_country' ...)
	CountryOfCitizenship1                    sql.NullString // MAX(CASE WHEN sa.name = 'country_of_citizenship_1' ...)
	CountryOfCitizenship2                    sql.NullString // MAX(CASE WHEN sa.name = 'country_of_citizenship_2' ...)
	CountryOfCitizenship3                    sql.NullString // MAX(CASE WHEN sa.name = 'country_of_citizenship_3' ...)
	Gender                                   sql.NullString // MAX(CASE WHEN sa.name = 'gender' ...)
	GuidanceCounselorFirstName               sql.NullString // MAX(CASE WHEN sa.name = 'guidance_counselor_first_name' ...)
	GuidanceCounselorLastName                sql.NullString // MAX(CASE WHEN sa.name = 'guidance_counselor_last_name' ...)
	GuidanceCounselorEmail                   sql.NullString // MAX(CASE WHEN sa.name = 'guidance_counselor_email' ...)
	CountryOfInterest1                       sql.NullString // MAX(CASE WHEN sa.name = 'country_of_interest_1' ...)
	CountryOfInterest2                       sql.NullString // MAX(CASE WHEN sa.name = 'country_of_interest_2' ...)
	CountryOfInterest3                       sql.NullString // MAX(CASE WHEN sa.name = 'country_of_interest_3' ...)
	AuthorizeCIS                             sql.NullString // MAX(CASE WHEN sa.name = 'authorize_cis' ...)
	TOEFLScore                               sql.NullString // MAX(CASE WHEN sa.name = 'toefl_score' ...)
	IELTSScore                               sql.NullString // MAX(CASE WHEN sa.name = 'ielts_score' ...)
	SSATScore                                sql.NullString // MAX(CASE WHEN sa.name = 'ssat_score' ...)
	ProfessionalType                         sql.NullString // MAX(CASE WHEN sa.name = 'professional_type' ...)
	PreferredName                            sql.NullString // MAX(CASE WHEN sa.name = 'preferred_name' ...)
	Pronouns                                 sql.NullString // MAX(CASE WHEN sa.name = 'pronouns' ...)
	JobTitle                                 sql.NullString // MAX(CASE WHEN sa.name = 'job_title' ...)
	WorkPhone                                sql.NullString // MAX(CASE WHEN sa.name = 'work_phone' ...)
	WorkPhoneExt                             sql.NullString // MAX(CASE WHEN sa.name = 'work_phone_ext' ...)
	WorkPhoneCountryCode                     sql.NullString // MAX(CASE WHEN sa.name = 'work_phone_country_code' ...)
	Organization                             sql.NullString // MAX(CASE WHEN sa.name = 'organization' ...)
	AdditionalData1                          sql.NullString // MAX(CASE WHEN sa.name = 'additional_data_1' ...)
	AdditionalData2                          sql.NullString // MAX(CASE WHEN sa.name = 'additional_data_2' ...)
	AdditionalData3                          sql.NullString // MAX(CASE WHEN sa.name = 'additional_data_3' ...)
	AdditionalData4                          sql.NullString // MAX(CASE WHEN sa.name = 'additional_data_4' ...)
	AdditionalData5                          sql.NullString // MAX(CASE WHEN sa.name = 'additional_data_5' ...)
	AdditionalData6                          sql.NullString // MAX(CASE WHEN sa.name = 'additional_data_6' ...)
	AdditionalData7                          sql.NullString // MAX(CASE WHEN sa.name = 'additional_data_7' ...)
	AdditionalData8                          sql.NullString // MAX(CASE WHEN sa.name = 'additional_data_8' ...)
	AdditionalData9                          sql.NullString // MAX(CASE WHEN sa.name = 'additional_data_9' ...)
	AdditionalData10                         sql.NullString // MAX(CASE WHEN sa.name = 'additional_data_10' ...)
	ParentFirstName                          sql.NullString // MAX(CASE WHEN sa.name = 'parent_first_name' ...)
	ParentLastName                           sql.NullString // MAX(CASE WHEN sa.name = 'parent_last_name' ...)
	ParentPhone                              sql.NullString // MAX(CASE WHEN sa.name = 'parent_phone' ...)
	ParentPhoneCountryCode                   sql.NullString // MAX(CASE WHEN sa.name = 'parent_phone_country_code' ...)
	ParentEmail                              sql.NullString // MAX(CASE WHEN sa.name = 'parent_email' ...)
	ParentRelationship                       sql.NullString // MAX(CASE WHEN sa.name = 'parent_relationship' ...)
	Notes                                    sql.NullString // ufs.notes
	Rating                                   sql.NullInt64  // ufs.rating
	FollowUp                                 sql.NullBool   // ufs.follow_up
	EthnicityCuban                           sql.NullString // MAX(CASE WHEN es.name = '1' THEN sav.value ELSE NULL END)
	EthnicityMexican                         sql.NullString // MAX(CASE WHEN es.name = '4' THEN sav.value ELSE NULL END)
	EthnicityPuertoRican                     sql.NullString // MAX(CASE WHEN es.name = '3' THEN sav.value ELSE NULL END)
	EthnicityOtherHispanicLatinoOrSpanish    sql.NullString // MAX(CASE WHEN es.name = '2' THEN sav.value ELSE NULL END)
	EthnicityNonHispanicLatinoOrSpanish      sql.NullString // MAX(CASE WHEN es.name = '5' THEN sav.value ELSE NULL END)
	RaceAmericanIndianOrAlaskanNative        sql.NullString // MAX(CASE WHEN rs.name = '4' THEN sav.value ELSE NULL END)
	RaceAsian                                sql.NullString // MAX(CASE WHEN rs.name = '3' THEN sav.value ELSE NULL END)
	RaceBlackOrAfricanAmerican               sql.NullString // MAX(CASE WHEN rs.name = '1' THEN sav.value ELSE NULL END)
	RaceNativeHawaiianOrOtherPacificIslander sql.NullString // MAX(CASE WHEN rs.name = '5' THEN sav.value ELSE NULL END)
	RaceWhite                                sql.NullString // MAX(CASE WHEN rs.name = '2' THEN sav.value ELSE NULL END)
	Locale                                   sql.NullString // s.locale
	ScanTime                                 sql.NullTime   // ufs.created_at
	ParentEncountered                        sql.NullBool   // ufs.parent_encountered
	UpdatedTime                              sql.NullTime   // ufs.updated_at
	ScanRep                                  sql.NullString // CONCAT(u.first_name, ' ', u.last_name)
	EventGuideFavourite                      sql.NullString // MAX(CASE WHEN EXISTS (
}

// Scan represents a basic scan record. (Removed as replaced by StudentScanData)
// type Scan struct {
// 	ID        int
// 	StudentID int // Assuming a link to a student
// 	Timestamp time.Time
// 	TeamID    int
// 	// Add other relevant scan fields here...
// }
