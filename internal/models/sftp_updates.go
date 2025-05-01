package models

import "time"

// SFTPUpdate represents a record of an SFTP upload attempt
type SFTPUpdate struct {
	ID               uint       `db:"id"`
	Status           string     `db:"status"`
	TeamID           *uint      `db:"team_id"`
	Error            *string    `db:"error"`
	ErrorDescription *string    `db:"error_description"`
	CreatedAt        *time.Time `db:"created_at"`
	UpdatedAt        *time.Time `db:"updated_at"`
	Type             string     `db:"type"`
}
