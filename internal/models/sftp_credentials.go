package models

import (
	"database/sql"
)

type SFTPCredentials struct {
	ID                int64          `db:"id"`
	TeamID            int64          `db:"team_id"`
	Host              string         `db:"host"`
	Port              string         `db:"port"`
	Username          string         `db:"username"`
	Password          sql.NullString `db:"password"`
	SSHKey            sql.NullString `db:"ssh_key"`
	SSHKeyFilename    sql.NullString `db:"ssh_key_filename"`
	Passphrase        sql.NullString `db:"passphrase"`
	UploadDirectory   sql.NullString `db:"upload_directory"`
	NotificationEmail sql.NullString `db:"notification_email"`
	CreatedAt         sql.NullTime   `db:"created_at"`
	UpdatedAt         sql.NullTime   `db:"updated_at"`
}
