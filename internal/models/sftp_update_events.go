package models

import "time"

// SFTPUpdateEvent represents an event associated with an SFTP update
type SFTPUpdateEvent struct {
	ID           uint      `gorm:"column:id;primaryKey;autoIncrement"`
	SFTPUpdateID uint      `gorm:"column:sftp_update_id;not null"`
	Type         string    `gorm:"column:type;type:enum('fair','visit');not null"`
	EventID      uint      `gorm:"column:event_id;not null"`
	CreatedAt    time.Time `gorm:"column:created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at"`
}
