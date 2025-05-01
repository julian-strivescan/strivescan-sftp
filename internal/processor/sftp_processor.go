package processor

import (
	"crypto/aes"
	"crypto/cipher"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cristalhq/base64"
	"github.com/strivescan/strivescan-sftp/internal/models"
)

// SFTPProcessor handles uploading files to SFTP servers
type SFTPProcessor struct {
	BaseProcessor
	db *sql.DB
}

// NewSFTPProcessor creates a new SFTP processor
func NewSFTPProcessor(db *sql.DB) *SFTPProcessor {
	return &SFTPProcessor{db: db}
}

// Run processes files and uploads them to SFTP servers
func (sp *SFTPProcessor) Run(files []string, totalFiles int) error {
	if len(files) == 0 {
		fmt.Println("No files to process")
		return nil
	}

	fmt.Printf("Processing %d files for SFTP upload\n", totalFiles)

	for i, file := range files {
		// Calculate progress percentage
		progress := float64(i+1) / float64(totalFiles) * 100

		// Create progress bar string
		const barWidth = 50
		completed := int(float64(barWidth) * float64(i+1) / float64(totalFiles))
		bar := make([]byte, barWidth)
		for j := 0; j < barWidth; j++ {
			if j < completed {
				bar[j] = '='
			} else {
				bar[j] = ' '
			}
		}

		fmt.Printf("\r[%s] %.1f%% (%d/%d) Processing: %s", string(bar), progress, i+1, totalFiles, filepath.Base(file))

		// Extract team ID from the file path and add to array
		// Expected format: output/team_123/filename.csv
		teamIDs := make([]string, len(files))
		for i, file := range files {
			dirPath := filepath.Dir(file)
			dirName := filepath.Base(dirPath)

			var teamID string
			if _, err := fmt.Sscanf(dirName, "%s", &teamID); err != nil {
				return fmt.Errorf("failed to extract team ID from directory %s: %w", dirName, err)
			}
			teamIDs[i] = teamID
		}

		err := sp.prepareForUpload(teamIDs)
		if err != nil {
			return fmt.Errorf("failed to prepare for upload: %w", err)
		}

		// TODO: Implement SFTP upload logic
		time.Sleep(100 * time.Millisecond) // Simulate work
	}
	return nil
}

func (sp *SFTPProcessor) prepareForUpload(teamIDs []string) error {
	// Convert teamIDs slice to a comma-separated string for the IN clause
	teamIDsStr := strings.Join(teamIDs, ",")

	query := fmt.Sprintf(`
		SELECT *
		FROM sftp_credentials 
		WHERE team_id IN (%s)`, teamIDsStr)

	rows, err := sp.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query sftp credentials: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var sftpCred models.SFTPCredentials
		err := rows.Scan(
			&sftpCred.ID,
			&sftpCred.TeamID,
			&sftpCred.Host,
			&sftpCred.Port,
			&sftpCred.Username,
			&sftpCred.Password,
			&sftpCred.SSHKey,
			&sftpCred.SSHKeyFilename,
			&sftpCred.Passphrase,
			&sftpCred.UploadDirectory,
			&sftpCred.NotificationEmail,
			&sftpCred.CreatedAt,
			&sftpCred.UpdatedAt,
		)

		if err != nil {
			return fmt.Errorf("failed to scan sftp credentials: %w", err)
		}

		// Log some info for debugging
		fmt.Printf("Processing team ID: %d\n", sftpCred.TeamID)

		// If password is encrypted, decrypt it
		if sftpCred.Password.Valid {
			fmt.Printf("Attempting to decrypt password for team %d\n", sftpCred.TeamID)
			// For debugging, show a prefix of the encrypted password (first 20 chars max)
			pwdPrefix := sftpCred.Password.String
			if len(pwdPrefix) > 20 {
				pwdPrefix = pwdPrefix[:20] + "..."
			}
			fmt.Printf("Encrypted password prefix: %s\n", pwdPrefix)

			decryptedPassword, err := sp.decryptString(sftpCred.Password.String)
			if err != nil {
				return fmt.Errorf("failed to decrypt password for team %d: %w", sftpCred.TeamID, err)
			}
			sftpCred.Password = sql.NullString{
				String: decryptedPassword,
				Valid:  true,
			}
		}

		// If passphrase is encrypted, decrypt it
		if sftpCred.Passphrase.Valid {
			fmt.Printf("Attempting to decrypt passphrase for team %d\n", sftpCred.TeamID)
			decryptedPassphrase, err := sp.decryptString(sftpCred.Passphrase.String)
			if err != nil {
				return fmt.Errorf("failed to decrypt passphrase for team %d: %w", sftpCred.TeamID, err)
			}
			sftpCred.Passphrase = sql.NullString{
				String: decryptedPassphrase,
				Valid:  true,
			}
		}
	}

	fmt.Println("Wrapping up")

	return nil
}

// LaravelEncrypted represents Laravel's encryption payload structure
type LaravelEncrypted struct {
	IV    string `json:"iv"`
	Value string `json:"value"`
	Mac   string `json:"mac"`
	Tag   string `json:"tag,omitempty"` // For AEAD ciphers
}

// decryptString decrypts a string encrypted by Laravel's Crypt::encryptString
func (sp *SFTPProcessor) decryptString(encrypted string) (string, error) {
	// Get the Laravel encryption key from environment
	appKey := os.Getenv("LARAVEL_ENCRYPTION_KEY")
	if appKey == "" {
		return "", errors.New("LARAVEL_ENCRYPTION_KEY environment variable not set")
	}

	// First try to parse as JSON directly
	var jsonData map[string]interface{}
	err := json.Unmarshal([]byte(encrypted), &jsonData)
	if err != nil {
		// If not valid JSON, try to repair base64
		fmt.Println("Not valid JSON, attempting base64 repair")
		// Remove any whitespace and newlines
		encrypted = strings.TrimSpace(encrypted)
		// Add padding if needed
		if len(encrypted)%4 != 0 {
			encrypted += strings.Repeat("=", 4-len(encrypted)%4)
		}

		// Try base64 decode again
		decoded, err := base64.StdEncoding.DecodeString(encrypted)
		if err != nil {
			fmt.Printf("Base64 decode error after repair: %v\n", err)
			return "", fmt.Errorf("failed to decode base64: %w", err)
		}asdsfsdf

		// Try to parse the decoded data as JSON
		err = json.Unmarshal(decoded, &jsonData)
		if err != nil {
			fmt.Printf("JSON unmarshal error after base64 decode: %v\n", err)
			return "", fmt.Errorf("failed to unmarshal JSON: %w", err)
		}
	}

	// Verify we have the required fields
	if jsonData["iv"] == nil || jsonData["value"] == nil {
		return "", errors.New("missing required fields in JSON")
	}

	return sp.decryptFromJSON(jsonData, appKey)
}

func (sp *SFTPProcessor) decryptFromJSON(jsonData map[string]interface{}, appKey string) (string, error) {
	// Extract IV and ciphertext
	ivStr, ok := jsonData["iv"].(string)
	if !ok {
		return "", errors.New("invalid IV format in JSON")
	}

	valueStr, ok := jsonData["value"].(string)
	if !ok {
		return "", errors.New("invalid value format in JSON")
	}

	// Decode the IV and ciphertext
	iv, err := base64.StdEncoding.DecodeString(ivStr)
	if err != nil {
		return "", fmt.Errorf("failed to decode IV: %w", err)
	}

	ciphertext, err := base64.StdEncoding.DecodeString(valueStr)
	if err != nil {
		return "", fmt.Errorf("failed to decode ciphertext: %w", err)
	}

	// Remove the "base64:" prefix if present
	if len(appKey) > 7 && appKey[:7] == "base64:" {
		appKey = appKey[7:]
	}

	// Decode the base64 encoded key
	key, err := base64.StdEncoding.DecodeString(appKey)
	if err != nil {
		return "", fmt.Errorf("error decoding key: %w", err)
	}

	// Create the AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create AES cipher: %w", err)
	}

	// Create the decrypter
	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := make([]byte, len(ciphertext))
	mode.CryptBlocks(plaintext, ciphertext)

	// Remove PKCS#7 padding
	plaintext, err = removePadding(plaintext)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// isBase64 checks if a string is valid base64
func isBase64(s string) bool {
	// Try standard base64 first
	_, err := base64.StdEncoding.DecodeString(s)
	return err == nil
}

func removeTag(s string) string {
	fmt.Printf("removeTag input: %s\n", s)
	// Remove the tag field if it exists
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		fmt.Printf("Base64 decode error: %v\n", err)
		return s
	}
	fmt.Printf("Decoded length: %d\n", len(decoded))

	var jsonData map[string]interface{}
	if err := json.Unmarshal(decoded, &jsonData); err != nil {
		fmt.Printf("JSON unmarshal error: %v\n", err)
		return s
	}

	// Remove tag if it exists
	delete(jsonData, "tag")

	// Re-encode the JSON without the tag
	prettyJSON, err := json.Marshal(jsonData)
	if err != nil {
		fmt.Printf("JSON marshal error: %v\n", err)
		return s
	}

	result := base64.StdEncoding.EncodeToString(prettyJSON)
	fmt.Printf("Final encoded result: %s\n", result)
	return result
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// removePadding removes PKCS#7 padding
func removePadding(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("empty data")
	}

	padding := int(data[len(data)-1])
	if padding > len(data) {
		return nil, errors.New("invalid padding")
	}

	for i := len(data) - padding; i < len(data); i++ {
		if int(data[i]) != padding {
			return nil, errors.New("invalid padding")
		}
	}

	return data[:len(data)-padding], nil
}
