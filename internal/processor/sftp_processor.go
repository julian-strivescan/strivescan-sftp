package processor

import (
	"crypto/aes"
	"crypto/cipher"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cristalhq/base64"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/fatih/color"
	"github.com/strivescan/strivescan-sftp/internal/models"
)

// Global variable to store error messages
var ProcessingErrors []string

// SFTPProcessor handles uploading files to SFTP servers
type SFTPProcessor struct {
	BaseProcessor
	db     *sql.DB
	teamID int
}

func NewSFTPProcessor(db *sql.DB, teamID int) *SFTPProcessor {
	return &SFTPProcessor{
		db:     db,
		teamID: teamID,
	}
}

func (s *SFTPProcessor) Process() error {
	color.Magenta("Warming up SFTP processor...")
	// Get SFTP credentials from database
	var query string
	var rows *sql.Rows
	var err error
	if s.teamID != 0 {
		query = "SELECT * FROM sftp_credentials WHERE team_id = ?"
		rows, err = s.db.Query(query, s.teamID)
	} else {
		query = "SELECT * FROM sftp_credentials"
		rows, err = s.db.Query(query)
	}
	if err != nil {
		ProcessingErrors = append(ProcessingErrors, "Failed to query SFTP credentials: "+err.Error())
		color.Red("Failed to query SFTP credentials: %v", err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var creds models.SFTPCredentials
		err = rows.Scan(
			&creds.ID,
			&creds.TeamID,
			&creds.Host,
			&creds.Port,
			&creds.Username,
			&creds.Password,
			&creds.SSHKey,
			&creds.SSHKeyFilename,
			&creds.Passphrase,
			&creds.UploadDirectory,
			&creds.NotificationEmail,
			&creds.CreatedAt,
			&creds.UpdatedAt,
		)
		if err != nil {
			ProcessingErrors = append(ProcessingErrors, "Failed to scan SFTP credentials: "+err.Error())
			color.Red("Failed to scan SFTP credentials: %v", err)
			return err
		}

		fmt.Printf("Processing SFTP credentials for team %d on host %s\n", creds.TeamID, creds.Host)
		err = s.processCredentials(creds)
		if err != nil {
			ProcessingErrors = append(ProcessingErrors, "Failed to process credentials: "+err.Error())
			color.Red("Failed to process credentials: %v", err)
			return err
		}
	}

	return nil
}

func (s *SFTPProcessor) processCredentials(creds models.SFTPCredentials) error {
	// We need to make sure to decode the password, passphrase, and ssh_key
	// If the password is encoded, we need to decode it
	// If the passphrase is encoded, we need to decode it
	// If the ssh_key is encoded, we need to decode it

	// Decode the password
	if creds.Password.Valid {
		fmt.Printf("Attempting to decrypt password for team %d\n", creds.TeamID)
		// For debugging, show a prefix of the encrypted password (first 20 chars max)
		pwdPrefix := creds.Password.String
		if len(pwdPrefix) > 20 {
			pwdPrefix = pwdPrefix[:20] + "..."
		}
		fmt.Printf("Encrypted password prefix: %s\n", pwdPrefix)

		decryptedPassword, err := s.decryptString(creds.Password.String)
		if err != nil {
			color.Red("Failed to decrypt password for team %d: %v", creds.TeamID, err)
			ProcessingErrors = append(ProcessingErrors, "Failed to decrypt password for team "+strconv.FormatInt(creds.TeamID, 10)+": "+err.Error())
			return fmt.Errorf("failed to decrypt password for team %d: %w", creds.TeamID, err)
		}

		creds.Password = sql.NullString{
			String: decryptedPassword,
			Valid:  true,
		}
	}

	if creds.SSHKey.Valid {
		fmt.Printf("Attempting to decrypt SSH Key for team %d\n", creds.TeamID)
		// For debugging, show a prefix of the encrypted password (first 20 chars max)
		pwdPrefix := creds.SSHKey.String
		if len(pwdPrefix) > 20 {
			pwdPrefix = pwdPrefix[:20] + "..."
		}
		fmt.Printf("Encrypted password prefix: %s\n", pwdPrefix)

		decryptedSSHKey, err := s.decryptString(creds.SSHKey.String)
		if err != nil {
			color.Red("Failed to decrypt SSH Key for team %d: %v", creds.TeamID, err)
			ProcessingErrors = append(ProcessingErrors, "Failed to decrypt SSH Key for team "+strconv.FormatInt(creds.TeamID, 10)+": "+err.Error())
			return fmt.Errorf("failed to decrypt password for team %d: %w", creds.TeamID, err)
		}

		creds.SSHKey = sql.NullString{
			String: decryptedSSHKey,
			Valid:  true,
		}
	}

	if creds.Passphrase.Valid {
		fmt.Printf("Attempting to decrypt Passphrase for team %d\n", creds.TeamID)
		// For debugging, show a prefix of the encrypted password (first 20 chars max)
		pwdPrefix := creds.Passphrase.String
		if len(pwdPrefix) > 20 {
			pwdPrefix = pwdPrefix[:20] + "..."
		}
		fmt.Printf("Encrypted password prefix: %s\n", pwdPrefix)

		decryptedPassphrase, err := s.decryptString(creds.Passphrase.String)
		if err != nil {
			color.Red("Failed to decrypt passphrase for team %d: %v", creds.TeamID, err)
			ProcessingErrors = append(ProcessingErrors, "Failed to decrypt passphrase for team "+strconv.FormatInt(creds.TeamID, 10)+": "+err.Error())
			return fmt.Errorf("failed to decrypt password for team %d: %w", creds.TeamID, err)
		}

		creds.Passphrase = sql.NullString{
			String: decryptedPassphrase,
			Valid:  true,
		}
	}

	client, err := s.ConnectToSFTP(creds)
	if err != nil {
		color.Red("Failed to connect to SFTP for team %d: %v", creds.TeamID, err)
		ProcessingErrors = append(ProcessingErrors, "Failed to connect to SFTP for team "+strconv.FormatInt(creds.TeamID, 10)+": "+err.Error())
		return fmt.Errorf("failed to connect to SFTP for team %d: %w", creds.TeamID, err)
	}

	err = s.uploadFiles(client, creds)
	if err != nil {
		color.Red("Failed to upload files for team %d: %v", creds.TeamID, err)
		ProcessingErrors = append(ProcessingErrors, "Failed to upload files for team "+strconv.FormatInt(creds.TeamID, 10)+": "+err.Error())
		return fmt.Errorf("failed to upload files for team %d: %w", creds.TeamID, err)
	}

	return nil
}

func (s *SFTPProcessor) uploadFiles(client *sftp.Client, creds models.SFTPCredentials) error {
	defer client.Close()
	// Read the output directory for the team
	files, err := os.ReadDir("output/" + strconv.FormatInt(creds.TeamID, 10))
	if err != nil {
		return fmt.Errorf("failed to read output directory for team %d: %w", creds.TeamID, err)
	}

	// Upload each file to SFTP server
	for _, file := range files {
		if file.IsDir() {
			continue // Skip directories
		}

		// Open local file
		localPath := filepath.Join("output", strconv.FormatInt(creds.TeamID, 10), file.Name())
		localFile, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("failed to open local file %s: %w", localPath, err)
		}

		creds.UploadDirectory = sql.NullString{
			String: "upload",
			Valid:  true,
		}

		// Create remote file
		remotePath := creds.UploadDirectory.String + "/" + file.Name() // Using root path since RemotePath is not defined in credentials
		remoteFile, err := client.Create(remotePath)
		if err != nil {
			return fmt.Errorf("failed to create remote file %s: %w", remotePath, err)
		}
		defer remoteFile.Close()

		// Copy file contents
		written, err := io.Copy(remoteFile, localFile)
		if err != nil {
			return fmt.Errorf("failed to copy file %s to remote: %w", localPath, err)
		}

		// Verify upload by checking file size
		remoteFileInfo, err := client.Stat(remotePath)
		if err != nil {
			return fmt.Errorf("failed to verify remote file %s: %w", remotePath, err)
		}

		localFileInfo, err := localFile.Stat()
		if err != nil {
			return fmt.Errorf("failed to get local file info %s: %w", localPath, err)
		}

		if remoteFileInfo.Size() != localFileInfo.Size() {
			return fmt.Errorf("file size mismatch for %s: local %d bytes, remote %d bytes",
				file.Name(), localFileInfo.Size(), remoteFileInfo.Size())
		}

		color.Green("Successfully uploaded and verified %s to %s (%d bytes)",
			localPath, remotePath, written)
	}

	return nil
}

func (s *SFTPProcessor) ConnectToSFTP(creds models.SFTPCredentials) (*sftp.Client, error) {
	host := creds.Host
	port := creds.Port
	user := "foo"
	password := "pass"
	sshKey := ""
	passphrase := ""

	config := &ssh.ClientConfig{
		User:            user,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// Check if SSH key is provided
	if sshKey != "" {
		var signer ssh.Signer
		var err error

		if passphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase([]byte(sshKey), []byte(passphrase))
		} else {
			signer, err = ssh.ParsePrivateKey([]byte(sshKey))
		}

		if err != nil {
			color.Red("Failed to parse SSH key for team %d: %v", creds.TeamID, err)
			ProcessingErrors = append(ProcessingErrors, "Failed to parse SSH key for team "+strconv.FormatInt(creds.TeamID, 10)+": "+err.Error())
			return nil, fmt.Errorf("failed to parse SSH key: %w", err)
		}

		config.Auth = []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		}
	} else if password != "" {
		// Fall back to password auth if no SSH key
		config.Auth = []ssh.AuthMethod{
			ssh.Password(password),
		}
	} else {
		color.Red("No authentication method provided - need either password or SSH key")
		ProcessingErrors = append(ProcessingErrors, "No authentication method provided - need either password or SSH key")
		return nil, fmt.Errorf("no authentication method provided - need either password or SSH key")
	}

	addr := fmt.Sprintf("%s:%s", host, port)

	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		color.Red("Failed to dial: %v", err)
		ProcessingErrors = append(ProcessingErrors, "Failed to dial: "+err.Error())
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		color.Red("Failed to create SFTP client: %v", err)
		ProcessingErrors = append(ProcessingErrors, "Failed to create SFTP client: "+err.Error())
		return nil, fmt.Errorf("failed to create SFTP client: %w", err)
	}

	fmt.Println("SFTP client created successfully")

	return sftpClient, nil
}

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
		}

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
