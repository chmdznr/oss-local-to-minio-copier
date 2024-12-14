package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/chmdznr/oss-local-to-minio-copier/pkg/models"
	_ "github.com/mattn/go-sqlite3"
)

// DB represents a database connection
type DB struct {
	*sql.DB
}

// New creates a new database connection
func New(projectName string) (*DB, error) {
	fmt.Printf("Initializing database for project: %s\n", projectName)
	dbPath := fmt.Sprintf("%s.db", projectName)
	sqlDB, err := sql.Open("sqlite3", dbPath+"?_journal=WAL&_timeout=5000&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}

	// Set connection pool settings
	sqlDB.SetMaxOpenConns(1) // Limit to one connection to prevent "database is locked" errors
	sqlDB.SetMaxIdleConns(1)
	sqlDB.SetConnMaxLifetime(time.Hour)

	db := &DB{sqlDB}
	if err := db.initialize(); err != nil {
		sqlDB.Close()
		return nil, err
	}

	return db, nil
}

// initialize creates the necessary tables if they don't exist
func (db *DB) initialize() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS projects (
			name TEXT PRIMARY KEY,
			source_path TEXT NOT NULL,
			endpoint TEXT NOT NULL,
			bucket TEXT NOT NULL,
			folder TEXT NOT NULL,
			access_key TEXT,
			secret_key TEXT
		);

		CREATE TABLE IF NOT EXISTS files (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			project_name TEXT NOT NULL,
			id_file TEXT,
			id_permohonan TEXT,
			id_from_csv TEXT,
			filepath TEXT NOT NULL,
			file_size INTEGER,
			file_type TEXT,
			bucketpath TEXT,
			f_metadata TEXT,
			userid TEXT DEFAULT 'migrator',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			str_key TEXT,
			str_subkey TEXT,
			timestamp TEXT,
			status TEXT DEFAULT 'pending',
			UNIQUE(project_name, filepath)
		);

		CREATE TABLE IF NOT EXISTS missing_files (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			filepath TEXT NOT NULL,
			id_upload TEXT NOT NULL,
			csv_line INTEGER,
			reported_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX IF NOT EXISTS idx_files_project_name ON files(project_name);
		CREATE INDEX IF NOT EXISTS idx_files_filepath ON files(filepath);
		CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
		CREATE INDEX IF NOT EXISTS idx_missing_files_filepath ON missing_files(filepath);
		CREATE INDEX IF NOT EXISTS idx_missing_files_id_upload ON missing_files(id_upload);

		PRAGMA journal_mode=WAL;
		PRAGMA synchronous=NORMAL;
		PRAGMA temp_store=MEMORY;
		PRAGMA mmap_size=30000000000;
		PRAGMA page_size=4096;
		PRAGMA cache_size=-2000000;
	`)
	return err
}

// GetProject retrieves a project by name
func (db *DB) GetProject(name string) (*models.Project, error) {
	var project models.Project
	err := db.QueryRow(`
		SELECT name, source_path, endpoint, bucket, folder, access_key, secret_key 
		FROM projects WHERE name = ?
	`, name).Scan(
		&project.Name,
		&project.SourcePath,
		&project.Destination.Endpoint,
		&project.Destination.Bucket,
		&project.Destination.Folder,
		&project.Destination.AccessKey,
		&project.Destination.SecretKey,
	)
	if err != nil {
		return nil, fmt.Errorf("project not found: %v", err)
	}
	return &project, nil
}

// CreateProject creates a new project
func (db *DB) CreateProject(project *models.Project) error {
	_, err := db.Exec(`
		INSERT INTO projects (name, source_path, endpoint, bucket, folder, access_key, secret_key)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
		project.Name,
		project.SourcePath,
		project.Destination.Endpoint,
		project.Destination.Bucket,
		project.Destination.Folder,
		project.Destination.AccessKey,
		project.Destination.SecretKey,
	)
	return err
}

// GetPendingFiles returns all files that need to be uploaded
func (db *DB) GetPendingFiles(projectName string) ([]models.FileRecord, error) {
	rows, err := db.Query(`
		SELECT 
			filepath,
			COALESCE(id_file, '') as id_file,
			COALESCE(id_from_csv, '') as id_from_csv,
			COALESCE(id_permohonan, '') as id_permohonan,
			COALESCE(timestamp, '') as timestamp,
			COALESCE(status, '') as status,
			COALESCE(file_size, 0) as file_size,
			COALESCE(f_metadata, '') as f_metadata
		FROM files
		WHERE project_name = ? AND (status = 'pending' OR status = 'failed')
	`, projectName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []models.FileRecord
	for rows.Next() {
		var file models.FileRecord
		var timestamp string
		var metadataStr string
		err := rows.Scan(
			&file.FilePath,
			&file.IDFile,
			&file.IDFromCSV,
			&file.IDPermohonan,
			&timestamp,
			&file.UploadStatus,
			&file.Size,
			&metadataStr,
		)
		if err != nil {
			return nil, err
		}

		// Parse timestamps
		if timestamp != "" {
			if t, err := time.Parse(time.RFC3339, timestamp); err == nil {
				file.Timestamp = t
			} else if t, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
				file.Timestamp = time.Unix(t, 0)
			}
		}

		// Initialize empty metadata map
		file.Metadata = make(map[string]string)

		// Parse metadata if present
		if metadataStr != "" {
			var metadata map[string]interface{}
			if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
				log.Printf("Warning: Failed to parse metadata for file %s: %v", file.FilePath, err)
			} else {
				// Convert all values to strings
				for k, v := range metadata {
					if v != nil {
						file.Metadata[k] = fmt.Sprintf("%v", v)
					}
				}
			}
		}

		files = append(files, file)
	}
	return files, rows.Err()
}

// UpdateFileStatus updates the status of a file and its upload-related fields
func (db *DB) UpdateFileStatus(projectName, filePath, status string) error {
	now := time.Now()
	_, err := db.Exec(`
		UPDATE files 
		SET 
			status = ?,
			timestamp = ?,
			created_at = ?
		WHERE project_name = ? AND filepath = ?
	`, status, now.Format(time.RFC3339), now.Format(time.RFC3339), projectName, filePath)
	return err
}

// UpdateFileStatusBatch updates the status of multiple files in a batch
func (db *DB) UpdateFileStatusBatch(projectName string, filePaths []string, status string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now()
	stmt, err := tx.Prepare(`
		UPDATE files 
		SET 
			status = ?,
			timestamp = ?,
			created_at = ?
		WHERE project_name = ? AND filepath = ?
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, filePath := range filePaths {
		_, err = stmt.Exec(status, now.Format(time.RFC3339), now.Format(time.RFC3339), projectName, filePath)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// UpdateFilesStatus updates the status of multiple files in a batch
func (db *DB) UpdateFilesStatus(project string, filePaths []string, status string) error {
	// First check if any files exist
	if len(filePaths) == 0 {
		return nil
	}

	query := `UPDATE files SET status = ?, timestamp = ? WHERE project_name = ? AND filepath IN (?` + strings.Repeat(",?", len(filePaths)-1) + `)`
	args := make([]interface{}, len(filePaths)+3)
	args[0] = status
	args[1] = time.Now().Format(time.RFC3339)
	args[2] = project
	for i, path := range filePaths {
		args[i+3] = path
	}
	_, err := db.Exec(query, args...)
	return err
}

// SaveFileRecord saves a file record
func (db *DB) SaveFileRecord(projectName string, record *models.FileRecord) error {
	_, err := db.Exec(`
		INSERT OR REPLACE INTO files (project_name, filepath, status, timestamp)
		VALUES (?, ?, ?, ?)
	`, projectName, record.FilePath, record.UploadStatus, record.Timestamp.Format(time.RFC3339))
	return err
}

// SaveFileRecordsBatch saves multiple file records in a single transaction
func (db *DB) SaveFileRecordsBatch(projectName string, records []models.FileRecord) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO files (project_name, filepath, status, timestamp)
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, record := range records {
		_, err = stmt.Exec(
			projectName,
			record.FilePath,
			record.UploadStatus,
			record.Timestamp.Format(time.RFC3339),
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetFileStats returns statistics about files in the project
func (db *DB) GetFileStats(projectName string) (totalFiles, totalSize, uploadedFiles, uploadedSize int64, err error) {
	// Get total files and size
	err = db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(file_size), 0)
		FROM files
		WHERE project_name = ?
	`, projectName).Scan(&totalFiles, &totalSize)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	// Get uploaded files and size
	err = db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(file_size), 0)
		FROM files
		WHERE project_name = ? AND status = 'uploaded'
	`, projectName).Scan(&uploadedFiles, &uploadedSize)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	return totalFiles, totalSize, uploadedFiles, uploadedSize, nil
}

// GetStats returns statistics about files in the project
func (db *DB) GetStats(projectName string) (*models.Stats, error) {
	stats := &models.Stats{}

	// Get total files and size
	err := db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(file_size), 0)
		FROM files
		WHERE project_name = ?
	`, projectName).Scan(&stats.TotalFiles, &stats.TotalSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get total stats: %v", err)
	}

	// Get uploaded files and size
	err = db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(file_size), 0)
		FROM files
		WHERE project_name = ? AND status = 'uploaded'
	`, projectName).Scan(&stats.UploadedFiles, &stats.UploadedSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get uploaded stats: %v", err)
	}

	// Get pending files and size
	err = db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(file_size), 0)
		FROM files
		WHERE project_name = ? AND status = 'pending'
	`, projectName).Scan(&stats.PendingFiles, &stats.PendingSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending stats: %v", err)
	}

	// Get failed files and size
	err = db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(file_size), 0)
		FROM files
		WHERE project_name = ? AND status = 'failed'
	`, projectName).Scan(&stats.FailedFiles, &stats.FailedSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get failed stats: %v", err)
	}

	// Get missing files count
	err = db.QueryRow(`
		SELECT COUNT(DISTINCT id_upload)
		FROM missing_files
	`).Scan(&stats.MissingFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to get missing files count: %v", err)
	}

	return stats, nil
}

// GetFileByPath retrieves a file record by its path
func (db *DB) GetFileByPath(projectName string, filePath string) (*models.FileRecord, error) {
	var record models.FileRecord
	err := db.QueryRow(
		"SELECT filepath, status, timestamp FROM files WHERE project_name = ? AND filepath = ?",
		projectName, filePath,
	).Scan(&record.FilePath, &record.UploadStatus, &record.Timestamp)

	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &record, nil
}

// SaveFileRecordFromCSV saves a file record from CSV data
func (db *DB) SaveFileRecordFromCSV(projectName string, csvRecord *models.CSVRecord) error {
	metadata := map[string]string{
		"nama_modul":     csvRecord.NamaModul,
		"nama_file_asli": csvRecord.NamaFileAsli,
		"id_profile":     csvRecord.IDProfile,
		"existing_id":    csvRecord.ID,
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		INSERT INTO files (
			project_name, id_file, id_permohonan, timestamp, filepath, status,
			bucketpath, f_metadata, userid, created_at, str_key,
			str_subkey
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(project_name, filepath) DO UPDATE SET
			id_file = excluded.id_file,
			id_permohonan = excluded.id_permohonan,
			timestamp = excluded.timestamp,
			status = excluded.status,
			bucketpath = excluded.bucketpath,
			f_metadata = excluded.f_metadata,
			userid = excluded.userid,
			created_at = excluded.created_at,
			str_key = excluded.str_key,
			str_subkey = excluded.str_subkey
	`,
		projectName,
		csvRecord.IDUpload,              // id_file
		csvRecord.StrKey,                // id_permohonan
		time.Now().Format(time.RFC3339), // timestamp
		csvRecord.Path,                  // filepath
		"pending",                       // status
		"",                              // bucketpath (to be set during upload)
		string(metadataJSON),            // f_metadata
		"migrator",                      // userid
		time.Now().Format(time.RFC3339), // created_at
		csvRecord.StrKey,                // str_key
		csvRecord.StrSubKey,             // str_subkey
	)

	return err
}

// SaveFileRecordsFromCSVBatch saves multiple file records from CSV data in a single transaction
func (db *DB) SaveFileRecordsFromCSVBatch(projectName string, records []models.CSVRecord) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// First prepare statement to get existing record
	getStmt, err := tx.Prepare(`
		SELECT file_size, timestamp, status 
		FROM files 
		WHERE project_name = ? AND filepath = ?
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %v", err)
	}
	defer getStmt.Close()

	// Then prepare insert/update statement
	insertStmt, err := tx.Prepare(`
		INSERT INTO files (
			project_name, id_file, id_permohonan, id_from_csv,
			filepath, file_size, file_type, bucketpath, f_metadata,
			userid, created_at, str_key, str_subkey, timestamp, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(project_name, filepath) DO UPDATE SET
			id_file = excluded.id_file,
			id_permohonan = excluded.id_permohonan,
			id_from_csv = excluded.id_from_csv,
			file_size = excluded.file_size,
			file_type = excluded.file_type,
			bucketpath = excluded.bucketpath,
			f_metadata = excluded.f_metadata,
			userid = excluded.userid,
			created_at = excluded.created_at,
			str_key = excluded.str_key,
			str_subkey = excluded.str_subkey,
			timestamp = excluded.timestamp,
			status = ?  -- Status will be determined separately
	`)
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	now := time.Now()
	for _, record := range records {
		metadata := map[string]string{
			"nama_modul":     record.NamaModul,
			"nama_file_asli": record.NamaFileAsli,
			"id_profile":     record.IDProfile,
			"existing_id":    record.ID,
		}

		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %v", err)
		}

		// Get existing record if any
		var existingSize int64
		var existingTimestampStr string
		var existingStatus string
		status := "pending" // default for new records

		err = getStmt.QueryRow(projectName, record.Path).Scan(&existingSize, &existingTimestampStr, &existingStatus)
		if err == nil {
			// Record exists, check if file has changed by comparing size and modification time
			var existingTimestamp time.Time
			if existingTimestampStr != "" {
				// Try parsing as Unix timestamp first
				if timestamp, err := strconv.ParseInt(existingTimestampStr, 10, 64); err == nil {
					existingTimestamp = time.Unix(timestamp, 0)
				} else {
					// If not Unix timestamp, try parsing as RFC3339
					if t, err := time.Parse(time.RFC3339, existingTimestampStr); err == nil {
						existingTimestamp = t
					}
				}
			}

			recordModTime := time.Unix(record.ModTime, 0)
			if existingSize == record.Size && existingTimestamp.Equal(recordModTime) {
				status = existingStatus // preserve existing status if file hasn't changed
			} else {
				status = "pending" // file has changed, needs re-upload
			}
		} else if err != sql.ErrNoRows {
			return fmt.Errorf("failed to query existing record: %v", err)
		}

		// Convert ModTime to string format for database
		modTimeStr := fmt.Sprintf("%d", record.ModTime) // Store as Unix timestamp string

		// Do the insert/update with determined status
		_, err = insertStmt.Exec(
			projectName,
			record.IDUpload,          // id_file
			record.ID,                // id_permohonan
			record.ID,                // id_from_csv
			record.Path,              // filepath
			record.Size,              // file_size
			record.FileType,          // file_type
			"",                       // bucketpath (to be set during upload)
			string(metadataJSON),     // f_metadata
			"migrator",               // userid
			now.Format(time.RFC3339), // created_at
			record.StrKey,            // str_key
			record.StrSubKey,         // str_subkey
			modTimeStr,               // timestamp - store as string
			status,                   // determined status
			status,                   // status for ON CONFLICT UPDATE
		)
		if err != nil {
			return fmt.Errorf("failed to insert/update record: %v", err)
		}
	}

	return tx.Commit()
}

// GetProjectFiles returns all files for a given project
func (db *DB) GetProjectFiles(projectName string) ([]models.FileRecord, error) {
	rows, err := db.Query(`
		SELECT 
			filepath, 
			COALESCE(status, '') as status,
			COALESCE(timestamp, '') as timestamp,
			COALESCE(f_metadata, '') as f_metadata
		FROM files
		WHERE project_name = ?
	`, projectName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []models.FileRecord
	for rows.Next() {
		var file models.FileRecord
		var timestamp string
		var metadataStr string
		err := rows.Scan(&file.FilePath, &file.UploadStatus, &timestamp, &metadataStr)
		if err != nil {
			return nil, err
		}
		if timestamp != "" {
			if t, err := time.Parse(time.RFC3339, timestamp); err == nil {
				file.Timestamp = t
			} else if t, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
				file.Timestamp = time.Unix(t, 0)
			}
		}

		// Parse metadata if present
		if metadataStr != "" {
			if err := json.Unmarshal([]byte(metadataStr), &file.Metadata); err != nil {
				return nil, fmt.Errorf("failed to parse metadata for file %s: %v", file.FilePath, err)
			}
		}

		files = append(files, file)
	}
	return files, rows.Err()
}

// UpdateFileMetadata updates the metadata of a file
func (db *DB) UpdateFileMetadata(projectName, filePath, metadata string) error {
	_, err := db.Exec(`
		UPDATE files 
		SET f_metadata = ?
		WHERE project_name = ? AND filepath = ?
	`, metadata, projectName, filePath)
	return err
}

// AddFileFromCSV adds a file record from CSV data
func (db *DB) AddFileFromCSV(projectName, idFile, idPermohonan, timestamp, filePath string) error {
	query := `INSERT INTO files (project_name, id_file, id_permohonan, timestamp, filepath, status) 
              VALUES (?, ?, ?, ?, ?, 'pending')`
	_, err := db.Exec(query, projectName, idFile, idPermohonan, timestamp, filePath)
	return err
}

// MissingFile represents a file that was in the CSV but not found on disk
type MissingFile struct {
	ID         int64
	FilePath   string
	IDUpload   string
	CSVLine    int
	ReportedAt time.Time
}

// AddMissingFile adds a record of a missing file
func (db *DB) AddMissingFile(filePath string, idUpload string, csvLine int) error {
	query := `INSERT OR IGNORE INTO missing_files (filepath, id_upload, csv_line) VALUES (?, ?, ?)`
	_, err := db.Exec(query, filePath, idUpload, csvLine)
	return err
}

// GetMissingFilesCount returns the total number of missing files
func (db *DB) GetMissingFilesCount() (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(DISTINCT id_upload) FROM missing_files").Scan(&count)
	return count, err
}

// GetMissingFiles returns all missing files
func (db *DB) GetMissingFiles() ([]MissingFile, error) {
	query := `SELECT id, filepath, id_upload, csv_line, reported_at FROM missing_files ORDER BY csv_line`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []MissingFile
	for rows.Next() {
		var f MissingFile
		err := rows.Scan(&f.ID, &f.FilePath, &f.IDUpload, &f.CSVLine, &f.ReportedAt)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}
