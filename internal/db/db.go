package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
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
	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	db := &DB{sqlDB}
	if err := db.initialize(); err != nil {
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
			COALESCE(id_permohonan, '') as id_permohonan,
			COALESCE(timestamp, '') as timestamp,
			COALESCE(status, '') as status,
			COALESCE(file_size, 0) as file_size
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
		err := rows.Scan(
			&file.FilePath,
			&file.IDFile,
			&file.IDPermohonan,
			&timestamp,
			&file.UploadStatus,
			&file.Size,
		)
		if err != nil {
			return nil, err
		}

		// Parse timestamps
		file.Timestamp, _ = time.Parse("2006-01-02 15:04:05", timestamp)

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
	`, status, now, now, projectName, filePath)
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
		_, err = stmt.Exec(status, now, now, projectName, filePath)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// SaveFileRecord saves a file record
func (db *DB) SaveFileRecord(projectName string, record *models.FileRecord) error {
	_, err := db.Exec(`
		INSERT OR REPLACE INTO files (project_name, filepath, status, timestamp)
		VALUES (?, ?, ?, ?)
	`, projectName, record.FilePath, record.UploadStatus, record.Timestamp)
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
			record.Timestamp,
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
	var stats models.Stats
	err := db.QueryRow(`
		SELECT 
			COUNT(*) as total_files,
			COALESCE(SUM(file_size), 0) as total_size,
			COUNT(CASE WHEN status = 'uploaded' THEN 1 END) as uploaded_files,
			COALESCE(SUM(CASE WHEN status = 'uploaded' THEN file_size ELSE 0 END), 0) as uploaded_size,
			COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_files,
			COALESCE(SUM(CASE WHEN status = 'pending' THEN file_size ELSE 0 END), 0) as pending_size,
			COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_files,
			COALESCE(SUM(CASE WHEN status = 'failed' THEN file_size ELSE 0 END), 0) as failed_size
		FROM files 
		WHERE project_name = ?
	`, projectName).Scan(
		&stats.TotalFiles,
		&stats.TotalSize,
		&stats.UploadedFiles,
		&stats.UploadedSize,
		&stats.PendingFiles,
		&stats.PendingSize,
		&stats.FailedFiles,
		&stats.FailedSize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get stats: %v", err)
	}
	return &stats, nil
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
	metadata := models.FileMetadata{
		Path:         csvRecord.Path,
		NamaModul:    csvRecord.NamaModul,
		NamaFileAsli: csvRecord.NamaFileAsli,
		IDProfile:    csvRecord.IDProfile,
		ExistingID:   csvRecord.ID,
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
		csvRecord.IDUpload,   // id_file
		csvRecord.StrKey,     // id_permohonan
		time.Now(),           // timestamp
		csvRecord.Path,       // filepath
		"pending",            // status
		"",                   // bucketpath (to be set during upload)
		string(metadataJSON), // f_metadata
		"migrator",           // userid
		time.Now(),           // created_at
		csvRecord.StrKey,     // str_key
		csvRecord.StrSubKey,  // str_subkey
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

	stmt, err := tx.Prepare(`
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
			status = excluded.status
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now()
	for _, record := range records {
		metadataJSON, err := json.Marshal(map[string]interface{}{
			"id_upload":      record.IDUpload,
			"nama_modul":     record.NamaModul,
			"nama_file_asli": record.NamaFileAsli,
			"id_profile":     record.IDProfile,
		})
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %v", err)
		}

		_, err = stmt.Exec(
			projectName,
			record.IDUpload,    // id_file
			record.ID,          // id_permohonan
			record.ID,          // id_from_csv
			record.Path,        // filepath
			record.Size,        // file_size
			record.FileType,    // file_type
			"",                 // bucketpath (to be set during upload)
			string(metadataJSON), // f_metadata
			"migrator",         // userid
			now,               // created_at
			record.StrKey,     // str_key
			record.StrSubKey,  // str_subkey
			now,               // timestamp
			"pending",         // status
		)
		if err != nil {
			return err
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
			COALESCE(timestamp, '') as timestamp
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
		err := rows.Scan(&file.FilePath, &file.UploadStatus, &timestamp)
		if err != nil {
			return nil, err
		}
		file.Timestamp, _ = time.Parse("2006-01-02 15:04:05", timestamp)
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
	ID        int64
	FilePath  string
	IDUpload  string
	CSVLine   int
	ReportedAt time.Time
}

// AddMissingFile adds a record of a missing file
func (db *DB) AddMissingFile(filePath string, idUpload string, csvLine int) error {
	query := `INSERT INTO missing_files (filepath, id_upload, csv_line) VALUES (?, ?, ?)`
	_, err := db.Exec(query, filePath, idUpload, csvLine)
	return err
}

// GetMissingFilesCount returns the total number of missing files
func (db *DB) GetMissingFilesCount() (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM missing_files").Scan(&count)
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
