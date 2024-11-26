package db

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"local-to-minio-copier/pkg/models"
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
			source_path TEXT,
			endpoint TEXT,
			bucket TEXT,
			folder TEXT,
			access_key TEXT,
			secret_key TEXT
		);
		CREATE TABLE IF NOT EXISTS files (
			project_name TEXT,
			file_path TEXT,
			size INTEGER,
			timestamp DATETIME,
			upload_status TEXT,
			PRIMARY KEY (project_name, file_path)
		);
		CREATE INDEX IF NOT EXISTS idx_files_status ON files(project_name, upload_status);
		CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(project_name, timestamp);
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

// GetPendingFiles retrieves pending files for a project
func (db *DB) GetPendingFiles(projectName string) ([]models.FileRecord, error) {
	rows, err := db.Query(`
		SELECT file_path, size 
		FROM files 
		WHERE project_name = ? AND upload_status = 'pending'
		ORDER BY size DESC
	`, projectName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []models.FileRecord
	for rows.Next() {
		var file models.FileRecord
		err = rows.Scan(&file.FilePath, &file.Size)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, nil
}

// UpdateFileStatus updates the status of a file
func (db *DB) UpdateFileStatus(projectName, filePath, status string) error {
	_, err := db.Exec(`
		UPDATE files 
		SET upload_status = ? 
		WHERE project_name = ? AND file_path = ?
	`, status, projectName, filePath)
	return err
}

// SaveFileRecord saves a file record
func (db *DB) SaveFileRecord(projectName string, record *models.FileRecord) error {
	_, err := db.Exec(`
		INSERT OR REPLACE INTO files (project_name, file_path, size, timestamp, upload_status)
		VALUES (?, ?, ?, ?, ?)
	`, projectName, record.FilePath, record.Size, record.Timestamp, record.UploadStatus)
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
		INSERT OR REPLACE INTO files (project_name, file_path, size, timestamp, upload_status)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, record := range records {
		_, err = stmt.Exec(
			projectName,
			record.FilePath,
			record.Size,
			record.Timestamp,
			record.UploadStatus,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// UpdateFileStatusBatch updates the status of multiple files in a single transaction
func (db *DB) UpdateFileStatusBatch(projectName string, filePaths []string, status string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		UPDATE files 
		SET upload_status = ? 
		WHERE project_name = ? AND file_path = ?
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, filePath := range filePaths {
		_, err = stmt.Exec(status, projectName, filePath)
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
		SELECT COUNT(*), COALESCE(SUM(size), 0)
		FROM files
		WHERE project_name = ?
	`, projectName).Scan(&totalFiles, &totalSize)
	if err != nil {
		return
	}

	// Get uploaded files and size
	err = db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(size), 0)
		FROM files
		WHERE project_name = ? AND upload_status = 'uploaded'
	`, projectName).Scan(&uploadedFiles, &uploadedSize)
	if err != nil {
		return
	}

	return
}

// GetStats returns statistics about files in the project
func (db *DB) GetStats(projectName string) (*models.Stats, error) {
	var stats models.Stats
	err := db.QueryRow(`
		SELECT 
			COUNT(*) as total_files,
			COALESCE(SUM(size), 0) as total_size,
			COUNT(CASE WHEN upload_status = 'uploaded' THEN 1 END) as uploaded_files,
			COALESCE(SUM(CASE WHEN upload_status = 'uploaded' THEN size ELSE 0 END), 0) as uploaded_size,
			COUNT(CASE WHEN upload_status = 'pending' THEN 1 END) as pending_files,
			COALESCE(SUM(CASE WHEN upload_status = 'pending' THEN size ELSE 0 END), 0) as pending_size
		FROM files 
		WHERE project_name = ?
	`, projectName).Scan(
		&stats.TotalFiles,
		&stats.TotalSize,
		&stats.UploadedFiles,
		&stats.UploadedSize,
		&stats.PendingFiles,
		&stats.PendingSize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get stats: %v", err)
	}
	return &stats, nil
}
