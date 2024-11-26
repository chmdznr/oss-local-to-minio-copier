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
			PRIMARY KEY (project_name, file_path),
			FOREIGN KEY (project_name) REFERENCES projects(name)
		);
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
