// main.go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/urfave/cli/v2"
)

type FileRecord struct {
	FilePath     string
	Size         int64
	Timestamp    time.Time
	UploadStatus string
}

type Project struct {
	Name        string
	SourcePath  string
	Destination struct {
		Endpoint  string
		Bucket    string
		Folder    string
		AccessKey string
		SecretKey string
	}
}

func main() {
	app := &cli.App{
		Name:  "msync",
		Usage: "MinIO sync tool for local to remote synchronization",
		Commands: []*cli.Command{
			{
				Name:  "create",
				Usage: "Create a new sync project",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "name",
						Usage:    "Project name",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "source",
						Usage:    "Source directory path",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "endpoint",
						Usage:    "MinIO endpoint",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "bucket",
						Usage:    "MinIO bucket",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "folder",
						Usage: "Destination folder path in bucket",
						Value: "", // Optional, defaults to root
					},
					&cli.StringFlag{
						Name:     "access-key",
						Usage:    "MinIO access key",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "secret-key",
						Usage:    "MinIO secret key",
						Required: true,
					},
				},
				Action: createProject,
			},
			{
				Name:  "scan",
				Usage: "Scan source directory and update file list",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "project",
						Usage:    "Project name",
						Required: true,
					},
				},
				Action: scanFiles,
			},
			{
				Name:  "status",
				Usage: "Show project status",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "project",
						Usage:    "Project name",
						Required: true,
					},
				},
				Action: showStatus,
			},
			{
				Name:  "sync",
				Usage: "Start or resume synchronization",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "project",
						Usage:    "Project name",
						Required: true,
					},
				},
				Action: startSync,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func initDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "msync.db")
	if err != nil {
		return nil, err
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
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
	return db, err
}

func createProject(c *cli.Context) error {
	db, err := initDB()
	if err != nil {
		return err
	}
	defer db.Close()

	// Clean and validate folder path
	folder := strings.Trim(c.String("folder"), "/")
	if folder != "" {
		folder = folder + "/"
	}

	// Insert project into database
	_, err = db.Exec(`
        INSERT INTO projects (name, source_path, endpoint, bucket, folder, access_key, secret_key)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    `,
		c.String("name"),
		c.String("source"),
		c.String("endpoint"),
		c.String("bucket"),
		folder,
		c.String("access-key"),
		c.String("secret-key"),
	)

	if err != nil {
		return fmt.Errorf("failed to create project: %v", err)
	}

	fmt.Printf("Project '%s' created successfully\n", c.String("name"))
	return nil
}

func scanFiles(c *cli.Context) error {
	db, err := initDB()
	if err != nil {
		return err
	}
	defer db.Close()

	projectName := c.String("project")

	// Get project details
	var project Project
	err = db.QueryRow(`
        SELECT name, source_path, endpoint, bucket, access_key, secret_key 
        FROM projects WHERE name = ?
    `, projectName).Scan(
		&project.Name,
		&project.SourcePath,
		&project.Destination.Endpoint,
		&project.Destination.Bucket,
		&project.Destination.AccessKey,
		&project.Destination.SecretKey,
	)
	if err != nil {
		return fmt.Errorf("project not found: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get existing files
	existingFiles := make(map[string]FileRecord)
	rows, err := tx.Query(`
        SELECT file_path, size, timestamp, upload_status 
        FROM files 
        WHERE project_name = ?
    `, projectName)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var record FileRecord
		var timestamp string
		err = rows.Scan(&record.FilePath, &record.Size, &timestamp, &record.UploadStatus)
		if err != nil {
			return err
		}
		record.Timestamp, _ = time.Parse("2006-01-02 15:04:05", timestamp)
		existingFiles[record.FilePath] = record
	}

	// Scan directory
	err = filepath.Walk(project.SourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(project.SourcePath, path)
		if err != nil {
			return err
		}

		existing, exists := existingFiles[relPath]
		if !exists || existing.Timestamp != info.ModTime() || existing.Size != info.Size() {
			// Insert or update file record
			_, err = tx.Exec(`
                INSERT OR REPLACE INTO files (project_name, file_path, size, timestamp, upload_status)
                VALUES (?, ?, ?, ?, ?)
            `,
				projectName,
				relPath,
				info.Size(),
				info.ModTime().Format("2006-01-02 15:04:05"),
				"pending",
			)
			if err != nil {
				return err
			}
		}
		delete(existingFiles, relPath)
		return nil
	})
	if err != nil {
		return err
	}

	// Remove files that no longer exist
	for filePath := range existingFiles {
		_, err = tx.Exec(`
            DELETE FROM files 
            WHERE project_name = ? AND file_path = ?
        `, projectName, filePath)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	fmt.Println("File scan completed successfully")
	return nil
}

func showStatus(c *cli.Context) error {
	db, err := initDB()
	if err != nil {
		return err
	}
	defer db.Close()

	projectName := c.String("project")

	var totalFiles, pendingFiles, completedFiles int
	var totalSize, completedSize int64

	err = db.QueryRow(`
        SELECT 
            COUNT(*) as total_files,
            SUM(CASE WHEN upload_status = 'pending' THEN 1 ELSE 0 END) as pending_files,
            SUM(CASE WHEN upload_status = 'completed' THEN 1 ELSE 0 END) as completed_files,
            SUM(size) as total_size,
            SUM(CASE WHEN upload_status = 'completed' THEN size ELSE 0 END) as completed_size
        FROM files
        WHERE project_name = ?
    `, projectName).Scan(&totalFiles, &pendingFiles, &completedFiles, &totalSize, &completedSize)

	if err != nil {
		return err
	}

	fmt.Printf("Project: %s\n", projectName)
	fmt.Printf("Total files: %d\n", totalFiles)
	fmt.Printf("Pending files: %d\n", pendingFiles)
	fmt.Printf("Completed files: %d\n", completedFiles)
	fmt.Printf("Total size: %.2f MB\n", float64(totalSize)/1024/1024)
	fmt.Printf("Completed size: %.2f MB (%.1f%%)\n",
		float64(completedSize)/1024/1024,
		float64(completedSize)/float64(totalSize)*100)

	return nil
}

func startSync(c *cli.Context) error {
	db, err := initDB()
	if err != nil {
		return err
	}
	defer db.Close()

	projectName := c.String("project")

	// Get project details
	var project Project
	err = db.QueryRow(`
        SELECT name, source_path, endpoint, bucket, folder, access_key, secret_key 
        FROM projects WHERE name = ?
    `, projectName).Scan(
		&project.Name,
		&project.SourcePath,
		&project.Destination.Endpoint,
		&project.Destination.Bucket,
		&project.Destination.Folder,
		&project.Destination.AccessKey,
		&project.Destination.SecretKey,
	)
	if err != nil {
		return fmt.Errorf("project not found: %v", err)
	}

	// Initialize MinIO client
	minioClient, err := minio.New(project.Destination.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(project.Destination.AccessKey, project.Destination.SecretKey, ""),
		Secure: true,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize MinIO client: %v", err)
	}

	// Get pending files
	rows, err := db.Query(`
        SELECT file_path, size 
        FROM files 
        WHERE project_name = ? AND upload_status = 'pending'
        ORDER BY size DESC
    `, projectName)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var filePath string
		var size int64
		err = rows.Scan(&filePath, &size)
		if err != nil {
			return err
		}

		fullPath := filepath.Join(project.SourcePath, filePath)

		// Combine destination folder with file path
		destinationPath := project.Destination.Folder + filePath

		// Upload file
		_, err = minioClient.FPutObject(context.Background(),
			project.Destination.Bucket,
			destinationPath,
			fullPath,
			minio.PutObjectOptions{})

		if err != nil {
			fmt.Printf("Failed to upload %s: %v\n", filePath, err)
			continue
		}

		// Update status
		_, err = db.Exec(`
            UPDATE files 
            SET upload_status = 'completed' 
            WHERE project_name = ? AND file_path = ?
        `, projectName, filePath)
		if err != nil {
			return err
		}

		fmt.Printf("Uploaded: %s -> %s\n", filePath, destinationPath)
	}

	fmt.Println("Sync completed successfully")
	return nil
}
