package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/chmdznr/oss-local-to-minio-copier/internal/db"
	"github.com/chmdznr/oss-local-to-minio-copier/internal/sync"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/models"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/utils"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/version"
	"github.com/urfave/cli/v2"
)

func main() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"v"},
		Usage:   "print the version",
	}

	app := &cli.App{
		Name:                 "msync",
		Usage:                "MinIO sync tool for local to remote synchronization",
		Version:             version.Version,
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			{
				Name:  "version",
				Usage: "Print detailed version information",
				Action: func(c *cli.Context) error {
					fmt.Printf("Version:    %s\n", version.Version)
					fmt.Printf("Git commit: %s\n", version.GitCommit)
					fmt.Printf("Built:      %s\n", version.BuildTime)
					return nil
				},
			},
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
						Usage:    "MinIO bucket name",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "folder",
						Usage:    "Destination folder path",
						Required: true,
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
				Name:  "sync",
				Usage: "Start synchronization",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "project",
						Usage:    "Project name",
						Required: true,
					},
					&cli.IntFlag{
						Name:  "workers",
						Usage: "Number of parallel workers for uploading files",
						Value: 16,
					},
					&cli.IntFlag{
						Name:  "batch",
						Usage: "Batch size for uploading files",
						Value: 100,
					},
				},
				Action: startSync,
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
				Name:  "import",
				Usage: "Import file list from CSV",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "project",
						Usage:    "Project name",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "csv",
						Usage:    "Path to CSV file",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "source",
						Usage:    "Source directory path",
						Required: true,
					},
					&cli.IntFlag{
						Name:  "batch",
						Usage: "Batch size for processing",
						Value: 1000,
					},
				},
				Action: importCSV,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

// createProject creates a new project configuration in the database.
//
// It takes the following flags from the cli context:
// - name: the name of the project
// - source: the local path to the source files
// - endpoint: the endpoint URL of the Minio server
// - bucket: the name of the bucket to store the files
// - folder: the subfolder within the bucket to store the files
// - access-key: the access key to use for authentication
// - secret-key: the secret key to use for authentication
//
// If the project is created successfully, it prints a success message to stdout.
func createProject(c *cli.Context) error {
	projectName := c.String("name")

	db, err := db.New(projectName)
	if err != nil {
		return err
	}
	defer db.Close()

	// Clean and validate folder path
	folder := strings.Trim(c.String("folder"), "/")
	if folder != "" {
		folder = folder + "/"
	}

	project := &models.Project{
		Name:       projectName,
		SourcePath: c.String("source"),
	}
	project.Destination.Endpoint = c.String("endpoint")
	project.Destination.Bucket = c.String("bucket")
	project.Destination.Folder = folder
	project.Destination.AccessKey = c.String("access-key")
	project.Destination.SecretKey = c.String("secret-key")

	if err := db.CreateProject(project); err != nil {
		return fmt.Errorf("failed to create project: %v", err)
	}

	fmt.Printf("Project '%s' created successfully\n", projectName)
	return nil
}

func startSync(c *cli.Context) error {
	projectName := c.String("project")
	if projectName == "" {
		return fmt.Errorf("project name is required")
	}

	db, err := db.New(projectName)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	project, err := db.GetProject(projectName)
	if err != nil {
		return fmt.Errorf("failed to get project: %v", err)
	}

	syncerConfig := sync.SyncerConfig{
		NumWorkers: c.Int("workers"),
		BatchSize:  c.Int("batch"),
	}

	syncer, err := sync.NewSyncer(db, project, &syncerConfig)
	if err != nil {
		return fmt.Errorf("failed to create syncer: %v", err)
	}

	if err := syncer.SyncFiles(); err != nil {
		return fmt.Errorf("failed to sync files: %v", err)
	}

	fmt.Println("Sync completed successfully")
	return nil
}

// showStatus shows the status of the project
//
// It will show the number of total files, files uploaded, files pending, and the progress of the sync process.
func showStatus(c *cli.Context) error {
	projectName := c.String("project")
	if projectName == "" {
		return fmt.Errorf("project name is required")
	}

	db, err := db.New(projectName)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	project, err := db.GetProject(projectName)
	if err != nil {
		return fmt.Errorf("failed to get project: %v", err)
	}

	stats, err := db.GetStats(projectName)
	if err != nil {
		return fmt.Errorf("failed to get stats: %v", err)
	}

	fmt.Printf("Project: %s\n", project.Name)
	fmt.Printf("Source Path: %s\n", project.SourcePath)
	fmt.Printf("Destination: %s/%s/\n", project.Destination.Endpoint, project.Destination.Bucket)
	fmt.Printf("Total Files: %d (Size: %s)\n", stats.TotalFiles, utils.FormatSize(stats.TotalSize))
	fmt.Printf("Files Uploaded: %d (Size: %s)\n", stats.UploadedFiles, utils.FormatSize(stats.UploadedSize))
	fmt.Printf("Files Pending: %d (Size: %s)\n", stats.PendingFiles, utils.FormatSize(stats.PendingSize))

	fileProgress := float64(stats.UploadedFiles) / float64(stats.TotalFiles) * 100
	sizeProgress := float64(stats.UploadedSize) / float64(stats.TotalSize) * 100
	fmt.Printf("Progress: %.2f%% (Files), %.2f%% (Size)\n", fileProgress, sizeProgress)

	return nil
}

// importCSV imports file records from a CSV file to the database
//
// The CSV file must contain the following columns:
//
// - id_upload
// - path
// - nama_modul
// - file_type
// - nama_file_asli
// - id_profile
// - id
// - str_key
// - str_subkey
//
// The CSV file may contain additional columns, which will be ignored.
//
// The function processes the CSV file in batches of batchSize records.
// When the batch size is reached, the batch is saved to the database.
// The function prints a message every time a batch is saved, indicating
// the number of records processed so far.
//
// The function returns an error if the project does not exist, if the
// CSV file is malformed, or if there is an error saving the records to
// the database.
func importCSV(c *cli.Context) error {
	projectName := c.String("project")
	csvPath := c.String("csv")
	sourcePath := c.String("source")
	batchSize := c.Int("batch")

	if projectName == "" {
		return fmt.Errorf("project name is required")
	}
	if csvPath == "" {
		return fmt.Errorf("CSV file path is required")
	}
	if sourcePath == "" {
		return fmt.Errorf("source directory path is required")
	}
	if batchSize <= 0 {
		batchSize = 1000 // default batch size
	}

	// Open database connection
	db, err := db.New(projectName)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	// Open CSV file
	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("error opening CSV file: %v", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	// Skip header row
	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("error reading CSV header: %v", err)
	}

	// Initialize counters
	processed := 0
	missing := 0
	batch := make([][]string, 0, batchSize)

	// Process each row
	for lineNum := 2; ; lineNum++ { // Start from 2 to account for header row
		record, err := reader.Read()
		if err == io.EOF {
			// Process remaining batch
			if len(batch) > 0 {
				if err := processBatch(db, projectName, sourcePath, batch, &processed, &missing); err != nil {
					return err
				}
			}
			break
		}
		if err != nil {
			return fmt.Errorf("error reading CSV line %d: %v", lineNum, err)
		}

		if len(record) < 4 {
			return fmt.Errorf("invalid CSV format at line %d: expected at least 4 columns", lineNum)
		}

		batch = append(batch, record)
		if len(batch) >= batchSize {
			if err := processBatch(db, projectName, sourcePath, batch, &processed, &missing); err != nil {
				return err
			}
			batch = batch[:0] // Clear batch
		}
	}

	fmt.Printf("\nImport Summary:\n")
	fmt.Printf("- Successfully processed: %d files\n", processed)
	fmt.Printf("- Missing files: %d\n", missing)

	return nil
}

func processBatch(db *db.DB, projectName, sourcePath string, batch [][]string, processed, missing *int) error {
	for _, record := range batch {
		filePath := filepath.Join(sourcePath, record[3])
		
		// Check if file exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			if err := db.AddMissingFile(filePath, 0); err != nil {
				log.Printf("Error recording missing file: %v", err)
			}
			*missing++
			continue
		}

		// Add file to database
		if err := db.AddFileFromCSV(projectName, record[0], record[1], record[2], filePath); err != nil {
			return fmt.Errorf("error adding file: %v", err)
		}
		*processed++
	}
	return nil
}
