package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"local-to-minio-copier/internal/db"
	"local-to-minio-copier/internal/sync"
	"local-to-minio-copier/pkg/models"
	"local-to-minio-copier/pkg/utils"

	"github.com/urfave/cli/v2"
)

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
				Name:  "scan",
				Usage: "Scan files in project directory",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "project",
						Usage:    "Project name",
						Required: true,
					},
					&cli.IntFlag{
						Name:  "workers",
						Usage: "Number of parallel workers for scanning files",
						Value: 8,
					},
					&cli.IntFlag{
						Name:  "batch",
						Usage: "Batch size for scanning files",
						Value: 1000,
					},
				},
				Action: scanFiles,
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
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

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

func scanFiles(c *cli.Context) error {
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

	scannerConfig := sync.ScannerConfig{
		NumWorkers: c.Int("workers"),
		BatchSize:  c.Int("batch"),
	}

	scanner := sync.NewScanner(db, project, &scannerConfig)
	if err := scanner.ScanFiles(); err != nil {
		return fmt.Errorf("failed to scan files: %v", err)
	}

	fmt.Println("Scan completed successfully")
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
