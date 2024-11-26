package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
	"local-to-minio-copier/internal/db"
	"local-to-minio-copier/internal/sync"
	"local-to-minio-copier/pkg/models"
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
				},
				Action: startSync,
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

	db, err := db.New(projectName)
	if err != nil {
		return err
	}
	defer db.Close()

	project, err := db.GetProject(projectName)
	if err != nil {
		return err
	}

	scanner := sync.NewScanner(db, project)
	if err := scanner.ScanFiles(); err != nil {
		return fmt.Errorf("failed to scan files: %v", err)
	}

	fmt.Println("File scan completed successfully")
	return nil
}

func startSync(c *cli.Context) error {
	projectName := c.String("project")

	db, err := db.New(projectName)
	if err != nil {
		return err
	}
	defer db.Close()

	project, err := db.GetProject(projectName)
	if err != nil {
		return err
	}

	syncer, err := sync.NewSyncer(db, project)
	if err != nil {
		return err
	}

	if err := syncer.SyncFiles(); err != nil {
		return fmt.Errorf("failed to sync files: %v", err)
	}

	return nil
}
