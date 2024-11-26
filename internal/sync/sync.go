package sync

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"local-to-minio-copier/internal/db"
	"local-to-minio-copier/pkg/models"
	"os"
	"path/filepath"
	"strings"
)

// Scanner handles file scanning operations
type Scanner struct {
	db     *db.DB
	project *models.Project
}

// NewScanner creates a new scanner instance
func NewScanner(db *db.DB, project *models.Project) *Scanner {
	return &Scanner{
		db:     db,
		project: project,
	}
}

// ScanFiles scans all files in the project directory
func (s *Scanner) ScanFiles() error {
	return filepath.Walk(s.project.SourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(s.project.SourcePath, path)
		if err != nil {
			return err
		}

		record := &models.FileRecord{
			FilePath:     relPath,
			Size:         info.Size(),
			Timestamp:    info.ModTime(),
			UploadStatus: "pending",
		}

		return s.db.SaveFileRecord(s.project.Name, record)
	})
}

// Syncer handles file synchronization operations
type Syncer struct {
	db          *db.DB
	project     *models.Project
	minioClient *minio.Client
}

// NewSyncer creates a new syncer instance
func NewSyncer(db *db.DB, project *models.Project) (*Syncer, error) {
	minioClient, err := minio.New(project.Destination.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(project.Destination.AccessKey, project.Destination.SecretKey, ""),
		Secure: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize MinIO client: %v", err)
	}

	return &Syncer{
		db:          db,
		project:     project,
		minioClient: minioClient,
	}, nil
}

// SyncFiles synchronizes pending files
func (s *Syncer) SyncFiles() error {
	files, err := s.db.GetPendingFiles(s.project.Name)
	if err != nil {
		return err
	}

	for _, file := range files {
		fullPath := filepath.Join(s.project.SourcePath, file.FilePath)
		destinationPath := strings.ReplaceAll(s.project.Destination.Folder+file.FilePath, "\\", "/")

		_, err = s.minioClient.FPutObject(context.Background(),
			s.project.Destination.Bucket,
			destinationPath,
			fullPath,
			minio.PutObjectOptions{})

		if err != nil {
			fmt.Printf("Failed to upload %s: %v\n", file.FilePath, err)
			continue
		}

		err = s.db.UpdateFileStatus(s.project.Name, file.FilePath, "uploaded")
		if err != nil {
			fmt.Printf("Failed to update status for %s: %v\n", file.FilePath, err)
			continue
		}

		fmt.Printf("Uploaded: %s -> %s\n", file.FilePath, destinationPath)
	}

	return nil
}
