package sync

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chmdznr/oss-local-to-minio-copier/internal/db"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/models"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/utils"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"encoding/json"
)

// Syncer handles file synchronization operations
type Syncer struct {
	db          *db.DB
	project     *models.Project
	minioClient *minio.Client
	numWorkers  int
	batchSize   int
}

// SyncerConfig holds configuration for the syncer
type SyncerConfig struct {
	NumWorkers int
	BatchSize  int
}

// DefaultSyncerConfig returns default syncer configuration
func DefaultSyncerConfig() SyncerConfig {
	return SyncerConfig{
		NumWorkers: 16,
		BatchSize:  100,
	}
}

// NewSyncer creates a new syncer instance
func NewSyncer(db *db.DB, project *models.Project, config *SyncerConfig) (*Syncer, error) {
	if config == nil {
		defaultConfig := DefaultSyncerConfig()
		config = &defaultConfig
	}

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
		numWorkers:  config.NumWorkers,
		batchSize:   config.BatchSize,
	}, nil
}

type syncProgress struct {
	TotalFiles    int64
	TotalSize     int64
	UploadedFiles int64
	UploadedSize  int64
	SkippedFiles  int64
	SkippedSize   int64
	RetryFiles    int64
	RetrySize     int64
	startTime     time.Time
	lastUpdate    time.Time
	lastSize      int64
	sync.Mutex
}

func newSyncProgress(totalFiles int64, totalSize int64) *syncProgress {
	now := time.Now()
	return &syncProgress{
		TotalFiles: totalFiles,
		TotalSize:  totalSize,
		startTime:  now,
		lastUpdate: now,
	}
}

func (p *syncProgress) Update(size int64, isRetry bool) {
	p.Lock()
	defer p.Unlock()
	p.UploadedFiles++
	p.UploadedSize += size
	if isRetry {
		p.RetryFiles++
		p.RetrySize += size
	}
}

func (p *syncProgress) Skip(size int64) {
	p.Lock()
	defer p.Unlock()
	p.SkippedFiles++
	p.SkippedSize += size
}

func (p *syncProgress) getSpeed() (avgSpeed float64, currentSpeed float64) {
	now := time.Now()
	totalDuration := now.Sub(p.startTime).Seconds()
	if totalDuration > 0 {
		avgSpeed = float64(p.UploadedSize) / totalDuration
	}

	// Calculate current speed over the last update interval
	intervalDuration := now.Sub(p.lastUpdate).Seconds()
	sizeDiff := float64(p.UploadedSize - p.lastSize)
	if intervalDuration > 0 {
		currentSpeed = sizeDiff / intervalDuration
	}

	// Update last values for next calculation
	p.lastUpdate = now
	p.lastSize = p.UploadedSize

	return avgSpeed, currentSpeed
}

func formatSpeed(bytesPerSecond float64) string {
	if bytesPerSecond < 1024 {
		return fmt.Sprintf("%.1f B/s", bytesPerSecond)
	} else if bytesPerSecond < 1024*1024 {
		return fmt.Sprintf("%.1f KB/s", bytesPerSecond/1024)
	} else if bytesPerSecond < 1024*1024*1024 {
		return fmt.Sprintf("%.1f MB/s", bytesPerSecond/1024/1024)
	}
	return fmt.Sprintf("%.1f GB/s", bytesPerSecond/1024/1024/1024)
}

func (p *syncProgress) Print() {
	p.Lock()
	defer p.Unlock()

	avgSpeed, currentSpeed := p.getSpeed()
	percentage := float64(p.UploadedSize) / float64(p.TotalSize) * 100

	fmt.Printf("\rProgress: %d/%d files (%.1f%%) - %s/%s | Speed: %s (avg: %s) | Retried: %d (%s) - Skipped: %d (%s)",
		p.UploadedFiles,
		p.TotalFiles,
		percentage,
		utils.FormatSize(p.UploadedSize),
		utils.FormatSize(p.TotalSize),
		formatSpeed(currentSpeed),
		formatSpeed(avgSpeed),
		p.RetryFiles,
		utils.FormatSize(p.RetrySize),
		p.SkippedFiles,
		utils.FormatSize(p.SkippedSize),
	)
}

// SyncFiles synchronizes pending files using a worker pool
func (s *Syncer) SyncFiles() error {
	// Create worker pool
	type workItem struct {
		filePath        string
		destinationPath string
		size            int64
		isRetry         bool
		metadata        map[string]string
	}

	jobs := make(chan workItem, s.numWorkers)
	results := make(chan []string, s.numWorkers)
	errors := make(chan error, 1)

	// Initialize progress tracking
	files, err := s.db.GetPendingFiles(s.project.Name)
	if err != nil {
		return err
	}

	var totalSize int64
	retriesCount := 0
	for _, file := range files {
		totalSize += file.Size
		if file.UploadStatus == "failed" {
			retriesCount++
		}
	}

	progress := newSyncProgress(int64(len(files)), totalSize)

	fmt.Printf("Starting sync of %d files (%s) - %d files being retried...\n",
		progress.TotalFiles,
		utils.FormatSize(progress.TotalSize),
		retriesCount)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < s.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var completedFiles []string

			for job := range jobs {
				fullPath := filepath.Join(s.project.SourcePath, job.filePath)

				// Check if file still exists
				if _, err := os.Stat(fullPath); os.IsNotExist(err) {
					log.Printf("\nSkipping %s: file no longer exists\n", job.filePath)
					progress.Skip(job.size)
					progress.Print()
					
					// Mark as skipped in database
					if err := s.db.UpdateFileStatus(s.project.Name, job.filePath, "skipped"); err != nil {
						log.Printf("\nFailed to update status for %s: %v\n", job.filePath, err)
					}
					continue
				}

				// Set metadata if available
				var opts minio.PutObjectOptions
				if job.metadata != nil {
					opts.UserMetadata = job.metadata
				}

				_, err := s.minioClient.FPutObject(
					context.Background(),
					s.project.Destination.Bucket,
					job.destinationPath,
					fullPath,
					opts,
				)

				if err != nil {
					log.Printf("\nFailed to upload %s: %v\n", job.filePath, err)
					// Mark as failed in database
					if dbErr := s.db.UpdateFileStatus(s.project.Name, job.filePath, "failed"); dbErr != nil {
						log.Printf("Failed to update status for %s: %v\n", job.filePath, dbErr)
					}
					continue
				}

				// After successful upload, update the metadata in database
				metadataJSON, err := json.Marshal(job.metadata)
				if err != nil {
					log.Printf("\nWarning: Failed to marshal metadata for %s: %v\n", job.filePath, err)
				} else {
					if err := s.db.UpdateFileMetadata(s.project.Name, job.filePath, string(metadataJSON)); err != nil {
						log.Printf("\nWarning: Failed to update metadata for %s: %v\n", job.filePath, err)
					}
				}

				completedFiles = append(completedFiles, job.filePath)
				progress.Update(job.size, job.isRetry)
				progress.Print()

				// Update status in batches
				if len(completedFiles) >= s.batchSize {
					results <- completedFiles
					completedFiles = nil
				}
			}

			if len(completedFiles) > 0 {
				results <- completedFiles
			}
		}()
	}

	// Start result processor
	go func() {
		for completedFiles := range results {
			if err := s.db.UpdateFileStatusBatch(s.project.Name, completedFiles, "uploaded"); err != nil {
				errors <- err
				return
			}
		}
	}()

	// Send jobs to workers
	for _, file := range files {
		// Ensure folder has trailing slash and construct path
		folder := strings.TrimRight(s.project.Destination.Folder, "/") + "/"
		destinationPath := fmt.Sprintf("%s%s", folder, file.IDFile)

		// Create complete metadata
		metadata := map[string]string{
			"path":          file.FilePath,
			"nama_modul":    "",  // Will be populated from CSV metadata
			"nama_file_asli": "", // Will be populated from CSV metadata
			"id_profile":    "",  // Will be populated from CSV metadata
			"bucket":        fmt.Sprintf("%s/%s", s.project.Destination.Bucket, destinationPath),
			"existing_id":   file.IDFromCSV,
		}

		// Parse existing metadata if available
		if file.Metadata != "" {
			var existingMeta map[string]string
			if err := json.Unmarshal([]byte(file.Metadata), &existingMeta); err == nil {
				metadata["nama_modul"] = existingMeta["nama_modul"]
				metadata["nama_file_asli"] = existingMeta["nama_file_asli"]
				metadata["id_profile"] = existingMeta["id_profile"]
			}
		}

		jobs <- workItem{
			filePath:        file.FilePath,
			destinationPath: destinationPath,
			size:           file.Size,
			isRetry:        file.UploadStatus == "failed",
			metadata:       metadata,
		}
	}

	close(jobs)
	wg.Wait()
	close(results)

	select {
	case err := <-errors:
		return err
	default:
		avgSpeed, _ := progress.getSpeed()
		fmt.Printf("\nSync completed in %s:\n", time.Since(progress.startTime).Round(time.Second))
		fmt.Printf("- Uploaded: %d files (%s) at %s average\n",
			progress.UploadedFiles,
			utils.FormatSize(progress.UploadedSize),
			formatSpeed(avgSpeed))
		fmt.Printf("- Retried: %d files (%s)\n", progress.RetryFiles, utils.FormatSize(progress.RetrySize))
		fmt.Printf("- Skipped: %d files (%s)\n", progress.SkippedFiles, utils.FormatSize(progress.SkippedSize))
		return nil
	}
}
