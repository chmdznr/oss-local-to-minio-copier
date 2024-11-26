package sync

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"local-to-minio-copier/internal/db"
	"local-to-minio-copier/pkg/models"
	"local-to-minio-copier/pkg/utils"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Scanner handles file scanning operations
type Scanner struct {
	db         *db.DB
	project    *models.Project
	numWorkers int
	batchSize  int
}

type scanProgress struct {
	CurrentFiles   int64
	TotalSize      int64
	NewFiles       int64
	ModifiedFiles  int64
	RequeueFiles   int64
	UnchangedFiles int64
	sync.Mutex
}

func (p *scanProgress) Update(size int64, isNew, isModified, isRequeue bool) {
	p.Lock()
	defer p.Unlock()
	p.CurrentFiles++
	p.TotalSize += size
	if isNew {
		p.NewFiles++
	} else if isModified {
		p.ModifiedFiles++
	} else if isRequeue {
		p.RequeueFiles++
	} else {
		p.UnchangedFiles++
	}
}

func (p *scanProgress) Print() {
	fmt.Printf("\rScanning: %d files (%s) - New: %d, Modified: %d, Requeued: %d, Unchanged: %d",
		p.CurrentFiles,
		utils.FormatSize(p.TotalSize),
		p.NewFiles,
		p.ModifiedFiles,
		p.RequeueFiles,
		p.UnchangedFiles,
	)
}

// ScannerConfig holds configuration for the scanner
type ScannerConfig struct {
	NumWorkers int
	BatchSize  int
}

// DefaultScannerConfig returns default scanner configuration
func DefaultScannerConfig() ScannerConfig {
	return ScannerConfig{
		NumWorkers: 8,
		BatchSize:  1000,
	}
}

// NewScanner creates a new scanner instance
func NewScanner(db *db.DB, project *models.Project, config *ScannerConfig) *Scanner {
	if config == nil {
		defaultConfig := DefaultScannerConfig()
		config = &defaultConfig
	}
	return &Scanner{
		db:         db,
		project:    project,
		numWorkers: config.NumWorkers,
		batchSize:  config.BatchSize,
	}
}

// ScanFiles scans all files in the project directory using parallel workers
func (s *Scanner) ScanFiles() error {
	progress := &scanProgress{}
	recordChan := make(chan models.FileRecord, s.batchSize*2)
	errorChan := make(chan error, s.numWorkers)
	done := make(chan bool)

	// Start the batch processor
	go s.processBatches(recordChan, errorChan, done)

	// Create a semaphore to limit concurrent goroutines
	sem := make(chan struct{}, s.numWorkers)
	var wg sync.WaitGroup

	// Use WalkDir which is more efficient than Walk
	if err := filepath.WalkDir(s.project.SourcePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			wg.Add(1)
			// Acquire semaphore
			sem <- struct{}{}

			go func(filePath string, dirEntry fs.DirEntry) {
				defer wg.Done()
				defer func() { <-sem }() // Release semaphore

				info, err := dirEntry.Info()
				if err != nil {
					errorChan <- fmt.Errorf("failed to get file info for %s: %v", filePath, err)
					return
				}

				relPath, err := filepath.Rel(s.project.SourcePath, filePath)
				if err != nil {
					errorChan <- fmt.Errorf("failed to get relative path for %s: %v", filePath, err)
					return
				}

				// Check if file exists in database
				existingFile, err := s.db.GetFileByPath(s.project.Name, relPath)
				if err != nil {
					errorChan <- fmt.Errorf("failed to check file in database: %v", err)
					return
				}

				isNew := existingFile == nil
				isModified := false
				isRequeue := false
				uploadStatus := "pending"

				if !isNew {
					// Check if file needs to be requeued (exists but not successfully uploaded)
					if existingFile.UploadStatus != "uploaded" {
						isRequeue = true
					} else {
						// Check if file is modified by comparing timestamps and size
						isModified = info.ModTime().After(existingFile.Timestamp) || info.Size() != existingFile.Size
						if !isModified {
							uploadStatus = existingFile.UploadStatus
						}
					}
				}

				record := models.FileRecord{
					FilePath:     relPath,
					Size:         info.Size(),
					Timestamp:    info.ModTime(),
					UploadStatus: uploadStatus,
				}

				progress.Update(info.Size(), isNew, isModified, isRequeue)
				if progress.CurrentFiles%1000 == 0 {
					progress.Print()
				}

				if isNew || isModified || isRequeue {
					recordChan <- record
				}
			}(path, d)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("walk error: %v", err)
	}

	// Wait for all workers to complete
	wg.Wait()
	close(recordChan)

	// Wait for batch processor to complete
	select {
	case err := <-errorChan:
		return fmt.Errorf("scanning error: %v", err)
	case <-done:
		progress.Print()
		fmt.Printf("\nScan completed: %d total files (%s)\n",
			progress.CurrentFiles,
			utils.FormatSize(progress.TotalSize))
		fmt.Printf("New files: %d\n", progress.NewFiles)
		fmt.Printf("Modified files: %d\n", progress.ModifiedFiles)
		fmt.Printf("Requeued files: %d\n", progress.RequeueFiles)
		fmt.Printf("Unchanged files: %d\n", progress.UnchangedFiles)
		return nil
	}
}

// processBatches handles batch processing of file records
func (s *Scanner) processBatches(records <-chan models.FileRecord, errorChan chan<- error, done chan<- bool) {
	var batch []models.FileRecord
	ticker := time.NewTicker(time.Second * 5) // Flush every 5 seconds if batch is not full
	defer ticker.Stop()

	for {
		select {
		case record, ok := <-records:
			if !ok {
				// Channel closed, flush remaining records
				if len(batch) > 0 {
					if err := s.db.SaveFileRecordsBatch(s.project.Name, batch); err != nil {
						errorChan <- fmt.Errorf("failed to save final batch: %v", err)
						return
					}
				}
				done <- true
				return
			}

			batch = append(batch, record)
			if len(batch) >= s.batchSize {
				if err := s.db.SaveFileRecordsBatch(s.project.Name, batch); err != nil {
					errorChan <- fmt.Errorf("failed to save batch: %v", err)
					return
				}
				batch = batch[:0]
			}

		case <-ticker.C:
			// Periodic flush of partial batches
			if len(batch) > 0 {
				if err := s.db.SaveFileRecordsBatch(s.project.Name, batch); err != nil {
					errorChan <- fmt.Errorf("failed to save periodic batch: %v", err)
					return
				}
				batch = batch[:0]
			}
		}
	}
}

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

				_, err := s.minioClient.FPutObject(
					context.Background(),
					s.project.Destination.Bucket,
					job.destinationPath,
					fullPath,
					minio.PutObjectOptions{},
				)

				if err != nil {
					log.Printf("\nFailed to upload %s: %v\n", job.filePath, err)
					// Mark as failed in database
					if dbErr := s.db.UpdateFileStatus(s.project.Name, job.filePath, "failed"); dbErr != nil {
						log.Printf("Failed to update status for %s: %v\n", job.filePath, dbErr)
					}
					continue
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
		destinationPath := strings.ReplaceAll(s.project.Destination.Folder+file.FilePath, "\\", "/")
		jobs <- workItem{
			filePath:        file.FilePath,
			destinationPath: destinationPath,
			size:           file.Size,
			isRetry:        file.UploadStatus == "failed",
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
