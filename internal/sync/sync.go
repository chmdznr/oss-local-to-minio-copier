package sync

import (
	"context"
	"fmt"
	"io/fs"
	"local-to-minio-copier/internal/db"
	"local-to-minio-copier/pkg/models"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
	TotalFiles   int64
	TotalSize    int64
	CurrentFiles int64
	mu           sync.Mutex
}

func (p *scanProgress) Update(size int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.CurrentFiles++
	p.TotalSize += size
}

func (p *scanProgress) Print() {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Printf("\rScanned %d files (%.2f GB)...", p.CurrentFiles, float64(p.TotalSize)/1024/1024/1024)
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

				record := models.FileRecord{
					FilePath:     relPath,
					Size:         info.Size(),
					Timestamp:    info.ModTime(),
					UploadStatus: "pending",
				}

				progress.Update(info.Size())
				if progress.CurrentFiles%1000 == 0 {
					progress.Print()
				}

				recordChan <- record
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
		fmt.Printf("\nScan completed: %d files (%.2f GB)\n",
			progress.CurrentFiles,
			float64(progress.TotalSize)/1024/1024/1024)
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

// SyncFiles synchronizes pending files using a worker pool
func (s *Syncer) SyncFiles() error {
	// Create worker pool
	type workItem struct {
		filePath        string
		destinationPath string
	}

	jobs := make(chan workItem, s.numWorkers)
	results := make(chan []string, s.numWorkers)
	errors := make(chan error, 1)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < s.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var completedFiles []string

			for job := range jobs {
				fullPath := filepath.Join(s.project.SourcePath, job.filePath)

				_, err := s.minioClient.FPutObject(
					context.Background(),
					s.project.Destination.Bucket,
					job.destinationPath,
					fullPath,
					minio.PutObjectOptions{},
				)

				if err != nil {
					fmt.Printf("Failed to upload %s: %v\n", job.filePath, err)
					continue
				}

				completedFiles = append(completedFiles, job.filePath)

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

	// Get pending files and distribute work
	files, err := s.db.GetPendingFiles(s.project.Name)
	if err != nil {
		return err
	}

	// Send jobs to workers
	for _, file := range files {
		destinationPath := strings.ReplaceAll(s.project.Destination.Folder+file.FilePath, "\\", "/")
		jobs <- workItem{
			filePath:        file.FilePath,
			destinationPath: destinationPath,
		}
	}

	close(jobs)
	wg.Wait()
	close(results)

	select {
	case err := <-errors:
		return err
	default:
		return nil
	}
}
