package sync

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"encoding/json"

	"github.com/chmdznr/oss-local-to-minio-copier/internal/db"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/models"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/utils"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/cheggaaa/pb/v3"
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

// workerProgress tracks progress for a single worker
type workerProgress struct {
	id             int
	totalFiles     int64
	totalSize      int64
	processedFiles int64
	processedSize  int64
	bar           *pb.ProgressBar
}

func newWorkerProgress(id int, totalFiles, totalSize int64) *workerProgress {
	bar := pb.New64(totalFiles)
	bar.Set(pb.Bytes, true)
	bar.SetTemplate(`Worker {{string . "id"}} {{counters . }} {{bar . }} {{percent . }} {{speed . }}`)
	bar.Set("id", fmt.Sprintf("%d", id))
	return &workerProgress{
		id:         id,
		totalFiles: totalFiles,
		totalSize:  totalSize,
		bar:        bar,
	}
}

func (wp *workerProgress) update(size int64) {
	wp.processedFiles++
	wp.processedSize += size
	wp.bar.Increment()
}

func (wp *workerProgress) start() {
	wp.bar.Start()
}

func (wp *workerProgress) finish() {
	wp.bar.Finish()
}

// NewSyncer creates a new syncer instance
func NewSyncer(db *db.DB, project *models.Project, config *SyncerConfig) (*Syncer, error) {
	if config == nil {
		defaultConfig := DefaultSyncerConfig()
		config = &defaultConfig
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// Create MinIO client with custom transport and proper region
	opts := minio.Options{
		Creds:        credentials.NewStaticV4(project.Destination.AccessKey, project.Destination.SecretKey, ""),
		Secure:       true,
		Transport:    tr,
		Region:       "auto", // Let MinIO detect the region
		BucketLookup: minio.BucketLookupAuto,
	}

	minioClient, err := minio.New(project.Destination.Endpoint, &opts)
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

	fmt.Printf("\rProgress: %d/%d files (%.1f%%) - %s/%s | Speed: %s (avg: %s) | Retried: %d (%s) - Skipped: %d (%s) | Time Elapsed: %s",
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
		utils.FormatDuration(time.Since(p.startTime)), // Add elapsed time
	)
}

func (s *Syncer) uploadFile(file models.FileRecord) error {
	// Use the metadata map directly since it's already parsed in GetPendingFiles
	userMetadata := file.Metadata
	if userMetadata == nil {
		userMetadata = make(map[string]string)
	}

	// Ensure required metadata fields exist
	requiredFields := []string{"id_file", "id_from_csv", "id_permohonan"}
	for _, field := range requiredFields {
		if _, exists := userMetadata[field]; !exists {
			// Add from FileRecord struct if available
			switch field {
			case "id_file":
				if file.IDFile != "" {
					userMetadata[field] = file.IDFile
				}
			case "id_from_csv":
				if file.IDFromCSV != "" {
					userMetadata[field] = file.IDFromCSV
				}
			case "id_permohonan":
				if file.IDPermohonan != "" {
					userMetadata[field] = file.IDPermohonan
				}
			}
		}
	}

	// Open the file
	localFile, err := os.Open(file.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", file.FilePath, err)
	}
	defer localFile.Close()

	// Upload the file with metadata
	_, err = s.minioClient.PutObject(
		context.Background(),
		s.project.Destination.Bucket,
		file.IDFile,
		localFile,
		file.Size,
		minio.PutObjectOptions{
			UserMetadata: userMetadata,
		},
	)

	if err != nil {
		return fmt.Errorf("failed to upload file %s: %v", file.FilePath, err)
	}

	return nil
}

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
	filesPerWorker := len(files) / s.numWorkers
	if filesPerWorker == 0 {
		filesPerWorker = 1
	}
	workerProgresses := make([]*workerProgress, s.numWorkers)
	for i := 0; i < s.numWorkers; i++ {
		wg.Add(1)
		startIdx := i * filesPerWorker
		endIdx := (i + 1) * filesPerWorker
		if i == s.numWorkers-1 {
			endIdx = len(files)
		}
		workerFiles := int64(endIdx - startIdx)
		var workerSize int64
		for j := startIdx; j < endIdx && j < len(files); j++ {
			workerSize += files[j].Size
		}
		workerProgresses[i] = newWorkerProgress(i, workerFiles, workerSize)
		workerProgresses[i].start()
		go func(id int) {
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
					// Sanitize metadata values and handle special cases
					sanitizedMetadata := make(map[string]string)
					for k, v := range job.metadata {
						sanitized := v
						if k == "path" {
							// Special handling for path metadata
							sanitized = sanitizePath(v)
						} else if k == "bucket" {
							// Special handling for bucket metadata - don't URL encode
							sanitized = v
						} else {
							// For other metadata values, first decode if already encoded
							decoded, err := url.QueryUnescape(v)
							if err == nil {
								sanitized = decoded
							}
							// Replace problematic characters
							sanitized = strings.ReplaceAll(sanitized, "&", "and")
							sanitized = strings.ReplaceAll(sanitized, "+", "plus")
							// Then encode
							sanitized = url.QueryEscape(sanitized)
						}
						sanitized = strings.TrimSpace(sanitized)
						if sanitized != "" {
							sanitizedMetadata[k] = sanitized
						}
					}
					opts.UserMetadata = sanitizedMetadata
				}

				// Ensure content type is set
				if strings.HasSuffix(strings.ToLower(fullPath), ".pdf") {
					opts.ContentType = "application/pdf"
				} else if strings.HasSuffix(strings.ToLower(fullPath), ".docx") {
					opts.ContentType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
				} else if strings.HasSuffix(strings.ToLower(fullPath), ".zip") {
					opts.ContentType = "application/zip"
				}

				// Upload file with retries and verification
				objInfo, err := s.minioClient.FPutObject(
					context.Background(),
					s.project.Destination.Bucket,
					job.destinationPath,
					fullPath,
					opts,
				)

				if err != nil {
					// Log detailed error information
					log.Printf("\nFailed to upload %s (attempt %d):\n", job.filePath, 1)
					log.Printf("  Local path: %s\n", fullPath)
					log.Printf("  Destination: %s/%s\n", s.project.Destination.Bucket, job.destinationPath)
					log.Printf("  Error: %v\n", err)

					// Check if it's a MinIO error
					if minioErr, ok := err.(minio.ErrorResponse); ok {
						log.Printf("  MinIO Error Details:\n")
						log.Printf("    Code: %s\n", minioErr.Code)
						log.Printf("    Message: %s\n", minioErr.Message)
						log.Printf("    Key: %s\n", minioErr.Key)
						log.Printf("    BucketName: %s\n", minioErr.BucketName)
					}

					// Mark as failed in database
					if dbErr := s.db.UpdateFileStatus(s.project.Name, job.filePath, "failed"); dbErr != nil {
						log.Printf("Failed to update status for %s: %v\n", job.filePath, dbErr)
					}
					continue
				}

				// Verify upload was successful by checking object info
				if objInfo.Size != job.size {
					log.Printf("\nWarning: Uploaded file size mismatch for %s:\n", job.filePath)
					log.Printf("  Expected: %d bytes\n", job.size)
					log.Printf("  Actual: %d bytes\n", objInfo.Size)
					if dbErr := s.db.UpdateFileStatus(s.project.Name, job.filePath, "failed"); dbErr != nil {
						log.Printf("Failed to update status for %s: %v\n", job.filePath, dbErr)
					}
					continue
				}

				// After successful upload and verification, update the metadata in database
				metadataJSON, err := json.Marshal(job.metadata)
				if err != nil {
					log.Printf("\nWarning: Failed to marshal metadata for %s: %v\n", job.filePath, err)
				} else {
					if err := s.db.UpdateFileMetadata(s.project.Name, job.filePath, string(metadataJSON)); err != nil {
						log.Printf("\nWarning: Failed to update metadata for %s: %v\n", job.filePath, err)
					}
				}

				// Update status immediately for this file
				if err := s.db.UpdateFileStatus(s.project.Name, job.filePath, "uploaded"); err != nil {
					log.Printf("\nWarning: Failed to update status for %s: %v\n", job.filePath, err)
				}

				completedFiles = append(completedFiles, job.filePath)
				progress.Update(job.size, job.isRetry)
				workerProgresses[id].update(job.size)
				progress.Print()

				// Update progress in batches
				if len(completedFiles) >= s.batchSize {
					completedFiles = nil
				}
			}
		}(i)
	}

	// Send jobs to workers
	for _, file := range files {
		// Ensure folder has trailing slash and construct path
		folder := strings.TrimRight(s.project.Destination.Folder, "/") + "/"
		destinationPath := fmt.Sprintf("%s%s", folder, file.IDFile)

		// Create complete metadata
		metadata := file.Metadata
		if metadata == nil {
			metadata = make(map[string]string)
		}

		// Preserve existing metadata values and set bucket without URL encoding
		metadata["path"] = sanitizePath(file.FilePath)
		metadata["bucket"] = fmt.Sprintf("%s/%s", s.project.Destination.Bucket, strings.TrimPrefix(destinationPath, "/"))
		metadata["existing_id"] = file.IDFromCSV

		// Sanitize destination path - replace problematic characters
		destinationPath = strings.Map(func(r rune) rune {
			switch r {
			case '　': // full-width space
				return ' ' // replace with regular space
			case '\u200B', '\uFEFF': // zero-width space and BOM
				return -1 // remove these characters
			default:
				return r
			}
		}, destinationPath)

		// Clean the path to ensure proper formatting
		destinationPath = strings.ReplaceAll(destinationPath, "\\", "/")
		destinationPath = strings.TrimPrefix(destinationPath, "/")

		jobs <- workItem{
			filePath:        file.FilePath,
			destinationPath: destinationPath,
			size:            file.Size,
			isRetry:         file.UploadStatus == "failed",
			metadata:        metadata,
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
		for _, wp := range workerProgresses {
			wp.finish()
		}
		return nil
	}
}

func sanitizePath(path string) string {
	// First, replace any backslashes with forward slashes
	path = strings.ReplaceAll(path, "\\", "/")

	// Split the path into segments
	segments := strings.Split(path, "/")

	// URL encode each segment individually
	for i, segment := range segments {
		// First decode the segment in case it's already encoded
		decoded, err := url.QueryUnescape(segment)
		if err == nil {
			segment = decoded
		}

		// Replace problematic characters
		segment = strings.ReplaceAll(segment, "&", "and")
		segment = strings.ReplaceAll(segment, "+", "plus")

		// Encode the segment
		segments[i] = url.QueryEscape(segment)
	}

	// Join the segments back together
	sanitized := strings.Join(segments, "/")

	// Remove any double slashes
	for strings.Contains(sanitized, "//") {
		sanitized = strings.ReplaceAll(sanitized, "//", "/")
	}

	return sanitized
}
