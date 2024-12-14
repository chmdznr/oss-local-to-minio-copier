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

	"github.com/cheggaaa/pb/v3"
	"github.com/chmdznr/oss-local-to-minio-copier/internal/db"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/models"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/utils"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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
	mu            sync.Mutex
}

func newWorkerProgress(id int, totalFiles, totalSize int64, pool *pb.Pool) *workerProgress {
	bar := pb.New64(totalFiles)
	bar.Set(pb.Bytes, false)
	bar.SetTemplate(pb.ProgressBarTemplate(fmt.Sprintf(`Worker %d {{counters . }} {{bar . }} {{percent . }} {{speed . }}`, id)))
	pool.Add(bar)
	return &workerProgress{
		id:         id,
		totalFiles: totalFiles,
		totalSize:  totalSize,
		bar:        bar,
	}
}

func (wp *workerProgress) update() {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.processedFiles++
	wp.bar.SetCurrent(wp.processedFiles)
}

func (wp *workerProgress) start() {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.bar.Start()
}

func (wp *workerProgress) finish() {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.bar.Finish()
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
	lastFiles     int64
	lastSize      int64
	bar          *pb.ProgressBar
	sync.Mutex
}

func newSyncProgress(totalFiles int64, totalSize int64, pool *pb.Pool) *syncProgress {
	now := time.Now()
	bar := pb.New64(totalFiles)
	bar.Set(pb.Bytes, false)
	bar.SetTemplate(pb.ProgressBarTemplate(`Total Progress: {{counters . }} {{bar . }} {{percent . }} | Speed: {{speed . }} files/s (avg: {{string . "avgSpeed"}}) | Retried: {{string . "retried"}} - Skipped: {{string . "skipped"}} | Time: {{string . "elapsed"}}`))
	pool.Add(bar)
	return &syncProgress{
		TotalFiles: totalFiles,
		TotalSize:  totalSize,
		startTime:  now,
		lastUpdate: now,
		bar:        bar,
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
	p.updateBar()
}

func (p *syncProgress) Skip(size int64) {
	p.Lock()
	defer p.Unlock()
	p.SkippedFiles++
	p.SkippedSize += size
	p.updateBar()
}

func (p *syncProgress) updateBar() {
	avgSpeed, _ := p.getSpeed()
	p.bar.Set("avgSpeed", fmt.Sprintf("%.1f files/s", avgSpeed))
	p.bar.Set("retried", fmt.Sprintf("%d files", p.RetryFiles))
	p.bar.Set("skipped", fmt.Sprintf("%d files", p.SkippedFiles))
	p.bar.Set("elapsed", utils.FormatDuration(time.Since(p.startTime)))
	p.bar.SetCurrent(p.UploadedFiles)
}

func (p *syncProgress) Print() {
	// This is now handled by the progress bar
}

func (p *syncProgress) start() {
	p.bar.Start()
}

func (p *syncProgress) finish() {
	p.bar.Finish()
}

func (p *syncProgress) getSpeed() (avgSpeed float64, currentSpeed float64) {
	now := time.Now()
	totalDuration := now.Sub(p.startTime).Seconds()
	if totalDuration > 0 {
		avgSpeed = float64(p.UploadedFiles) / totalDuration
	}

	// Calculate current speed over the last update interval
	intervalDuration := now.Sub(p.lastUpdate).Seconds()
	filesDiff := float64(p.UploadedFiles - p.lastFiles)
	if intervalDuration > 0 {
		currentSpeed = filesDiff / intervalDuration
	}

	// Update last values for next calculation
	p.lastUpdate = now
	p.lastFiles = p.UploadedFiles

	return avgSpeed, currentSpeed
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

func (s *Syncer) processWorkerFiles(id int, files []models.FileRecord, startIdx, endIdx int, progress *syncProgress, workerProgress *workerProgress) {
	var completedFiles []string

	// Start the worker progress bar
	workerProgress.start()

	// Process only files assigned to this worker
	for j := startIdx; j < endIdx && j < len(files); j++ {
		file := files[j]
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

		fullPath := filepath.Join(s.project.SourcePath, file.FilePath)

		// Check if file still exists
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			log.Printf("\nSkipping %s: file no longer exists\n", file.FilePath)
			progress.Skip(file.Size)
			progress.updateBar()

			// Mark as skipped in database
			if err := s.db.UpdateFileStatus(s.project.Name, file.FilePath, "skipped"); err != nil {
				log.Printf("\nFailed to update status for %s: %v\n", file.FilePath, err)
			}
			continue
		}

		// Set metadata if available
		var opts minio.PutObjectOptions
		if metadata != nil {
			// Sanitize metadata values and handle special cases
			sanitizedMetadata := make(map[string]string)
			for k, v := range metadata {
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
			destinationPath,
			fullPath,
			opts,
		)

		if err != nil {
			// Log detailed error information
			log.Printf("\nFailed to upload %s (attempt %d):\n", file.FilePath, 1)
			log.Printf("  Local path: %s\n", fullPath)
			log.Printf("  Destination: %s/%s\n", s.project.Destination.Bucket, destinationPath)
			log.Printf("  Error: %v\n", err)

			// Mark as failed in database
			if err := s.db.UpdateFileStatus(s.project.Name, file.FilePath, "failed"); err != nil {
				log.Printf("\nFailed to update status for %s: %v\n", file.FilePath, err)
			}
			continue
		}

		// Update progress
		progress.Update(objInfo.Size, false)
		workerProgress.update()

		// Add to completed files
		completedFiles = append(completedFiles, file.FilePath)

		// Update database in batches
		if len(completedFiles) >= s.batchSize {
			if err := s.db.UpdateFilesStatus(s.project.Name, completedFiles, "uploaded"); err != nil {
				log.Printf("\nFailed to update status for batch: %v\n", err)
			}
			completedFiles = nil // Reset the slice
		}
	}

	// Update any remaining files in the last batch
	if len(completedFiles) > 0 {
		if err := s.db.UpdateFilesStatus(s.project.Name, completedFiles, "uploaded"); err != nil {
			log.Printf("\nFailed to update status for final batch: %v\n", err)
		}
	}

	// Finish the worker progress bar
	workerProgress.finish()
}

func (s *Syncer) SyncFiles() error {
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

	// Create a pool for all progress bars
	pool := pb.NewPool()

	// Create global progress first
	progress := newSyncProgress(int64(len(files)), totalSize, pool)

	fmt.Printf("Starting sync of %d files (%s) - %d files being retried...\n",
		progress.TotalFiles,
		utils.FormatSize(progress.TotalSize),
		retriesCount)

	// Calculate files per worker - ensure even distribution
	filesPerWorker := (len(files) + s.numWorkers - 1) / s.numWorkers // Round up division
	
	// Create worker progress bars
	workerProgresses := make([]*workerProgress, s.numWorkers)
	for i := 0; i < s.numWorkers; i++ {
		startIdx := i * filesPerWorker
		endIdx := startIdx + filesPerWorker
		if endIdx > len(files) {
			endIdx = len(files)
		}
		workerFiles := int64(endIdx - startIdx)
		var workerSize int64
		for j := startIdx; j < endIdx && j < len(files); j++ {
			workerSize += files[j].Size
		}
		workerProgresses[i] = newWorkerProgress(i, workerFiles, workerSize, pool)
	}

	// Start the pool
	pool.Start()

	// Start global progress bar
	progress.start()
	fmt.Println() // Add a newline before worker progress bars

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < s.numWorkers; i++ {
		wg.Add(1)
		startIdx := i * filesPerWorker
		endIdx := startIdx + filesPerWorker
		if endIdx > len(files) {
			endIdx = len(files)
		}
		if startIdx >= len(files) {
			wg.Done()
			continue
		}
		go func(id int, startIdx, endIdx int) {
			defer wg.Done()
			s.processWorkerFiles(id, files, startIdx, endIdx, progress, workerProgresses[id])
		}(i, startIdx, endIdx)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Stop global progress bar
	progress.finish()

	// Calculate final statistics
	avgSpeed, _ := progress.getSpeed()
	fmt.Printf("\nSync completed in %s:\n", utils.FormatDuration(time.Since(progress.startTime)))
	fmt.Printf("- Uploaded: %d files at %.1f files/s average\n",
		progress.UploadedFiles,
		avgSpeed)
	fmt.Printf("- Retried: %d files\n",
		progress.RetryFiles)
	fmt.Printf("- Skipped: %d files\n",
		progress.SkippedFiles)

	return nil
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

		// Then encode
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

func NewSyncer(db *db.DB, project *models.Project, config *SyncerConfig) (*Syncer, error) {
	if config == nil {
		defaultConfig := DefaultSyncerConfig()
		config = &defaultConfig
	}

	// Create a transport with optimized settings for parallel uploads
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100, // Match MaxIdleConns to allow full parallel usage
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true, // Disable compression since we're dealing with already compressed files
		MaxConnsPerHost:       100,  // Allow more concurrent connections per host
		ForceAttemptHTTP2:    true, // Enable HTTP/2 for better performance
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

	// Set client options for better performance
	minioClient.SetAppInfo("msync", "1.0.0")

	return &Syncer{
		db:          db,
		project:     project,
		minioClient: minioClient,
		numWorkers:  config.NumWorkers,
		batchSize:   config.BatchSize,
	}, nil
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
