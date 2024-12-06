package sync

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

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

	// Enable request/response debugging if needed
	minioClient.TraceOn(os.Stdout)

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

func (s *Syncer) uploadFile(ctx context.Context, file *models.File) error {
	// Sanitize the destination path
	destPath := sanitizePath(file.DestPath)
	
	// Open the file
	fileReader, err := os.Open(file.LocalPath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", file.LocalPath, err)
	}
	defer fileReader.Close()

	// Get file info for content type detection
	fileInfo, err := fileReader.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info for %s: %v", file.LocalPath, err)
	}

	// Detect content type
	contentType := mime.TypeByExtension(filepath.Ext(file.LocalPath))
	if contentType == "" {
		// If no extension match, try to detect from content
		buffer := make([]byte, 512)
		_, err = fileReader.Read(buffer)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read file content for type detection: %v", err)
		}
		contentType = http.DetectContentType(buffer)
		// Reset the reader to the beginning
		_, err = fileReader.Seek(0, 0)
		if err != nil {
			return fmt.Errorf("failed to reset file reader: %v", err)
		}
	}

	// Sanitize metadata - encode both keys and values
	sanitizedMetadata := make(map[string]string)
	for k, v := range file.Metadata {
		// URL encode both keys and values to handle special characters
		encodedKey := url.QueryEscape(strings.TrimSpace(k))
		encodedValue := url.QueryEscape(strings.TrimSpace(v))
		if encodedValue != "" {
			// For x-amz-meta-path, we need to encode each path segment
			if strings.ToLower(k) == "path" {
				pathSegments := strings.Split(v, "/")
				for i, segment := range pathSegments {
					pathSegments[i] = url.PathEscape(segment)
				}
				encodedValue = strings.Join(pathSegments, "/")
			}
			sanitizedMetadata[encodedKey] = encodedValue
		}
	}

	// Add content disposition to force download with original filename
	// Use RFC 5987 encoding for the filename
	originalFilename := filepath.Base(file.LocalPath)
	contentDisposition := fmt.Sprintf("attachment; filename=\"%s\"; filename*=UTF-8''%s", 
		strings.ReplaceAll(originalFilename, "\"", "\\\""),
		url.PathEscape(originalFilename))

	// Prepare upload options with proper headers
	opts := minio.PutObjectOptions{
		ContentType:        contentType,
		ContentDisposition: contentDisposition,
		UserMetadata:      sanitizedMetadata,
	}

	// Attempt the upload with retry logic
	maxRetries := 3
	var uploadErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		info, uploadErr := s.minioClient.PutObject(
			ctx,
			s.project.Destination.Bucket,
			destPath,
			fileReader,
			fileInfo.Size(),
			opts,
		)

		if uploadErr == nil {
			// Update file status on successful upload
			if info.Size > 0 {
				return nil
			}
		}

		// Log detailed error information
		log.Printf("Upload attempt %d failed for file %s: %v", attempt, file.LocalPath, uploadErr)

		// Check if error is retriable
		if minioErr, ok := uploadErr.(minio.ErrorResponse); ok {
			log.Printf("MinIO Error Details - Code: %s, Message: %s, RequestID: %s", 
				minioErr.Code, minioErr.Message, minioErr.RequestID)
			
			// Handle specific error cases
			switch minioErr.Code {
			case "AccessDenied":
				return fmt.Errorf("access denied to bucket %s: %v", s.project.Destination.Bucket, uploadErr)
			case "NoSuchBucket":
				return fmt.Errorf("bucket %s does not exist: %v", s.project.Destination.Bucket, uploadErr)
			case "InvalidAccessKeyId":
				return fmt.Errorf("invalid access key: %v", uploadErr)
			case "SignatureDoesNotMatch":
				// Log additional details for signature mismatch
				log.Printf("Signature mismatch details - Region: %s, Endpoint: %s", 
					minioErr.Region, s.project.Destination.Endpoint)
			}
		}

		// Reset file pointer for retry
		if attempt < maxRetries {
			_, err = fileReader.Seek(0, 0)
			if err != nil {
				return fmt.Errorf("failed to reset file for retry: %v", err)
			}
			// Wait before retrying with exponential backoff
			time.Sleep(time.Duration(attempt*attempt) * time.Second)
		}
	}

	if uploadErr != nil {
		return fmt.Errorf("failed to upload file %s after %d attempts: %v", file.LocalPath, maxRetries, uploadErr)
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
					// Sanitize metadata values
					sanitizedMetadata := make(map[string]string)
					for k, v := range job.metadata {
						// Remove any non-printable characters and normalize spaces
						sanitized := strings.Map(func(r rune) rune {
							if r < 32 || r == 127 { // control characters
								return -1
							}
							if r == '　' { // full-width space
								return ' '
							}
							return r
						}, v)
						
						// Trim spaces and ensure non-empty value
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

func sanitizePath(path string) string {
	// First clean the path by removing non-printable characters
	sanitized := strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) {
			return r
		}
		return -1
	}, path)
	sanitized = strings.TrimSpace(sanitized)

	// Split path into segments and encode each segment
	segments := strings.Split(sanitized, "/")
	for i, segment := range segments {
		// URL encode each segment, preserving forward slashes
		segments[i] = url.PathEscape(segment)
	}

	// Rejoin the path with forward slashes
	return strings.Join(segments, "/")
}
