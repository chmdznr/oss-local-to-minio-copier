package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cheggaaa/pb/v3"
	"github.com/chmdznr/oss-local-to-minio-copier/internal/db"
	syncer "github.com/chmdznr/oss-local-to-minio-copier/internal/sync"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/models"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/utils"
	"github.com/chmdznr/oss-local-to-minio-copier/pkg/version"
	"github.com/urfave/cli/v2"
	"github.com/xuri/excelize/v2"
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
				Action: syncFiles,
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
				Usage: "Import files from CSV or Excel",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "project",
						Usage:    "Project name",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "csv",
						Usage:    "CSV or Excel file path",
						Required: true,
					},
					&cli.IntFlag{
						Name:  "batch",
						Usage: "Batch size for processing records (default: 1000)",
						Value: 1000,
					},
					&cli.IntFlag{
						Name:  "workers",
						Usage: "Number of concurrent workers (default: 8)",
						Value: 8,
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

func syncFiles(c *cli.Context) error {
	projectName := c.String("project")
	if projectName == "" {
		return fmt.Errorf("project name is required")
	}

	// Open database connection
	db, err := db.New(projectName)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	// Get project details
	project, err := db.GetProject(projectName)
	if err != nil {
		return fmt.Errorf("failed to get project details: %v", err)
	}

	// Create syncer with config from command line arguments
	config := syncer.SyncerConfig{
		NumWorkers: c.Int("workers"),
		BatchSize:  c.Int("batch"),
	}
	s, err := syncer.NewSyncer(db, project, &config)
	if err != nil {
		return fmt.Errorf("failed to create syncer: %v", err)
	}

	// Start sync process
	return s.SyncFiles()
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
	fmt.Printf("Files Failed: %d (Size: %s)\n", stats.FailedFiles, utils.FormatSize(stats.FailedSize))
	fmt.Printf("Files Missing: %d\n", stats.MissingFiles)

	var fileProgress, sizeProgress float64
	if stats.TotalFiles > 0 {
		fileProgress = float64(stats.UploadedFiles) / float64(stats.TotalFiles) * 100
	}
	if stats.TotalSize > 0 {
		sizeProgress = float64(stats.UploadedSize) / float64(stats.TotalSize) * 100
	}
	fmt.Printf("Progress: %.2f%% (Files), %.2f%% (Size)\n", fileProgress, sizeProgress)

	return nil
}

// importCSV imports file records from a CSV or Excel file to the database
//
// The input file must contain the following columns:
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
// The file may contain additional columns, which will be ignored.
//
// Supported file formats:
// - CSV (.csv)
// - Excel (.xlsx)
//
// The function processes the records in batches of batchSize.
// When the batch size is reached, the batch is saved to the database.
// The function prints a message every time a batch is saved, indicating
// the number of records processed so far.
//
// The function returns an error if the project does not exist, if the
// input file is malformed, or if there is an error saving the records to
// the database.
func importCSV(c *cli.Context) error {
	projectName := c.String("project")
	inputPath := c.String("csv")
	batchSize := c.Int("batch")
	numWorkers := c.Int("workers")

	if projectName == "" {
		return fmt.Errorf("project name is required")
	}
	if inputPath == "" {
		return fmt.Errorf("input file path (CSV or Excel) is required")
	}
	if batchSize <= 0 {
		batchSize = 1000 // default batch size
	}
	if numWorkers <= 0 {
		numWorkers = 8 // default number of workers
	}

	// Open database connection
	db, err := db.New(projectName)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	// Get project details to get source path
	project, err := db.GetProject(projectName)
	if err != nil {
		return fmt.Errorf("failed to get project details: %v", err)
	}

	// Detect file type by extension
	ext := strings.ToLower(filepath.Ext(inputPath))
	var reader rowReader
	var totalRows int

	switch ext {
	case ".csv":
		reader, totalRows, err = newCSVReader(inputPath)
	case ".xlsx":
		reader, totalRows, err = newExcelReader(inputPath)
	default:
		return fmt.Errorf("unsupported file format: %s (supported formats: .csv, .xlsx)", ext)
	}
	if err != nil {
		return fmt.Errorf("failed to create file reader: %v", err)
	}
	defer reader.Close()

	// Get existing files to track updates
	existingFiles, err := db.GetProjectFiles(projectName)
	if err != nil {
		return fmt.Errorf("failed to get existing files: %v", err)
	}
	existingFilesMap := make(map[string]bool)
	for _, f := range existingFiles {
		existingFilesMap[f.FilePath] = true
	}

	type workResult struct {
		records     []models.CSVRecord
		totalSize   int64
		newCount    int
		updateCount int
		skipCount   int
		err         error
	}

	results := make(chan workResult, numWorkers)
	dbMutex := &sync.Mutex{} // Mutex for database operations

	// Create progress bars for each worker
	bars := make([]*pb.ProgressBar, numWorkers)
	workerRowCounts := make([]int, numWorkers)
	
	// Calculate exact number of rows for each worker
	remainingRows := totalRows
	for i := range bars {
		// Calculate rows for this worker
		rowCount := remainingRows / (numWorkers - i)
		workerRowCounts[i] = rowCount
		remainingRows -= rowCount

		// Create progress bar
		bars[i] = pb.New(rowCount)
		bars[i].Set("prefix", fmt.Sprintf("Worker %d ", i))
		bars[i].SetMaxWidth(100)
	}

	fmt.Printf("Starting import with %d workers, processing %d rows...\n", numWorkers, totalRows)
	for i, count := range workerRowCounts {
		fmt.Printf("Worker %d will process %d rows\n", i, count)
	}

	pool, err := pb.StartPool(bars...)
	if err != nil {
		return fmt.Errorf("error creating progress pool: %v", err)
	}
	defer pool.Stop()

	// Create channels for each worker
	workerChannels := make([]chan csvRow, numWorkers)
	for i := range workerChannels {
		workerChannels[i] = make(chan csvRow, numWorkers)
	}

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			var records []models.CSVRecord
			result := workResult{}
			rowCount := 0

			// Get progress bar for this worker
			bar := bars[workerID]

			// Process rows sent to this worker
			for row := range workerChannels[workerID] {
				rowCount++
				getColumnValue := func(name string) string {
					idx := reader.HeaderIndex(name)
					if idx < 0 || idx >= len(row.data) {
						log.Printf("Warning: Column '%s' index out of bounds at line %d", name, row.lineNum)
						return ""
					}
					return row.data[idx]
				}

				filePath := getColumnValue("path")
				if filePath == "" {
					result.skipCount++
					log.Printf("Warning: Empty or invalid path at line %d", row.lineNum)
					continue
				}

				// Check if file exists
				fileInfo, err := os.Stat(filepath.Join(project.SourcePath, filePath))
				if err != nil {
					if os.IsNotExist(err) {
						result.skipCount++
						dbMutex.Lock()
						if err := db.AddMissingFile(filePath, getColumnValue("id_upload"), row.lineNum); err != nil {
							log.Printf("Warning: Failed to record missing file %s: %v", filePath, err)
						}
						dbMutex.Unlock()
						continue
					}
					results <- workResult{err: fmt.Errorf("worker %d failed to stat file: %v", workerID, err)}
					return
				}

				record := models.CSVRecord{
					IDUpload:     getColumnValue("id_upload"),
					Path:         filePath,
					NamaModul:    getColumnValue("nama_modul"),
					FileType:     getColumnValue("file_type"),
					NamaFileAsli: getColumnValue("nama_file_asli"),
					IDProfile:    getColumnValue("id_profile"),
					ID:           getColumnValue("id"),
					StrKey:       getColumnValue("str_key"),
					StrSubKey:    getColumnValue("str_subkey"),
					Size:         fileInfo.Size(),
					ModTime:      fileInfo.ModTime().Unix(),
				}

				records = append(records, record)
				result.totalSize += fileInfo.Size()

				if existingFilesMap[filePath] {
					result.updateCount++
				} else {
					result.newCount++
				}

				bar.Increment()

				if len(records) >= batchSize {
					dbMutex.Lock()
					if err := db.SaveFileRecordsFromCSVBatch(projectName, records); err != nil {
						dbMutex.Unlock()
						results <- workResult{err: fmt.Errorf("worker %d failed to save batch: %v", workerID, err)}
						return
					}
					dbMutex.Unlock()
					records = records[:0]
				}
			}

			// Save any remaining records
			if len(records) > 0 {
				result.records = records
			}

			results <- result
		}(i)
	}

	// Distribute rows to workers
	go func() {
		currentWorker := 0
		rowsDistributed := make([]int, numWorkers)
		lineNum := 1 // Start from 1 since we already read header

		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading row: %v", err)
				continue
			}
			lineNum++ // Increment for each row read

			// Find next available worker
			workerFound := false
			for attempt := 0; attempt < numWorkers; attempt++ {
				if rowsDistributed[currentWorker] < workerRowCounts[currentWorker] {
					workerFound = true
					break
				}
				currentWorker = (currentWorker + 1) % numWorkers
			}

			if !workerFound {
				log.Printf("Warning: No workers available for row %d, skipping", lineNum)
				continue
			}

			workerChannels[currentWorker] <- csvRow{data: row, lineNum: lineNum}
			rowsDistributed[currentWorker]++
			currentWorker = (currentWorker + 1) % numWorkers
		}

		// Close all worker channels
		for _, ch := range workerChannels {
			close(ch)
		}
	}()

	// Wait for all workers and collect results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results
	var totalNewFiles, totalUpdatedFiles, totalSkippedFiles int
	var totalFileSize int64
	var firstError error
	var remainingRecords []models.CSVRecord

	for result := range results {
		if result.err != nil {
			if firstError == nil {
				firstError = result.err
			}
			continue
		}
		totalNewFiles += result.newCount
		totalUpdatedFiles += result.updateCount
		totalSkippedFiles += result.skipCount
		totalFileSize += result.totalSize
		if len(result.records) > 0 {
			remainingRecords = append(remainingRecords, result.records...)
		}
	}

	// Save any remaining records from all workers
	if len(remainingRecords) > 0 && firstError == nil {
		if err := db.SaveFileRecordsFromCSVBatch(projectName, remainingRecords); err != nil {
			if firstError == nil {
				firstError = fmt.Errorf("failed to save final batch: %v", err)
			}
		}
	}

	if firstError != nil {
		return fmt.Errorf("import failed: %v", firstError)
	}

	fmt.Printf("\nImport completed:\n")
	fmt.Printf("- New files imported: %d\n", totalNewFiles)
	fmt.Printf("- Existing files updated: %d\n", totalUpdatedFiles)
	fmt.Printf("- Total size: %s\n", utils.FormatSize(totalFileSize))
	fmt.Printf("- Skipped: %d files\n", totalSkippedFiles)
	return nil
}

// rowReader is an interface for reading rows from different file formats
type rowReader interface {
	Read() ([]string, error)
	HeaderIndex(name string) int
	Close() error
}

// csvRow represents a row from any input file format
type csvRow struct {
	data     []string
	lineNum  int // 1-based line number in original file
}

// csvRowReader implements rowReader for CSV files
type csvRowReader struct {
	reader     *csv.Reader
	file       *os.File
	headerMap  map[string]int
}

func newCSVReader(path string) (rowReader, int, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open CSV file: %v", err)
	}

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	// Read header
	header, err := reader.Read()
	if err != nil {
		file.Close()
		return nil, 0, fmt.Errorf("failed to read CSV header: %v", err)
	}

	// Map header indices
	headerMap := make(map[string]int)
	for i, h := range header {
		headerMap[strings.TrimSpace(strings.ToLower(h))] = i
	}

	// Verify required fields
	if err := verifyRequiredFields(headerMap); err != nil {
		file.Close()
		return nil, 0, err
	}

	// Count rows
	totalRows := 0
	countFile, err := os.Open(path)
	if err != nil {
		file.Close()
		return nil, 0, fmt.Errorf("failed to open CSV file for counting: %v", err)
	}
	countReader := csv.NewReader(countFile)
	for {
		_, err := countReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			countFile.Close()
			file.Close()
			return nil, 0, fmt.Errorf("failed to count CSV rows: %v", err)
		}
		totalRows++
	}
	countFile.Close()
	totalRows-- // Subtract header row

	return &csvRowReader{
		reader:    reader,
		file:      file,
		headerMap: headerMap,
	}, totalRows, nil
}

func (r *csvRowReader) Read() ([]string, error) {
	return r.reader.Read()
}

func (r *csvRowReader) HeaderIndex(name string) int {
	return r.headerMap[strings.ToLower(name)]
}

func (r *csvRowReader) Close() error {
	return r.file.Close()
}

// excelRowReader implements rowReader for Excel files
type excelRowReader struct {
	file      *excelize.File
	rows      *excelize.Rows
	headerMap map[string]int
}

func newExcelReader(path string) (rowReader, int, error) {
	file, err := excelize.OpenFile(path)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open Excel file: %v", err)
	}

	// Get the first sheet
	sheets := file.GetSheetList()
	if len(sheets) == 0 {
		file.Close()
		return nil, 0, fmt.Errorf("Excel file contains no sheets")
	}

	// Get rows from the first sheet
	rows, err := file.Rows(sheets[0])
	if err != nil {
		file.Close()
		return nil, 0, fmt.Errorf("failed to read Excel rows: %v", err)
	}

	// Read header row
	if !rows.Next() {
		file.Close()
		return nil, 0, fmt.Errorf("Excel file is empty")
	}

	header, err := rows.Columns()
	if err != nil {
		file.Close()
		return nil, 0, fmt.Errorf("failed to read Excel header: %v", err)
	}

	// Map header indices
	headerMap := make(map[string]int)
	for i, h := range header {
		headerMap[strings.TrimSpace(strings.ToLower(h))] = i
	}

	// Verify required fields
	if err := verifyRequiredFields(headerMap); err != nil {
		file.Close()
		return nil, 0, err
	}

	// Count total rows
	totalRows := 0
	countFile, err := excelize.OpenFile(path)
	if err != nil {
		file.Close()
		return nil, 0, fmt.Errorf("failed to open Excel file for counting: %v", err)
	}
	countRows, err := countFile.Rows(sheets[0])
	if err != nil {
		countFile.Close()
		file.Close()
		return nil, 0, fmt.Errorf("failed to count Excel rows: %v", err)
	}
	for countRows.Next() {
		totalRows++
	}
	countFile.Close()
	totalRows-- // Subtract header row

	return &excelRowReader{
		file:      file,
		rows:      rows,
		headerMap: headerMap,
	}, totalRows, nil
}

func (r *excelRowReader) Read() ([]string, error) {
	if !r.rows.Next() {
		return nil, io.EOF
	}
	return r.rows.Columns()
}

func (r *excelRowReader) HeaderIndex(name string) int {
	return r.headerMap[strings.ToLower(name)]
}

func (r *excelRowReader) Close() error {
	return r.file.Close()
}

// verifyRequiredFields checks if all required fields are present in the header
func verifyRequiredFields(headerMap map[string]int) error {
	requiredFields := []string{
		"id_upload", "path", "nama_modul", "file_type",
		"nama_file_asli", "id_profile", "id", "str_key", "str_subkey",
	}

	for _, field := range requiredFields {
		if _, ok := headerMap[field]; !ok {
			return fmt.Errorf("required field '%s' not found in file", field)
		}
	}
	return nil
}
