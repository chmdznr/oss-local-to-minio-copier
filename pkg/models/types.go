package models

import (
	"encoding/json"
	"time"
)

type FileMetadata struct {
	Path         string `json:"path"`
	NamaModul    string `json:"nama_modul"`
	NamaFileAsli string `json:"nama_file_asli"`
	IDProfile    string `json:"id_profile"`
	Bucket       string `json:"bucket"`
	ExistingID   string `json:"existing_id"`
}

// CSVRecord represents a record from the CSV file
type CSVRecord struct {
	IDUpload     string `csv:"id_upload"`
	Path         string `csv:"path"`
	NamaModul    string `csv:"nama_modul"`
	FileType     string `csv:"file_type"`
	NamaFileAsli string `csv:"nama_file_asli"`
	IDProfile    string `csv:"id_profile"`
	ID           string `csv:"id"`
	StrKey       string `csv:"str_key"`
	StrSubKey    string `csv:"str_subkey"`
	Size         int64  `csv:"-"` // This is computed from the actual file
}

// FileRecord represents a file record in the database
type FileRecord struct {
	FilePath     string    `json:"file_path"`
	IDFile       string    `json:"id_file"`
	IDPermohonan string    `json:"id_permohonan"`
	IDFromCSV    string    `json:"id_from_csv"`
	Size         int64     `json:"size"`
	FileType     string    `json:"file_type"`
	BucketPath   string    `json:"bucketpath"`
	Metadata     string    `json:"f_metadata"`
	UserID       string    `json:"userid"`
	CreatedAt    time.Time `json:"created_at"`
	StrKey       string    `json:"str_key"`
	StrSubKey    string    `json:"str_subkey"`
	Timestamp    time.Time `json:"timestamp"`
	UploadStatus string    `json:"upload_status"`
}

func (f *FileRecord) SetMetadata(metadata FileMetadata) error {
	jsonData, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	f.Metadata = string(jsonData)
	return nil
}

func (f *FileRecord) GetMetadata() (*FileMetadata, error) {
	var metadata FileMetadata
	if err := json.Unmarshal([]byte(f.Metadata), &metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}

type Project struct {
	Name        string
	SourcePath  string
	Destination struct {
		Endpoint  string
		Bucket    string
		Folder    string
		AccessKey string
		SecretKey string
	}
}
