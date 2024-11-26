package models

import "time"

type FileRecord struct {
	FilePath     string
	Size         int64
	Timestamp    time.Time
	UploadStatus string
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
