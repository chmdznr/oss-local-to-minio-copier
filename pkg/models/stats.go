package models

// Stats represents project statistics
type Stats struct {
	TotalFiles    int64
	TotalSize     int64
	UploadedFiles int64
	UploadedSize  int64
	PendingFiles  int64
	PendingSize   int64
	FailedFiles   int64
	FailedSize    int64
	MissingFiles  int64 // New field for missing files count
}
