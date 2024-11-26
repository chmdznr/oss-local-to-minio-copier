package models

// Stats represents project statistics
type Stats struct {
	TotalFiles    int64
	TotalSize     int64
	UploadedFiles int64
	UploadedSize  int64
	PendingFiles  int64
	PendingSize   int64
}
