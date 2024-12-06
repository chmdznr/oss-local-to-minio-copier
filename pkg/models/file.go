package models

// File represents a file to be synchronized
type File struct {
	ID         int64
	LocalPath  string
	DestPath   string
	Size       int64
	Status     string
	Metadata   map[string]string
	CreatedAt  string
	UpdatedAt  string
	LastError  string
}
