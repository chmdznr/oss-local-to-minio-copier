# Local to MinIO Copier (msync)

A command-line tool for synchronizing local directories with MinIO object storage. This tool helps you manage and automate file uploads from your local system to MinIO buckets.

## Features

- Fast parallel file scanning and uploading
- Efficient handling of large files and directories
- Real-time progress tracking
- Smart file change detection
- Automatic retry of failed uploads
- Detailed status reporting

## Project Structure

```
local-to-minio-copier/
├── cmd/
│   └── msync/       # CLI application entry point
├── internal/
│   ├── db/         # Database operations
│   └── sync/       # File scanning and sync logic
└── pkg/
    └── models/     # Shared data structures
```

## Prerequisites

- Go 1.16 or later
- MinIO server access (endpoint, access key, and secret key)

## Installation

### Using go install

You can install msync using Go's package manager:

```bash
go install github.com/chmdznr/local-to-minio-copier/cmd/msync@latest
```

Make sure your Go bin directory (typically `$GOPATH/bin` or `$HOME/go/bin`) is in your system's PATH.

### Manual Build

Clone the repository and build manually:

```bash
git clone https://github.com/chmdznr/local-to-minio-copier.git
cd local-to-minio-copier
go mod tidy
go build -o msync.exe ./cmd/msync
```

## Usage

### Create a Project

```bash
msync create --name <project-name> --source <source-dir> --endpoint <minio-endpoint> --bucket <bucket-name> --folder <dest-folder> --access-key <access-key> --secret-key <secret-key>
```

### Scan Files

```bash
msync scan --project <project-name> [--workers <num-workers>] [--batch <batch-size>]
```

The scan command:
- Detects new files not in the database
- Identifies modified files by comparing timestamps and sizes
- Requeues files that weren't successfully uploaded
- Shows real-time progress including:
  - New files count and size
  - Modified files count and size
  - Requeued files count and size
  - Unchanged files count and size

### Sync Files

```bash
msync sync --project <project-name> [--workers <num-workers>] [--batch <batch-size>]
```

The sync command:
- Uploads all pending files to MinIO
- Automatically retries previously failed uploads
- Skips files that no longer exist
- Shows real-time progress including:
  - Upload progress (files and sizes)
  - Retry attempts
  - Skipped files

### Show Status

```bash
msync status --project <project-name>
```

## File States

Files can be in one of the following states:
- `pending`: New, modified, or queued for upload
- `uploaded`: Successfully uploaded to MinIO
- `failed`: Upload attempt failed
- `skipped`: File no longer exists in source directory

## Performance Tuning

### Scan Performance
- Increase `--workers` for faster scanning on systems with good disk I/O
- Increase `--batch` to reduce database overhead with large file sets
- Example for large directories: `--workers 16 --batch 5000`

### Sync Performance
- Increase `--workers` for faster uploads with good network connection
- Adjust `--batch` based on memory availability and update frequency needs
- Example for fast networks: `--workers 32 --batch 200`
- Example for slower networks: `--workers 8 --batch 50`

## Configuration Parameters

- `name`: Project name for identification (also used as database name)
- `source`: Local directory path to sync
- `endpoint`: MinIO server endpoint
- `bucket`: Target MinIO bucket
- `folder`: (Optional) Destination folder path within the bucket
- `access-key`: MinIO access key
- `secret-key`: MinIO secret key

## Project Files

- `[project-name].db`: SQLite database for each project, storing project configuration and file status
- `[project-name].db-wal`, `[project-name].db-shm`: SQLite write-ahead log files
