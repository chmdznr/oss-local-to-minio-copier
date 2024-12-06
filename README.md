# Local to MinIO Copier (msync)

A command-line tool for synchronizing local directories with MinIO object storage. This tool helps you manage and automate file uploads from your local system to MinIO buckets.

## Features

- Fast parallel file scanning and uploading
- Efficient handling of large files and directories
- Real-time progress tracking
- Smart file change detection
- Automatic retry of failed uploads
- Detailed status reporting
- CSV-based file import with metadata support
- Rich metadata handling for uploaded files

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
go install github.com/chmdznr/oss-local-to-minio-copier/cmd/msync@latest
```

Make sure your Go bin directory (typically `$GOPATH/bin` or `$HOME/go/bin`) is in your system's PATH.

### Building from Source

Clone the repository and build:

```bash
git clone https://github.com/chmdznr/oss-local-to-minio-copier.git
cd oss-local-to-minio-copier
go mod tidy
go build -o msync.exe ./cmd/msync
```

The compiled binary will be created as `msync.exe` in the current directory.

## Usage

### Create a Project

```bash
msync create --name <project-name> --source <source-dir> --endpoint <minio-endpoint> --bucket <bucket-name> --folder <dest-folder> --access-key <access-key> --secret-key <secret-key>
```

### Import Files from CSV

```bash
msync import --project <project-name> --csv <csv-file> [--batch <batch-size>]
```

The import command processes a CSV file containing file information. The CSV must include these columns:
- `id_upload`: Unique identifier for the upload
- `path`: Relative path to the file
- `nama_modul`: Module name
- `file_type`: Type of file
- `nama_file_asli`: Original file name
- `id_profile`: Profile ID
- `id`: Unique identifier (preserved as existing_id)
- `str_key`: String key
- `str_subkey`: String subkey

The command:
- Validates file existence in the source directory
- Skips non-existent or directory entries
- Preserves metadata for successful uploads
- Shows real-time progress of the import process

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

## Metadata Handling

Files uploaded to MinIO include rich metadata that is stored both in MinIO and the local database:

### During Import
- CSV metadata is temporarily stored
- File records are created with pending status
- Original CSV IDs are preserved

### After Upload
The following metadata is stored for each file:
- `path`: Original file path
- `nama_modul`: Module name from CSV
- `nama_file_asli`: Original file name from CSV
- `id_profile`: Profile ID from CSV
- `bucket`: Full bucket path including destination folder
- `existing_id`: Original ID from CSV

This metadata can be used for tracking, verification, and integration with other systems.

## Performance Tuning

### Import Performance
- Adjust `--batch` based on memory availability and CSV size
- Example for large CSV files: `--batch 5000`

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
