# Local to MinIO Copier (msync)

A command-line tool for synchronizing local directories with MinIO object storage. This tool helps you manage and automate file uploads from your local system to MinIO buckets.

## Features

- Create and manage multiple sync projects
- Fast parallel file scanning with real-time progress tracking
- Efficient batch processing for database operations
- Concurrent file uploads with configurable workers
- Track file sync status using project-specific SQLite databases
- Support for custom destination folders within buckets
- Cross-platform support (Windows paths are automatically converted)
- Real-time progress reporting for both scanning and syncing operations

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

Clone the repository and build manually:

```bash
git clone https://github.com/chmdznr/local-to-minio-copier.git
cd local-to-minio-copier
go mod tidy
go build -o msync.exe ./cmd/msync
```

## Usage

### Create a New Sync Project

Windows:
```cmd
msync.exe create --name "project-name" ^
            --source "D:\path\to\local\directory" ^
            --endpoint "minio.example.com" ^
            --bucket "your-bucket" ^
            --folder "optional/folder/path" ^
            --access-key "your-access-key" ^
            --secret-key "your-secret-key"
```

Unix-like:
```bash
./msync create --name "project-name" \
            --source "/path/to/local/directory" \
            --endpoint "minio.example.com" \
            --bucket "your-bucket" \
            --folder "optional/folder/path" \
            --access-key "your-access-key" \
            --secret-key "your-secret-key"
```

### Scan Files in Project

```bash
# Basic usage
msync.exe scan --project "project-name"

# With performance tuning
msync.exe scan --project "project-name" --workers 8 --batch 2000
```

### Start Synchronization

```bash
# Basic usage
msync.exe sync --project "project-name"

# With performance tuning
msync.exe sync --project "project-name" --workers 32 --batch 200
```

### View Project Status

```bash
msync.exe status --project "project-name"
```

## Command Details

### Create Command
- Creates a new sync project with specified parameters

### Scan Command
- Scans the source directory and updates the project database
- Shows real-time progress (files scanned and total size)
- Performance options:
  - `--workers`: Number of parallel scanner workers (default: 8)
  - `--batch`: Number of files to process in one database transaction (default: 1000)

### Sync Command
- Synchronizes files from local to MinIO storage
- Shows real-time progress (files uploaded, percentage, and size)
- Performance options:
  - `--workers`: Number of parallel upload workers (default: 16)
  - `--batch`: Number of files to update in one database transaction (default: 100)

### Status Command
- Shows current project status including pending uploads

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
