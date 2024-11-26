# Local to MinIO Copier (msync)

A command-line tool for synchronizing local directories with MinIO object storage. This tool helps you manage and automate file uploads from your local system to MinIO buckets.

## Features

- Create and manage multiple sync projects
- Scan local directories for changes
- Track file sync status using project-specific SQLite databases
- Synchronize files to MinIO buckets
- Support for custom destination folders within buckets
- Cross-platform support (Windows paths are automatically converted)

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
msync.exe scan --project "project-name"
```

### Start Synchronization

```bash
msync.exe sync --project "project-name"
```

## Command Details

- `create`: Creates a new sync project with specified parameters
- `scan`: Scans the source directory and updates the project database with file information
- `sync`: Starts the synchronization process from local to MinIO

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

## Dependencies

- github.com/mattn/go-sqlite3: SQLite database driver
- github.com/minio/minio-go/v7: MinIO client
- github.com/urfave/cli/v2: CLI framework

## License

[Add your chosen license here]

## Contributing

[Add contribution guidelines here]
