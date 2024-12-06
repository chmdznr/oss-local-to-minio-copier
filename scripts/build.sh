#!/bin/bash

# Get the current Git commit hash
GIT_COMMIT=$(git rev-parse --short HEAD)

# Get the current timestamp in UTC
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Build the application with version information
go build -ldflags "-X github.com/chmdznr/oss-local-to-minio-copier/pkg/version.GitCommit=${GIT_COMMIT} -X github.com/chmdznr/oss-local-to-minio-copier/pkg/version.BuildTime=${BUILD_TIME}" -o msync ./cmd/msync
