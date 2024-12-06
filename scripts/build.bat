@echo off
setlocal

REM Get the current Git commit hash
for /f %%i in ('git rev-parse --short HEAD') do set GIT_COMMIT=%%i

REM Get the current timestamp in UTC
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
set BUILD_TIME=%datetime:~0,4%-%datetime:~4,2%-%datetime:~6,2%T%datetime:~8,2%:%datetime:~10,2%:%datetime:~12,2%Z

REM Build the application with version information
go build -ldflags "-X github.com/chmdznr/oss-local-to-minio-copier/pkg/version.GitCommit=%GIT_COMMIT% -X github.com/chmdznr/oss-local-to-minio-copier/pkg/version.BuildTime=%BUILD_TIME%" -o msync.exe ./cmd/msync
