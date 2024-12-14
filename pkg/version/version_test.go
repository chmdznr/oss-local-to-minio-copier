package version

import (
	"testing"
)

func TestVersionVariables(t *testing.T) {
	// Test Version
	if Version == "" {
		t.Error("Version should not be empty")
	}

	// Test GitCommit
	if GitCommit == "" {
		t.Error("GitCommit should not be empty")
	}
	if GitCommit != "unknown" && len(GitCommit) < 7 {
		t.Errorf("GitCommit '%s' seems invalid, should be 'unknown' or a git hash", GitCommit)
	}

	// Test BuildTime
	if BuildTime == "" {
		t.Error("BuildTime should not be empty")
	}
}
