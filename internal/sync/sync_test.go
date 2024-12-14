package sync

import (
	"testing"
)

func TestSanitizePath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "normal path",
			input:    "path/to/file.txt",
			expected: "path/to/file.txt",
		},
		{
			name:     "windows path",
			input:    "path\\to\\file.txt",
			expected: "path/to/file.txt",
		},
		{
			name:     "path with spaces",
			input:    "path/to/my file.txt",
			expected: "path/to/my+file.txt",
		},
		{
			name:     "path with special chars",
			input:    "path/to/file&name+test.txt",
			expected: "path/to/fileandname+test.txt",
		},
		{
			name:     "path with double slashes",
			input:    "path//to//file.txt",
			expected: "path/to/file.txt",
		},
		{
			name:     "path with dot segments",
			input:    "path/./to/../file.txt",
			expected: "path/./to/../file.txt", // Current implementation preserves dot segments
		},
		{
			name:     "empty path",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizePath(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizePath(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}
