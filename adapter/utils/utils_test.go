package utils

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTruncateUTF8(t *testing.T) {
	euro := "€" // 3 bytes
	tests := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"shorter than limit", "abc", 10, "abc"},
		{"exact limit", "abc", 3, "abc"},
		{"ascii cut", "abcdef", 4, "abcd"},
		{"cut inside rune drops partial rune", "ab" + euro, 3, "ab"},
		{"cut after full rune keeps it", "ab" + euro, 5, "ab" + euro},
		{"zero limit", "abc", 0, ""},
		{"empty input", "", 5, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := TruncateUTF8(tc.in, tc.n)
			if got != tc.want {
				t.Errorf("TruncateUTF8(%q, %d) = %q, want %q", tc.in, tc.n, got, tc.want)
			}
			if len(got) > tc.n || !utf8.ValidString(got) {
				t.Errorf("result len=%d valid=%v", len(got), utf8.ValidString(got))
			}
		})
	}
	if got := TruncateUTF8(strings.Repeat(euro, 100), 256); len(got) > 256 || !utf8.ValidString(got) {
		t.Errorf("long multibyte input: len=%d", len(got))
	}
}
