package utils

import (
	"reflect"
	"unicode/utf8"

	"github.com/danish-a1/goqueue/job"
	"github.com/google/uuid"
)

// genearte JOB Id for jobs pushed
func GenerateID() string {
	return uuid.New().String()
}

// getJobName returns the concrete type name of a job
func GetJobName(jb job.Job) string {
	t := reflect.TypeOf(jb)
	if t == nil {
		return ""
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

// TruncateUTF8 cuts s to at most n bytes without splitting a multibyte rune.
func TruncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
