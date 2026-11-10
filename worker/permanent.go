package worker

import "github.com/danish-a1/goqueue/job"

// isPermanent exists because processJobSafely's job parameter shadows the
// job package.
func isPermanent(err error) bool { return job.IsPermanent(err) }
