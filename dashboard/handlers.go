package dashboard

import (
	"context"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// dayOptions are the history ranges the UI offers; anything else falls back to 30.
var dayOptions = []int{7, 30, 90, 180}

// summary is the reader summary with queues sorted by name for stable display.
func (s *server) summary(ctx context.Context) (Summary, error) {
	sum, err := s.rd.Summary(ctx)
	slices.SortFunc(sum.Queues, func(a, b QueueSummary) int { return strings.Compare(a.Name, b.Name) })
	return sum, err
}

// view loads what every page needs: the stat bar summary and layout fields.
func (s *server) view(ctx context.Context, tab, title string) (view, error) {
	sum, err := s.summary(ctx)
	return view{Base: s.BasePath, Poll: s.PollInterval.Milliseconds(), Tab: tab, Title: title, Sum: sum}, err
}

func (s *server) dashboardPage(w http.ResponseWriter, r *http.Request) {
	s.historyPage(w, r, "dashboard", "Dashboard", 30)
}

func (s *server) analyticsPage(w http.ResponseWriter, r *http.Request) {
	s.historyPage(w, r, "analytics", "Analytics", daysParam(r))
}

func (s *server) historyPage(w http.ResponseWriter, r *http.Request, tab, title string, days int) {
	v, err := s.view(r.Context(), tab, title)
	if err == nil {
		v.Days, err = s.rd.History(r.Context(), days)
	}
	if err != nil {
		s.fail(w, "page", err)
		return
	}
	v.DaysSel, v.DayOpts, v.Chart = days, dayOptions, newChart(v.Days)
	s.render(w, tab, v)
}

func (s *server) queuesPage(w http.ResponseWriter, r *http.Request) {
	v, err := s.view(r.Context(), "queues", "Queues")
	if err != nil {
		s.fail(w, "page", err)
		return
	}
	s.render(w, "queues", v)
}

// queuePage lists the pending jobs of one queue; unregistered names are 404.
func (s *server) queuePage(w http.ResponseWriter, r *http.Request) {
	v, err := s.view(r.Context(), "queues", "Pending")
	if err != nil {
		s.fail(w, "page", err)
		return
	}
	s.jobsPage(w, r, v, r.PathValue("name"), SourcePending)
}

// listPage serves retries/dead/deleted: per-queue counts without ?queue=, else that queue's jobs.
func (s *server) listPage(src Source, tab, title string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		v, err := s.view(r.Context(), tab, title)
		if err != nil {
			s.fail(w, "page", err)
			return
		}
		v.Filter = true
		s.jobsPage(w, r, v, r.URL.Query().Get("queue"), src)
	}
}

func (s *server) jobsPage(w http.ResponseWriter, r *http.Request, v view, queue string, src Source) {
	v.Queue, v.Source = queue, src
	if queue != "" {
		if !slices.ContainsFunc(v.Sum.Queues, func(q QueueSummary) bool { return q.Name == queue }) {
			http.NotFound(w, r)
			return
		}
		v.Page, v.Per = clamp(intParam(r, "page", 1), 1, 1_000_000), min(intParam(r, "per", 25), 100)
		var err error
		v.Jobs, v.Total, err = s.rd.Jobs(r.Context(), queue, src, v.Page, v.Per)
		if last := int(v.Pages()); err == nil && v.Total > 0 && v.Page > last {
			v.Page = last // past the end: show the last page so pager and rows agree
			v.Jobs, v.Total, err = s.rd.Jobs(r.Context(), queue, src, v.Page, v.Per)
		}
		if err != nil {
			s.fail(w, "page", err)
			return
		}
	}
	s.render(w, "jobs", v)
}

func (s *server) busyPage(w http.ResponseWriter, r *http.Request) {
	v, err := s.view(r.Context(), "busy", "Busy")
	if err == nil {
		v.Procs, err = s.rd.Processes(r.Context())
	}
	if err != nil {
		s.fail(w, "page", err)
		return
	}
	s.render(w, "busy", v)
}

// static serves the embedded assets; it runs after secure(), so this Cache-Control wins.
func (s *server) static(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, max-age=3600")
	http.ServeFileFS(w, r, staticFS, r.PathValue("file"))
}

// loginCSS serves the login stylesheet without a session; the login page cannot inline
// it under the CSP. Every other asset stays behind requireSession.
func (s *server) loginCSS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "public, max-age=3600")
	http.ServeFileFS(w, r, staticFS, "login.css")
}

// fail logs the error (never request bodies or job payloads) and returns a bare 500.
func (s *server) fail(w http.ResponseWriter, what string, err error) {
	s.Logger("dashboard "+what+" failed", "err", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

// intParam reads a positive int query value, def when missing or invalid.
func intParam(r *http.Request, key string, def int) int {
	n, err := strconv.Atoi(r.URL.Query().Get(key))
	if err != nil || n < 1 {
		return def
	}
	return n
}

func daysParam(r *http.Request) int {
	if n := intParam(r, "days", 30); slices.Contains(dayOptions, n) {
		return n
	}
	return 30
}
