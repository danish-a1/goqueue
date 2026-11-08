package dashboard

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

//go:embed templates/layout.html templates/dashboard.html templates/analytics.html
//go:embed templates/queues.html templates/jobs.html templates/busy.html
var pageFS embed.FS

//go:embed static
var staticRoot embed.FS

var staticFS, _ = fs.Sub(staticRoot, "static")

// maxArgsShown caps the rendered payload per row so a 100-row page stays small.
const maxArgsShown = 4 << 10

var pages = template.Must(template.New("").Funcs(template.FuncMap{
	"pathEscape": url.PathEscape,
	"tm":         fmtTime,
	"args":       prettyArgs,
	"actions":    actionsFor,
	"label":      func(s Action) string { return strings.ToUpper(string(s[:1])) + string(s[1:]) },
	"nav":        func() []navItem { return navItems },
}).ParseFS(pageFS, "templates/*.html"))

type navItem struct{ Tab, Path, Label string }

var navItems = []navItem{
	{"dashboard", "/", "Dashboard"}, {"busy", "/busy", "Busy"}, {"queues", "/queues", "Queues"},
	{"retries", "/retries", "Retries"}, {"dead", "/dead", "Dead"}, {"deleted", "/deleted", "Deleted"},
	{"analytics", "/analytics", "Analytics"},
}

// view is the data for every page; page-specific fields stay zero elsewhere.
type view struct {
	Base, Tab, Title string
	Poll             int64 // stat poll interval, ms
	Sum              Summary

	Queue  string // job list queue; "" shows the per-queue table
	Source Source
	Filter bool // queue travels in ?queue= (list pages) rather than the path
	Jobs   []JobRow
	Page   int
	Per    int
	Total  int64

	Days    []DayStat
	DaysSel int
	DayOpts []int
	Chart   chart

	Procs []Process
}

func (v view) PrevPage() int {
	if v.Page > 1 {
		return v.Page - 1
	}
	return 0
}

func (v view) NextPage() int {
	if int64(v.Page)*int64(v.Per) < v.Total {
		return v.Page + 1
	}
	return 0
}

func (v view) Pages() int64 { return max(1, (v.Total+int64(v.Per)-1)/int64(max(v.Per, 1))) }

// chart is a server-rendered SVG line chart of processed vs failed per day.
type chart struct {
	W, H              int
	Processed, Failed string // polyline points
	Max               int64
	From, To          string
}

func newChart(days []DayStat) chart {
	c := chart{W: 800, H: 200}
	if len(days) == 0 {
		return c
	}
	for _, d := range days {
		c.Max = max(c.Max, d.Processed, d.Failed)
	}
	c.From, c.To = days[0].Day, days[len(days)-1].Day
	c.Processed = c.points(days, func(d DayStat) int64 { return d.Processed })
	c.Failed = c.points(days, func(d DayStat) int64 { return d.Failed })
	return c
}

func (c chart) points(days []DayStat, val func(DayStat) int64) string {
	var b strings.Builder
	step := float64(c.W) / float64(max(len(days)-1, 1))
	top := float64(max(c.Max, 1))
	for i, d := range days {
		fmt.Fprintf(&b, "%.1f,%.1f ", float64(i)*step, float64(c.H)*(1-float64(val(d))/top))
	}
	return strings.TrimSpace(b.String())
}

// actionsFor lists the actions allowed from src, in button order.
func actionsFor(src Source) []Action {
	var out []Action
	for _, a := range []Action{ActionRetry, ActionKill, ActionDelete, ActionRestore} {
		if slices.Contains(allowedSources[a], src) {
			out = append(out, a)
		}
	}
	return out
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format("2006-01-02 15:04:05")
}

// prettyArgs indents the job payload for display; html/template escapes the result.
func prettyArgs(raw json.RawMessage) string {
	var b bytes.Buffer
	s := string(raw)
	if json.Indent(&b, raw, "", "  ") == nil {
		s = b.String()
	}
	if len(s) > maxArgsShown {
		s = strings.ToValidUTF8(s[:maxArgsShown], "") + "\n... (truncated)"
	}
	return s
}

// render executes into a buffer first so a template error never sends half a page.
func (s *server) render(w http.ResponseWriter, name string, v view) {
	var buf bytes.Buffer
	if err := pages.ExecuteTemplate(&buf, name, v); err != nil {
		s.fail(w, "render", err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}
