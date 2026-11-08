package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	gqredis "github.com/danish-a1/goqueue/adapter/redis"
)

// authed logs in and returns a request modifier carrying the session cookie.
func authed(t *testing.T, e *env) func(*http.Request) {
	t.Helper()
	c := sessionCookie(e.login(testToken))
	if c == nil {
		t.Fatal("login did not set a session cookie")
	}
	return withCookie(c.Value)
}

func get(e *env, path string, mods ...func(*http.Request)) *httptest.ResponseRecorder {
	return e.do("GET", testBase+path, nil, mods...)
}

func TestPages_200WhenLoggedIn(t *testing.T) {
	e := newEnv(t, testToken)
	rc := e.newRedis(t)
	seedQueue(t, rc, "alpha", 3, 2, 2, 1)
	ck := authed(t, e)

	paths := []string{
		"/", "/analytics", "/analytics?days=30", "/queues", "/queues/alpha",
		"/retries", "/retries?queue=alpha", "/dead", "/dead?queue=alpha",
		"/deleted", "/deleted?queue=alpha", "/busy",
	}
	for _, p := range paths {
		w := get(e, p, ck)
		if w.Code != 200 {
			t.Errorf("GET %s = %d, want 200", p, w.Code)
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Errorf("GET %s content-type = %q, want text/html", p, ct)
		}
	}
}

func TestPages_UnauthenticatedRedirects(t *testing.T) {
	e := newEnv(t, testToken)
	seedQueue(t, e.newRedis(t), "alpha", 1, 0, 0, 0)
	for _, p := range []string{"/queues/alpha", "/queues", "/dead", "/api/stats", "/static/app.css"} {
		w := get(e, p)
		if w.Code != http.StatusFound || !strings.Contains(w.Header().Get("Location"), "/login") {
			t.Errorf("GET %s unauth = %d loc %q, want 302 to login", p, w.Code, w.Header().Get("Location"))
		}
	}
}

func TestQueuePage_UnknownQueue404(t *testing.T) {
	e := newEnv(t, testToken)
	w := get(e, "/queues/nope", authed(t, e))
	if w.Code != 404 {
		t.Fatalf("unknown queue = %d, want 404", w.Code)
	}
}

func TestQueuePage_ColonName(t *testing.T) {
	e := newEnv(t, testToken)
	rc := e.newRedis(t)
	ck := authed(t, e)
	hrefRe := regexp.MustCompile(`<nav class="sub"><a href="([^"]+)"`)
	for _, name := range []string{"order-hub:status-delivery", "a/b"} {
		seedQueue(t, rc, name, 2, 0, 0, 0)
		w := get(e, "/queues/"+url.PathEscape(name), ck)
		if w.Code != 200 {
			t.Fatalf("queue %q = %d, want 200", name, w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "<small>"+name+"</small>") {
			t.Errorf("queue %q: heading <small>%s</small> missing", name, name)
		}
		if !strings.Contains(body, "<code>"+name+"-p0</code>") || !strings.Contains(body, `value="`+name+`-p1"`) {
			t.Errorf("queue %q: pending job IDs not shown exactly", name)
		}
		if !strings.Contains(body, `data-queue="`+name+`"`) {
			t.Errorf("queue %q: data-queue attribute not exact", name)
		}
		// The rendered Pending link must lead back to the same page.
		m := hrefRe.FindStringSubmatch(body)
		if m == nil {
			t.Fatalf("queue %q: sub-nav link missing", name)
		}
		want := testBase + "/queues/" + url.PathEscape(name)
		if m[1] != want {
			t.Errorf("queue %q: link %q, want %q", name, m[1], want)
		}
		r := httptest.NewRequest("GET", m[1], nil)
		if r.URL.Path != testBase+"/queues/"+name {
			t.Errorf("queue %q: link does not unescape to the name: %q", name, r.URL.Path)
		}
	}
}

func TestListPages_QueueFilterAndPagination(t *testing.T) {
	e := newEnv(t, testToken)
	rc := e.newRedis(t)
	seedQueue(t, rc, "alpha", 0, 30, 30, 30)
	seedQueue(t, rc, "beta", 0, 1, 1, 1)
	ck := authed(t, e)
	rd := newReader(rc)

	for page, src := range map[string]Source{"/retries": SourceRetry, "/dead": SourceDead, "/deleted": SourceDeleted} {
		// no ?queue= lists queues with links
		w := get(e, page, ck)
		body := w.Body.String()
		if w.Code != 200 || !strings.Contains(body, "queue=alpha") || !strings.Contains(body, "queue=beta") {
			t.Errorf("%s without queue: code %d, want per-queue links", page, w.Code)
		}

		p1, _, err := rd.Jobs(context.Background(), "alpha", src, 1, 25)
		if err != nil || len(p1) != 25 {
			t.Fatalf("%s reader page1: %v len %d", page, err, len(p1))
		}
		p2, _, err := rd.Jobs(context.Background(), "alpha", src, 2, 25)
		if err != nil || len(p2) != 5 {
			t.Fatalf("%s reader page2: %v len %d", page, err, len(p2))
		}

		w = get(e, page+"?queue=alpha", ck)
		body = w.Body.String()
		if w.Code != 200 || !strings.Contains(body, p1[0].ID) {
			t.Errorf("%s?queue=alpha: code %d, first row %q missing", page, w.Code, p1[0].ID)
		}
		if strings.Contains(body, "beta-") {
			t.Errorf("%s?queue=alpha leaked another queue's rows", page)
		}

		w = get(e, page+"?queue=alpha&page=2&per=25", ck)
		body = w.Body.String()
		if w.Code != 200 || !strings.Contains(body, p2[0].ID) {
			t.Errorf("%s page 2: code %d, id %q missing", page, w.Code, p2[0].ID)
		}
		if strings.Contains(body, p1[0].ID+"<") || strings.Contains(body, ">"+p1[0].ID) || strings.Contains(body, p1[0].ID+"\"") {
			t.Errorf("%s page 2 still shows page-1 id %q", page, p1[0].ID)
		}
	}
}

func TestTemplates_EscapeArgs(t *testing.T) {
	e := newEnv(t, testToken)
	rc := e.newRedis(t)
	ctx := context.Background()
	rc.HSet(ctx, gqredis.KeyQueues, "alpha", "dlq:alpha")
	m := (`{"job":{"x":"<script>alert(1)</script>"},"id":"xss-1","job_name":"Job","enqueued_at":"2026-01-01T00:00:00Z","retry_count":0}`)
	rc.LPush(ctx, gqredis.KeyPending("alpha"), m)

	w := get(e, "/queues/alpha", authed(t, e))
	body := w.Body.String()
	if w.Code != 200 || !strings.Contains(body, "xss-1") {
		t.Fatalf("page code %d, row missing", w.Code)
	}
	if strings.Contains(body, "<script>alert(1)</script>") {
		t.Error("raw script tag rendered unescaped")
	}
	if !strings.Contains(body, "&lt;script&gt;") {
		t.Error("escaped script tag not found")
	}
}

func TestStatic_CacheHeaders(t *testing.T) {
	e := newEnv(t, testToken)
	ck := authed(t, e)
	cases := map[string][]string{
		"/static/app.css": {"text/css"},
		"/static/app.js":  {"text/javascript", "application/javascript"},
	}
	for p, types := range cases {
		w := get(e, p, ck)
		if w.Code != 200 {
			t.Errorf("%s = %d", p, w.Code)
			continue
		}
		ct := w.Header().Get("Content-Type")
		ok := false
		for _, ty := range types {
			ok = ok || strings.HasPrefix(ct, ty)
		}
		if !ok {
			t.Errorf("%s content-type %q, want one of %v", p, ct, types)
		}
		if cc := w.Header().Get("Cache-Control"); !strings.Contains(cc, "max-age") {
			t.Errorf("%s cache-control %q, want max-age", p, cc)
		}
	}
}

func TestDashboardPage_PollIntervalInHTML(t *testing.T) {
	e := newEnv(t, testToken)
	w := get(e, "/", authed(t, e))
	if !strings.Contains(w.Body.String(), `data-poll="3000"`) {
		t.Error(`dashboard page lacks data-poll="3000"`)
	}
}
