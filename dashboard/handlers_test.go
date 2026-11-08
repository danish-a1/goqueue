package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	for _, name := range []string{"order-hub:status-delivery", "a/b"} {
		seedQueue(t, rc, name, 2, 0, 0, 0)
		w := get(e, "/queues/"+url.PathEscape(name), ck)
		if w.Code != 200 {
			t.Fatalf("queue %q = %d, want 200", name, w.Code)
		}
		body := w.Body.String()
		// html/template may escape "/" or ":" differently; accept the name or its escaped form.
		if !strings.Contains(body, name) && !strings.Contains(body, strings.ReplaceAll(name, "/", "&#47;")) {
			t.Errorf("queue %q: name not shown in page", name)
		}
		if !strings.Contains(body, name+"-p0") && !strings.Contains(body, name+"-p1") {
			t.Errorf("queue %q: pending job IDs not shown", name)
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

func TestAPIStats_JSONShape(t *testing.T) {
	e := newEnv(t, testToken)
	rc := e.newRedis(t)
	seedQueue(t, rc, "alpha", 3, 2, 1, 1)
	seedQueue(t, rc, "beta", 1, 0, 0, 0)

	w := get(e, "/api/stats", authed(t, e))
	if w.Code != 200 {
		t.Fatalf("stats = %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type %q", ct)
	}
	var s Summary
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if s.Pending != 4 || s.Retry != 2 || s.Dead != 1 || s.Deleted != 1 || len(s.Queues) != 2 {
		t.Errorf("summary = %+v", s)
	}
}

func TestAPIHistory_Days(t *testing.T) {
	e := newEnv(t, testToken)
	w := get(e, "/api/history?days=7", authed(t, e))
	if w.Code != 200 {
		t.Fatalf("history = %d", w.Code)
	}
	var d []DayStat
	if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(d) != 7 {
		t.Errorf("len = %d, want 7", len(d))
	}
}

func postAction(e *env, body, ctype string, mods ...func(*http.Request)) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", testBase+"/api/actions", strings.NewReader(body))
	r.Header.Set("Content-Type", ctype)
	r.Host = testHost
	r.RemoteAddr = testIP + ":4242"
	for _, m := range mods {
		m(r)
	}
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, r)
	return w
}

const retryBody = `{"action":"retry","queue":"alpha","source":"dead","job_ids":["alpha-d0"]}`

func TestAPIActions_RequiresSessionAndOrigin(t *testing.T) {
	e := newEnv(t, testToken)
	rc := e.newRedis(t)
	seedQueue(t, rc, "alpha", 0, 0, 2, 0)
	ctx := context.Background()
	deadLen := func() int64 { return rc.LLen(ctx, "dlq:alpha").Val() }
	ck := authed(t, e)

	// no session
	w := postAction(e, retryBody, "application/json", sameOrigin)
	if w.Code == 200 || (w.Code != 302 && w.Code != 401) {
		t.Errorf("no session = %d, want 302/401", w.Code)
	}
	if deadLen() != 2 {
		t.Fatal("unauthenticated action moved a job")
	}

	// cross origin
	w = postAction(e, retryBody, "application/json", ck, func(r *http.Request) { r.Header.Set("Origin", "http://evil.test") })
	if w.Code != 403 {
		t.Errorf("cross-origin = %d, want 403", w.Code)
	}
	if deadLen() != 2 {
		t.Fatal("cross-origin action moved a job")
	}

	// wrong content type
	w = postAction(e, retryBody, "text/plain", ck, sameOrigin)
	if w.Code != http.StatusUnsupportedMediaType {
		t.Errorf("text/plain = %d, want 415", w.Code)
	}
	if deadLen() != 2 {
		t.Fatal("non-JSON action moved a job")
	}

	// invalid
	for _, bad := range []string{
		`{"action":"explode","queue":"alpha","source":"dead","job_ids":["x"]}`,
		`{"action":"retry","queue":"alpha","source":"pending","job_ids":["x"]}`,
		`{"action":"retry","queue":"alpha","source":"dead","job_ids":[]}`,
		`not json`,
	} {
		if w = postAction(e, bad, "application/json", ck, sameOrigin); w.Code != 400 {
			t.Errorf("invalid %q = %d, want 400", bad, w.Code)
		}
	}
	if deadLen() != 2 {
		t.Fatal("invalid action moved a job")
	}

	// happy path
	w = postAction(e, retryBody, "application/json", ck, sameOrigin)
	if w.Code != 200 {
		t.Fatalf("valid action = %d body %s", w.Code, w.Body)
	}
	var res ActionResult
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(res.Done) != 1 || res.Done[0] != "alpha-d0" {
		t.Errorf("result = %+v", res)
	}
	if deadLen() != 1 || rc.LLen(ctx, gqredis.KeyPending("alpha")).Val() != 1 {
		t.Error("job not moved dead -> pending")
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
