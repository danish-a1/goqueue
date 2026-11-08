package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gqredis "github.com/danish-a1/goqueue/adapter/redis"
)

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

func TestAPIActions_Oversize413(t *testing.T) {
	e := newEnv(t, testToken)
	seedQueue(t, e.newRedis(t), "alpha", 0, 0, 1, 0)
	body := `{"action":"retry","queue":"alpha","source":"dead","job_ids":["` + strings.Repeat("x", 70<<10) + `"]}`
	if w := postAction(e, body, "application/json", authed(t, e), sameOrigin); w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversize body = %d, want 413", w.Code)
	}
}

func TestAPIActions_StrictDecode(t *testing.T) {
	e := newEnv(t, testToken)
	rc := e.newRedis(t)
	seedQueue(t, rc, "alpha", 0, 0, 2, 0)
	ck := authed(t, e)
	for _, bad := range []string{
		`{"action":"retry","queue":"alpha","source":"dead","job_ids":["alpha-d0"],"extra":1}`,
		retryBody + `{"action":"retry"}`,
		retryBody + ` []`,
	} {
		if w := postAction(e, bad, "application/json", ck, sameOrigin); w.Code != 400 {
			t.Errorf("%q = %d, want 400", bad, w.Code)
		}
	}
	if n := rc.LLen(context.Background(), "dlq:alpha").Val(); n != 2 {
		t.Fatalf("rejected body moved a job (dead len %d)", n)
	}
	if w := postAction(e, retryBody+"\n", "application/json", ck, sameOrigin); w.Code != 200 {
		t.Errorf("trailing newline = %d, want 200", w.Code)
	}
}

func TestAPIActions_AuditLogCountsOnly(t *testing.T) {
	e := newEnv(t, testToken)
	seedQueue(t, e.newRedis(t), "alpha", 0, 0, 2, 0)
	body := `{"action":"delete","queue":"alpha","source":"dead","job_ids":["alpha-d0","ghost"]}`
	if w := postAction(e, body, "application/json", authed(t, e), sameOrigin); w.Code != 200 {
		t.Fatalf("action = %d", w.Code)
	}
	logs := e.logs.all()
	for _, want := range []string{"dashboard action", "delete", `"alpha"`, "dead", testIP, "done", "not_found"} {
		if !strings.Contains(logs, want) {
			t.Errorf("audit log %q lacks %q", logs, want)
		}
	}
	for _, leak := range []string{"alpha-d0", "ghost", `"n"`} {
		if strings.Contains(logs, leak) {
			t.Errorf("audit log leaks %q: %q", leak, logs)
		}
	}
}

func TestAPI_500HidesInternals(t *testing.T) {
	e := newEnv(t, testToken)
	ck := authed(t, e)
	addr := e.mr.Addr()
	e.mr.Close()
	for _, w := range []*httptest.ResponseRecorder{
		get(e, "/api/stats", ck),
		get(e, "/", ck),
		postAction(e, retryBody, "application/json", ck, sameOrigin),
	} {
		body := w.Body.String()
		if w.Code != 500 || !strings.Contains(body, "internal error") {
			t.Errorf("code %d body %q, want 500 internal error", w.Code, body)
		}
		if strings.Contains(body, addr) || strings.Contains(body, "dial") || strings.Contains(body, "refused") {
			t.Errorf("500 body leaks internals: %q", body)
		}
	}
}
