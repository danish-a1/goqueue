package dashboard

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const (
	testBase  = "/goqueue-ui"
	testToken = "s3cret-token-value-0123456789abcdef"
	testHost  = "example.test"
	testIP    = "203.0.113.7"
)

type logSink struct {
	mu   sync.Mutex
	msgs []string
}

func (l *logSink) log(msg string, kv ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msgs = append(l.msgs, msg+" "+fmt.Sprint(kv...))
}

func (l *logSink) all() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.msgs, "\n")
}

type env struct {
	h    http.Handler
	mr   *miniredis.Miniredis
	logs *logSink
}

func newEnv(t *testing.T, token string) *env {
	t.Helper()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	logs := &logSink{}
	h, err := New(Options{BasePath: testBase, AccessToken: token, Redis: rc, Logger: logs.log})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &env{h: h, mr: mr, logs: logs}
}

func (e *env) do(method, path string, form url.Values, mods ...func(*http.Request)) *httptest.ResponseRecorder {
	var r *http.Request
	if form != nil {
		r = httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	r.Host = testHost
	r.RemoteAddr = testIP + ":4242"
	for _, m := range mods {
		m(r)
	}
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, r)
	return w
}

func sameOrigin(r *http.Request) { r.Header.Set("Origin", "http://"+testHost) }

func withCookie(v string) func(*http.Request) {
	return func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "goqueue_session", Value: v}) }
}

func (e *env) login(token string) *httptest.ResponseRecorder {
	return e.do("POST", testBase+"/login", url.Values{"token": {token}}, sameOrigin)
}

func signCookie(token string, expiry time.Time) string {
	exp := strconv.FormatInt(expiry.Unix(), 10)
	m := hmac.New(sha256.New, []byte(token))
	m.Write([]byte("goqueue-ui:" + exp))
	return exp + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func sessionCookie(w *httptest.ResponseRecorder) *http.Cookie {
	for _, c := range w.Result().Cookies() {
		if c.Name == "goqueue_session" {
			return c
		}
	}
	return nil
}

func (e *env) newRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	rc := redis.NewClient(&redis.Options{Addr: e.mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	return rc
}
