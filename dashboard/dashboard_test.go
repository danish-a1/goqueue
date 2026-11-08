package dashboard

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestNew_EmptyToken_404Everywhere(t *testing.T) {
	e := newEnv(t, "")
	for _, c := range []struct{ method, path string }{
		{"GET", testBase},
		{"GET", testBase + "/"},
		{"GET", testBase + "/login"},
		{"POST", testBase + "/login"},
		{"POST", testBase + "/logout"},
	} {
		var form url.Values
		if c.method == "POST" {
			form = url.Values{"token": {""}}
		}
		w := e.do(c.method, c.path, form, sameOrigin)
		if w.Code != http.StatusNotFound {
			t.Errorf("%s %s = %d, want 404", c.method, c.path, w.Code)
		}
	}
}

func TestNew_Validation(t *testing.T) {
	rc := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	defer rc.Close()
	for name, o := range map[string]Options{
		"empty base path":      {AccessToken: testToken, Redis: rc},
		"base path no slash":   {BasePath: "x", AccessToken: testToken, Redis: rc},
		"nil redis with token": {BasePath: "/x", AccessToken: testToken},
		"double slash":         {BasePath: "//x", AccessToken: testToken, Redis: rc},
		"backslash":            {BasePath: "/x\\y", AccessToken: testToken, Redis: rc},
		"brace":                {BasePath: "/x{y}", AccessToken: testToken, Redis: rc},
		"space":                {BasePath: "/x y", AccessToken: testToken, Redis: rc},
	} {
		if h, err := New(o); err == nil || h != nil {
			t.Errorf("%s: want error and nil handler, got h=%v err=%v", name, h, err)
		}
	}
	if _, err := New(Options{BasePath: "/x/", AccessToken: testToken, Redis: rc}); err != nil {
		t.Errorf("trailing slash must be accepted: %v", err)
	}
	if _, err := New(Options{BasePath: "/x", AccessToken: "short-token", Redis: rc}); err == nil {
		t.Error("token under 32 bytes must be rejected")
	}
	if _, err := New(Options{BasePath: "/x", AccessToken: "  " + testToken + "\n", Redis: rc}); err != nil {
		t.Errorf("padded token must be trimmed: %v", err)
	}
	if _, err := New(Options{BasePath: "/x"}); err != nil {
		t.Errorf("no token, no redis must be accepted: %v", err)
	}
}

func TestNew_TrailingSlashBasePathTrimmed(t *testing.T) {
	e := newEnv(t, testToken)
	e.h, _ = New(Options{BasePath: testBase + "/", AccessToken: testToken, Redis: e.newRedis(t)})
	w := e.do("GET", testBase+"/", nil)
	if w.Code != http.StatusFound || w.Header().Get("Location") != testBase+"/login" {
		t.Fatalf("got %d %q", w.Code, w.Header().Get("Location"))
	}
}

func TestUnauthenticated_RedirectsToLogin(t *testing.T) {
	e := newEnv(t, testToken)
	for _, p := range []string{testBase + "/", testBase, testBase + "/queues"} {
		w := e.do("GET", p, nil)
		if w.Code != http.StatusFound || w.Header().Get("Location") != testBase+"/login" {
			t.Errorf("GET %s = %d %q", p, w.Code, w.Header().Get("Location"))
		}
	}
}

func TestLoginForm_RendersWithoutSession(t *testing.T) {
	e := newEnv(t, testToken)
	w := e.do("GET", testBase+"/login", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("got %d", w.Code)
	}
	if b := w.Body.String(); !strings.Contains(b, `name="token"`) {
		t.Errorf("form lacks token field: %s", b)
	}
}

func TestPlaceholderRoot_AuthenticatedOK(t *testing.T) {
	e := newEnv(t, testToken)
	c := sessionCookie(e.login(testToken))
	w := e.do("GET", testBase+"/", nil, withCookie(c.Value))
	if w.Code != http.StatusOK {
		t.Fatalf("got %d", w.Code)
	}
}

func TestSecurityHeaders_OnEveryResponse(t *testing.T) {
	e := newEnv(t, testToken)
	want := map[string]string{
		"Cache-Control":           "no-store",
		"X-Frame-Options":         "DENY",
		"Content-Security-Policy": "default-src 'self'; base-uri 'none'; form-action 'self'; object-src 'none'; frame-ancestors 'none'",
		"X-Content-Type-Options":  "nosniff",
		"Referrer-Policy":         "same-origin",
	}
	for _, w := range []*httptest.ResponseRecorder{
		e.do("GET", testBase+"/login", nil),
		e.do("GET", testBase+"/", nil),
		e.login("bad"),
		e.do("GET", testBase+"/nope/nope", nil),
	} {
		for k, v := range want {
			if got := w.Header().Get(k); got != v {
				t.Errorf("%d %s = %q, want %q", w.Code, k, got, v)
			}
		}
	}
}
