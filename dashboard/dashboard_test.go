package dashboard

import (
	"net/http"
	"net/url"
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
		"empty base path":      {AccessToken: "t", Redis: rc},
		"base path no slash":   {BasePath: "x", AccessToken: "t", Redis: rc},
		"nil redis with token": {BasePath: "/x", AccessToken: "t"},
	} {
		if h, err := New(o); err == nil || h != nil {
			t.Errorf("%s: want error and nil handler, got h=%v err=%v", name, h, err)
		}
	}
	if _, err := New(Options{BasePath: "/x/", AccessToken: "t", Redis: rc}); err != nil {
		t.Errorf("trailing slash must be accepted: %v", err)
	}
	if _, err := New(Options{BasePath: "/x"}); err != nil {
		t.Errorf("no token, no redis must be accepted: %v", err)
	}
}

func TestNew_TrailingSlashBasePathTrimmed(t *testing.T) {
	e := newEnv(t, testToken)
	e.h, _ = New(Options{BasePath: testBase + "/", AccessToken: testToken, Redis: redis.NewClient(&redis.Options{Addr: e.mr.Addr()})})
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
	if b := w.Body.String(); !contains(b, `name="token"`) {
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

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
