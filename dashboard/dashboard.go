// Package dashboard serves the goqueue web UI as a plain http.Handler.
package dashboard

import (
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/redis/go-redis/v9"
)

// Options configures the dashboard. AccessToken empty means every path is 404.
type Options struct {
	BasePath     string                      // required, e.g. "/goqueue-ui"; trailing slash trimmed
	AccessToken  string                      // empty = 404 everywhere
	Redis        redis.UniversalClient       // required when AccessToken is set
	SessionTTL   time.Duration               // default 12h
	PollInterval time.Duration               // default 3s
	ClientIP     func(*http.Request) string  // default: host part of RemoteAddr
	Logger       func(msg string, kv ...any) // optional; default no-op
}

type server struct {
	Options
	mux *http.ServeMux
}

// New validates o and returns the dashboard handler.
func New(o Options) (http.Handler, error) {
	o.BasePath = strings.TrimRight(o.BasePath, "/")
	if o.BasePath == "" || !strings.HasPrefix(o.BasePath, "/") || strings.HasPrefix(o.BasePath, "//") ||
		strings.ContainsAny(o.BasePath, "\\{}") || strings.IndexFunc(o.BasePath, unicode.IsSpace) >= 0 {
		return nil, errors.New("dashboard: BasePath must start with a single \"/\" and contain no whitespace, backslash or braces")
	}
	o.AccessToken = strings.TrimSpace(o.AccessToken)
	if o.AccessToken == "" {
		return secure(http.NotFoundHandler()), nil
	}
	if len(o.AccessToken) < minTokenLen {
		return nil, errors.New("dashboard: access token must be at least 32 bytes; generate with: openssl rand -hex 32")
	}
	if o.Redis == nil {
		return nil, errors.New("dashboard: Redis is required when AccessToken is set")
	}
	if o.SessionTTL <= 0 {
		o.SessionTTL = 12 * time.Hour
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 3 * time.Second
	}
	if o.ClientIP == nil {
		o.ClientIP = remoteHost
	}
	if o.Logger == nil {
		o.Logger = func(string, ...any) {}
	}
	s := &server{Options: o, mux: http.NewServeMux()}
	s.routes()
	return secure(s.mux), nil
}

// routes registers every route; later tasks add theirs here, wrapped in
// s.requireSession (GET pages) and s.sameOrigin (POST actions).
func (s *server) routes() {
	b := s.BasePath
	s.mux.HandleFunc("GET "+b+"/login", s.loginForm)
	s.mux.Handle("POST "+b+"/login", s.sameOrigin(http.HandlerFunc(s.loginSubmit)))
	s.mux.Handle("POST "+b+"/logout", s.sameOrigin(http.HandlerFunc(s.logout)))
	s.mux.Handle("GET "+b, s.requireSession(http.RedirectHandler(b+"/", http.StatusFound)))
	s.mux.Handle("GET "+b+"/{path...}", s.requireSession(http.HandlerFunc(s.page)))
}

// page is the authenticated catch-all; only the root exists until later tasks.
func (s *server) page(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != s.BasePath+"/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte("<!doctype html><title>goqueue</title><p>goqueue dashboard</p>"))
}

const minTokenLen = 32

// secure sets the hardening headers on every response, including errors and redirects.
func secure(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Cache-Control", "no-store")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		next.ServeHTTP(w, r)
	})
}
