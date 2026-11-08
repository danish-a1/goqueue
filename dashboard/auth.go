package dashboard

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"html/template"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	cookieName   = "goqueue_session"
	loginMaxFail = 10
	loginWindow  = 15 * time.Minute
)

//go:embed templates/login.html
var loginHTML string

var loginTmpl = template.Must(template.New("login").Parse(loginHTML))

var failScript = redis.NewScript(`local n=redis.call('INCR',KEYS[1])
if n==1 then redis.call('PEXPIRE',KEYS[1],ARGV[1]) end
return n`)

func remoteHost(r *http.Request) string {
	if h, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return h
	}
	return r.RemoteAddr
}

func (s *server) sign(exp string) string {
	m := hmac.New(sha256.New, []byte(s.AccessToken))
	m.Write([]byte("goqueue-ui:" + exp))
	return exp + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func (s *server) validSession(r *http.Request) bool {
	c, err := r.Cookie(cookieName)
	if err != nil {
		return false
	}
	exp, _, ok := strings.Cut(c.Value, ".")
	if n, err := strconv.ParseInt(exp, 10, 64); !ok || err != nil || n <= time.Now().Unix() {
		return false
	}
	return hmac.Equal([]byte(c.Value), []byte(s.sign(exp)))
}

// requireSession redirects to the login page unless the request has a valid cookie.
func (s *server) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.validSession(r) {
			http.Redirect(w, r, s.BasePath+"/login", http.StatusFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// sameOrigin is the CSRF check: Origin (or Referer when absent) host must equal r.Host.
func (s *server) sameOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		src := r.Header.Get("Origin")
		if src == "" {
			src = r.Header.Get("Referer")
		}
		u, err := url.Parse(src)
		if src == "" || err != nil || u.Host == "" || u.Host != r.Host {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) renderLogin(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	_ = loginTmpl.Execute(w, map[string]string{"BasePath": s.BasePath, "Error": msg})
}

func (s *server) loginForm(w http.ResponseWriter, _ *http.Request) {
	s.renderLogin(w, http.StatusOK, "")
}

func (s *server) loginSubmit(w http.ResponseWriter, r *http.Request) {
	ctx, key := r.Context(), "goqueue:ui:login_fail:"+s.ClientIP(r)
	if n, err := s.Redis.Get(ctx, key).Int(); err != nil && err != redis.Nil {
		s.Logger("login throttle unavailable", "err", err)
		s.renderLogin(w, http.StatusServiceUnavailable, "Try again later.")
		return
	} else if n >= loginMaxFail {
		s.renderLogin(w, http.StatusTooManyRequests, "Too many attempts. Try again later.")
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.PostFormValue("token")), []byte(s.AccessToken)) != 1 {
		s.recordFail(ctx, key)
		s.renderLogin(w, http.StatusUnauthorized, "Invalid token.")
		return
	}
	exp := strconv.FormatInt(time.Now().Add(s.SessionTTL).Unix(), 10)
	s.setCookie(w, s.sign(exp), int(s.SessionTTL.Seconds()))
	http.Redirect(w, r, s.BasePath+"/", http.StatusSeeOther)
}

func (s *server) recordFail(ctx context.Context, key string) {
	if err := failScript.Run(ctx, s.Redis, []string{key}, loginWindow.Milliseconds()).Err(); err != nil {
		s.Logger("login fail counter error", "err", err)
	}
}

func (s *server) logout(w http.ResponseWriter, r *http.Request) {
	s.setCookie(w, "", -1)
	http.Redirect(w, r, s.BasePath+"/login", http.StatusSeeOther)
}

func (s *server) setCookie(w http.ResponseWriter, val string, maxAge int) {
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: val, Path: s.BasePath, MaxAge: maxAge,
		HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})
}
