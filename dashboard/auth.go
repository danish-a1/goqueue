package dashboard

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"html/template"
	"mime"
	"net"
	"net/http"
	"net/netip"
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

var attemptScript = redis.NewScript(`local n=redis.call('INCR',KEYS[1])
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
	n, _ := strconv.ParseInt(exp, 10, 64)
	if n > time.Now().Add(s.SessionTTL+time.Minute).Unix() {
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
	if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/x-www-form-urlencoded" {
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
	ctx, key := r.Context(), "goqueue:ui:login_fail:"+s.throttleIP(r)
	// Reserve the attempt before comparing so parallel guesses cannot all see a low count.
	n, err := attemptScript.Run(ctx, s.Redis, []string{key}, loginWindow.Milliseconds()).Int()
	if err != nil {
		s.Logger("login throttle unavailable", "err", err)
		s.renderLogin(w, http.StatusServiceUnavailable, "Try again later.")
		return
	}
	if n > loginMaxFail {
		s.renderLogin(w, http.StatusTooManyRequests, "Too many attempts. Try again later.")
		return
	}
	guess, want := sha256.Sum256([]byte(r.PostFormValue("token"))), sha256.Sum256([]byte(s.AccessToken))
	if subtle.ConstantTimeCompare(guess[:], want[:]) != 1 {
		s.renderLogin(w, http.StatusUnauthorized, "Invalid token.")
		return
	}
	if err := s.Redis.Del(ctx, key).Err(); err != nil {
		s.Logger("login counter reset failed", "err", err)
	}
	exp := strconv.FormatInt(time.Now().Add(s.SessionTTL).Unix(), 10)
	s.setCookie(w, s.sign(exp), int(s.SessionTTL.Seconds()))
	http.Redirect(w, r, s.BasePath+"/", http.StatusSeeOther)
}

// throttleIP returns the client IP (IPv6 masked to /64), falling back to RemoteAddr
// when ClientIP yields something that is not an IP.
func (s *server) throttleIP(r *http.Request) string {
	a, err := netip.ParseAddr(s.ClientIP(r))
	if err != nil {
		if a, err = netip.ParseAddr(remoteHost(r)); err != nil {
			return "unknown"
		}
	}
	a = a.Unmap()
	if a.Is6() {
		p, _ := a.Prefix(64)
		return p.Masked().Addr().String()
	}
	return a.String()
}

func (s *server) logout(w http.ResponseWriter, r *http.Request) {
	s.setCookie(w, "", -1)
	http.Redirect(w, r, s.BasePath+"/login", http.StatusSeeOther)
}

func (s *server) setCookie(w http.ResponseWriter, val string, maxAge int) {
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: val, Path: s.BasePath, MaxAge: maxAge,
		HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})
}
