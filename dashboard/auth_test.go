package dashboard

import (
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const failKey = "goqueue:ui:login_fail:" + testIP

func TestLogin_WrongToken_401_AndCounts(t *testing.T) {
	e := newEnv(t, testToken)
	for i := 1; i <= 2; i++ {
		w := e.login("nope")
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i, w.Code)
		}
		if sessionCookie(w) != nil {
			t.Fatal("cookie set on failure")
		}
	}
	got, err := e.mr.Get(failKey)
	if err != nil || got != "2" {
		t.Fatalf("counter = %q err=%v, want 2", got, err)
	}
	if ttl := e.mr.TTL(failKey); ttl <= 0 || ttl > 15*time.Minute {
		t.Fatalf("ttl = %v, want (0,15m]", ttl)
	}
}

func TestLogin_Throttled_429(t *testing.T) {
	e := newEnv(t, testToken)
	for i := 1; i <= 10; i++ {
		if w := e.login("bad"); w.Code != http.StatusUnauthorized {
			t.Fatalf("fail %d = %d, want 401", i, w.Code)
		}
	}
	w := e.login(testToken)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("correct token after 10 fails = %d, want 429", w.Code)
	}
	if sessionCookie(w) != nil {
		t.Fatal("cookie set while throttled")
	}
}

func TestLogin_Throttle_PerIP(t *testing.T) {
	e := newEnv(t, testToken)
	e.mr.Set(failKey, "10")
	w := e.do("POST", testBase+"/login", url.Values{"token": {testToken}}, sameOrigin,
		func(r *http.Request) { r.RemoteAddr = "198.51.100.9:1" })
	if w.Code != http.StatusSeeOther {
		t.Fatalf("other IP = %d, want 303", w.Code)
	}
}

func TestLogin_CustomClientIP(t *testing.T) {
	e := newEnv(t, testToken)
	h, err := New(Options{BasePath: testBase, AccessToken: testToken, Redis: e.newRedis(t),
		ClientIP: func(*http.Request) string { return "10.9.9.9" }})
	if err != nil {
		t.Fatal(err)
	}
	e.h = h
	e.login("bad")
	if v, _ := e.mr.Get("goqueue:ui:login_fail:10.9.9.9"); v != "1" {
		t.Fatalf("custom ClientIP key counter = %q", v)
	}
}

func TestLogin_Correct_SetsCookieFlags(t *testing.T) {
	e := newEnv(t, testToken)
	w := e.login(testToken)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != testBase+"/" {
		t.Fatalf("got %d %q", w.Code, w.Header().Get("Location"))
	}
	c := sessionCookie(w)
	if c == nil {
		t.Fatal("no goqueue_session cookie")
	}
	if !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteStrictMode || c.Path != testBase {
		t.Errorf("flags: httponly=%v secure=%v samesite=%v path=%q", c.HttpOnly, c.Secure, c.SameSite, c.Path)
	}
	if !strings.Contains(c.Value, ".") {
		t.Errorf("value not expiry.sig: %q", c.Value)
	}
	if d := time.Until(time.Unix(mustExpiry(t, c.Value), 0)); d < 11*time.Hour || d > 12*time.Hour+time.Minute {
		t.Errorf("default TTL off: %v", d)
	}
}

func TestSession_AcceptsHandSignedCookie(t *testing.T) {
	e := newEnv(t, testToken)
	w := e.do("GET", testBase+"/", nil, withCookie(signCookie(testToken, time.Now().Add(time.Hour))))
	if w.Code != http.StatusOK {
		t.Fatalf("got %d, cookie format drifted from brief", w.Code)
	}
}

func TestSession_Rejects(t *testing.T) {
	e := newEnv(t, testToken)
	good := signCookie(testToken, time.Now().Add(time.Hour))
	exp, sig, _ := strings.Cut(good, ".")
	for name, v := range map[string]string{
		"tampered sig":    exp + "." + flip(sig),
		"tampered expiry": "9999999999." + sig,
		"expired":         signCookie(testToken, time.Now().Add(-time.Minute)),
		"old token":       signCookie("old-token", time.Now().Add(time.Hour)),
		"garbage":         "garbage",
		"empty":           "",
	} {
		w := e.do("GET", testBase+"/", nil, withCookie(v))
		if w.Code != http.StatusFound || w.Header().Get("Location") != testBase+"/login" {
			t.Errorf("%s: %d %q, want 302 login", name, w.Code, w.Header().Get("Location"))
		}
	}
}

func TestPost_CrossOrigin_403(t *testing.T) {
	e := newEnv(t, testToken)
	ck := withCookie(sessionCookie(e.login(testToken)).Value)
	for name, m := range map[string]func(*http.Request){
		"foreign origin":  func(r *http.Request) { r.Header.Set("Origin", "http://evil.test") },
		"foreign referer": func(r *http.Request) { r.Header.Set("Referer", "http://evil.test/x") },
		"neither header":  func(*http.Request) {},
	} {
		w := e.do("POST", testBase+"/logout", url.Values{}, ck, m)
		if w.Code != http.StatusForbidden {
			t.Errorf("%s: %d, want 403", name, w.Code)
		}
	}
	w := e.do("POST", testBase+"/logout", url.Values{}, ck,
		func(r *http.Request) { r.Header.Set("Referer", "https://"+testHost+"/goqueue-ui/") })
	if w.Code == http.StatusForbidden {
		t.Errorf("same-host Referer without Origin rejected")
	}
}

func TestLogin_CrossOrigin_403(t *testing.T) {
	e := newEnv(t, testToken)
	w := e.do("POST", testBase+"/login", url.Values{"token": {testToken}},
		func(r *http.Request) { r.Header.Set("Origin", "http://evil.test") })
	if w.Code != http.StatusForbidden || sessionCookie(w) != nil {
		t.Fatalf("got %d", w.Code)
	}
}

func TestLogout_ClearsCookie(t *testing.T) {
	e := newEnv(t, testToken)
	ck := sessionCookie(e.login(testToken)).Value
	w := e.do("POST", testBase+"/logout", url.Values{}, sameOrigin, withCookie(ck))
	if w.Code != http.StatusSeeOther && w.Code != http.StatusFound {
		t.Fatalf("got %d", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != testBase+"/login" {
		t.Errorf("Location = %q", loc)
	}
	c := sessionCookie(w)
	if c == nil || c.Value != "" || (c.MaxAge >= 0 && c.Expires.After(time.Now())) || c.Path != testBase {
		t.Errorf("cookie not cleared: %+v", c)
	}
}

func TestLogin_NeverLogsToken(t *testing.T) {
	e := newEnv(t, testToken)
	const wrong = "wrong-guess-value"
	e.login(wrong)
	w := e.login(testToken)
	ck := sessionCookie(w).Value
	e.do("GET", testBase+"/", nil, withCookie(ck))
	e.do("POST", testBase+"/logout", url.Values{}, sameOrigin, withCookie(ck))
	e.mr.Close()
	if w := e.login(wrong); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("redis down = %d, want 503", w.Code)
	}
	out := e.logs.all()
	if out == "" {
		t.Fatal("logger never called on redis error path")
	}
	for _, secret := range []string{testToken, wrong} {
		if strings.Contains(out, secret) {
			t.Errorf("logs leak %q:\n%s", secret, out)
		}
	}
}

func mustExpiry(t *testing.T, v string) int64 {
	t.Helper()
	exp, _, _ := strings.Cut(v, ".")
	var n int64
	for _, ch := range exp {
		if ch < '0' || ch > '9' {
			t.Fatalf("bad expiry %q", exp)
		}
		n = n*10 + int64(ch-'0')
	}
	return n
}

func flip(s string) string {
	b := []byte(s)
	if b[0] == 'A' {
		b[0] = 'B'
	} else {
		b[0] = 'A'
	}
	return string(b)
}

func TestLogin_Concurrent_AtMost10Get401(t *testing.T) {
	e := newEnv(t, testToken)
	var mu sync.Mutex
	codes := map[int]int{}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := e.login("bad").Code
			mu.Lock()
			codes[c]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if codes[401] != 10 || codes[429] != 10 {
		t.Fatalf("codes = %v, want 10x401 and 10x429", codes)
	}
}

func TestLogin_SuccessClearsCounter(t *testing.T) {
	e := newEnv(t, testToken)
	e.login("bad")
	if w := e.login(testToken); w.Code != http.StatusSeeOther {
		t.Fatalf("got %d", w.Code)
	}
	if e.mr.Exists(failKey) {
		t.Fatal("counter not deleted on success")
	}
}

func clientIPEnv(t *testing.T, ip string) *env {
	e := newEnv(t, testToken)
	h, err := New(Options{BasePath: testBase, AccessToken: testToken, Redis: e.newRedis(t),
		ClientIP: func(*http.Request) string { return ip }})
	if err != nil {
		t.Fatal(err)
	}
	e.h = h
	return e
}

func TestLogin_ClientIPEmptyOrGarbage_FallsBackToRemoteAddr(t *testing.T) {
	for _, ip := range []string{"", "not-an-ip"} {
		e := clientIPEnv(t, ip)
		e.login("bad")
		if v, _ := e.mr.Get(failKey); v != "1" {
			t.Errorf("ip %q: remote-addr key counter = %q", ip, v)
		}
	}
}

func TestLogin_IPv6MaskedTo64(t *testing.T) {
	e := clientIPEnv(t, "2001:db8:1:2:aaaa:bbbb:cccc:dddd")
	e.login("bad")
	e.h, _ = New(Options{BasePath: testBase, AccessToken: testToken, Redis: e.newRedis(t),
		ClientIP: func(*http.Request) string { return "2001:db8:1:2:1:2:3:4" }})
	e.login("bad")
	if v, _ := e.mr.Get("goqueue:ui:login_fail:2001:db8:1:2::"); v != "2" {
		t.Fatalf("/64 counter = %q, keys=%v", v, e.mr.Keys())
	}
}

func TestLogin_BadContentTypeAndOversize(t *testing.T) {
	e := newEnv(t, testToken)
	w := e.do("POST", testBase+"/login", url.Values{"token": {testToken}}, sameOrigin,
		func(r *http.Request) { r.Header.Set("Content-Type", "application/json") })
	if w.Code != http.StatusUnsupportedMediaType || sessionCookie(w) != nil {
		t.Errorf("json = %d", w.Code)
	}
	big := url.Values{"token": {strings.Repeat("a", 8<<10)}}
	if w := e.do("POST", testBase+"/login", big, sameOrigin); w.Code == http.StatusSeeOther {
		t.Errorf("oversize body accepted")
	}
}

func TestSession_RejectsExpiryBeyondTTL(t *testing.T) {
	e := newEnv(t, testToken)
	far := signCookie(testToken, time.Now().Add(100*24*time.Hour))
	w := e.do("GET", testBase+"/", nil, withCookie(far))
	if w.Code != http.StatusFound {
		t.Fatalf("far-future cookie = %d, want 302", w.Code)
	}
}
