package dashboard

import (
	"context"
	"regexp"
	"strings"
	"testing"
)

func TestLoginPage_NoInlineAndPublicCSS(t *testing.T) {
	e := newEnv(t, testToken)
	body := get(e, "/login").Body.String()
	if strings.Contains(body, "<style") || strings.Contains(body, "<script") {
		t.Error("login page has inline <style>/<script>, blocked by CSP")
	}
	if !strings.Contains(body, `href="`+testBase+`/static/login.css"`) {
		t.Error("login page does not reference login.css")
	}
	w := get(e, "/static/login.css")
	if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/css") {
		t.Errorf("unauth login.css = %d %q, want 200 text/css", w.Code, w.Header().Get("Content-Type"))
	}
	if w = get(e, "/static/app.css"); w.Code != 302 {
		t.Errorf("unauth app.css = %d, want 302", w.Code)
	}
}

var (
	scriptTag   = regexp.MustCompile(`<script\b[^>]*>`)
	inlineStyle = regexp.MustCompile(`<[^>]*\sstyle\s*=`)
	inlineEvent = regexp.MustCompile(`<[^>]*\son[a-z]+\s*=`)
)

func TestPages_NoInlineScriptOrStyle(t *testing.T) {
	e := newEnv(t, testToken)
	seedQueue(t, e.newRedis(t), "alpha", 2, 2, 2, 2)
	ck := authed(t, e)
	pages := []string{"/login", "/", "/analytics", "/queues", "/queues/alpha", "/retries", "/retries?queue=alpha",
		"/dead?queue=alpha", "/deleted?queue=alpha", "/busy"}
	for _, p := range pages {
		body := get(e, p, ck).Body.String()
		for _, tag := range scriptTag.FindAllString(body, -1) {
			if !strings.Contains(tag, " src=") {
				t.Errorf("%s: inline script %q", p, tag)
			}
		}
		if strings.Contains(body, "<style") || inlineStyle.MatchString(body) || inlineEvent.MatchString(body) {
			t.Errorf("%s: inline style or event handler present", p)
		}
	}
}

func TestPages_PerCapAndDaysFallback(t *testing.T) {
	e := newEnv(t, testToken)
	seedQueue(t, e.newRedis(t), "alpha", 0, 150, 0, 0)
	ck := authed(t, e)
	body := get(e, "/retries?queue=alpha&per=500", ck).Body.String()
	if n := strings.Count(body, `<input type="checkbox" value=`); n != 100 {
		t.Errorf("per=500 rendered %d rows, want 100", n)
	}
	if !strings.Contains(body, "Page 1 of 2") {
		t.Error("per=500 pager not computed with per=100")
	}
	body = get(e, "/analytics?days=5", ck).Body.String()
	if !strings.Contains(body, `href="?days=30" class="on"`) || strings.Count(body, "<tr><td>") != 30 {
		t.Error("days=5 did not fall back to 30")
	}
}

func TestPages_HugePageClampedToLast(t *testing.T) {
	e := newEnv(t, testToken)
	rc := e.newRedis(t)
	seedQueue(t, rc, "alpha", 0, 30, 0, 0)
	last, _, err := newReader(rc).Jobs(context.Background(), "alpha", SourceRetry, 2, 25)
	if err != nil || len(last) == 0 {
		t.Fatalf("reader: %v", err)
	}
	w := get(e, "/retries?queue=alpha&page=9223372036854775807", authed(t, e))
	body := w.Body.String()
	if w.Code != 200 || !strings.Contains(body, "Page 2 of 2") || !strings.Contains(body, `value="`+last[0].ID+`"`) {
		t.Errorf("huge page: code %d, want last page (2 of 2) with %q", w.Code, last[0].ID)
	}
	if strings.Contains(body, "Next &rarr;") || strings.Contains(body, "Next →") {
		t.Error("last page still offers Next")
	}
}

func TestSubNav_MarksCurrentSource(t *testing.T) {
	e := newEnv(t, testToken)
	seedQueue(t, e.newRedis(t), "alpha", 1, 1, 1, 1)
	ck := authed(t, e)
	cases := map[string]string{
		"/queues/alpha":        testBase + `/queues/alpha"`,
		"/retries?queue=alpha": testBase + `/retries?queue=alpha"`,
		"/dead?queue=alpha":    testBase + `/dead?queue=alpha"`,
		"/deleted?queue=alpha": testBase + `/deleted?queue=alpha"`,
	}
	for p, href := range cases {
		body := get(e, p, ck).Body.String()
		if !strings.Contains(body, href+` class="on"`) {
			t.Errorf("%s: current sub-nav link not marked on", p)
		}
		if strings.Count(body, `class="on" aria-current="page"`) != 2 { // top nav + sub nav
			t.Errorf("%s: want exactly one current sub-nav link", p)
		}
	}
}
