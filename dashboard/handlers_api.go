package dashboard

import (
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"strconv"
)

// actionRequest is the POST /api/actions body.
type actionRequest struct {
	Action Action   `json:"action"`
	Queue  string   `json:"queue"`
	Source Source   `json:"source"`
	JobIDs []string `json:"job_ids"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// apiStats returns the Summary for the stat bar poll.
func (s *server) apiStats(w http.ResponseWriter, r *http.Request) {
	sum, err := s.summary(r.Context())
	if err != nil {
		s.Logger("dashboard stats failed", "err", err)
		jsonError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

// apiHistory returns []DayStat for ?days=7|30|90|180 (default 30), oldest first.
func (s *server) apiHistory(w http.ResponseWriter, r *http.Request) {
	days, err := s.rd.History(r.Context(), daysParam(r))
	if err != nil {
		s.Logger("dashboard history failed", "err", err)
		jsonError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, days)
}

// apiActions applies {"action","queue","source","job_ids"} and returns the ActionResult.
// Caller errors are 400 with the reason; Redis errors are a bare 500.
func (s *server) apiActions(w http.ResponseWriter, r *http.Request) {
	if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/json" {
		jsonError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	var req actionRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(&req)
	if err == nil && dec.More() {
		err = errors.New("trailing data")
	}
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		jsonError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	case err != nil:
		jsonError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	ip := s.throttleIP(r)
	res, err := s.act.Do(r.Context(), req.Action, req.Queue, req.Source, req.JobIDs, ip)
	switch {
	case errors.Is(err, ErrBadRequest):
		jsonError(w, http.StatusBadRequest, err.Error())
	case err != nil:
		s.Logger("dashboard action failed", "action", req.Action, "queue", req.Queue, "done", len(res.Done), "err", err)
		jsonError(w, http.StatusInternalServerError, "internal error")
	default:
		// Audit line: counts only, never job IDs or payloads. Queue is quoted so a crafted
		// name cannot forge log lines.
		s.Logger("dashboard action", "action", req.Action, "queue", strconv.Quote(req.Queue), "source", req.Source,
			"done", len(res.Done), "not_found", len(res.NotFound), "skipped", len(res.Skipped), "ip", ip)
		writeJSON(w, http.StatusOK, res)
	}
}
