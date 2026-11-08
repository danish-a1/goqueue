package dashboard

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/danish-a1/goqueue/adapter/dlq"
	gqredis "github.com/danish-a1/goqueue/adapter/redis"
)

// Action is a dashboard mutation applied to one or more jobs.
type Action string

const (
	ActionRetry   Action = "retry"
	ActionKill    Action = "kill"
	ActionDelete  Action = "delete"
	ActionRestore Action = "restore"
)

// maxActionIDs caps one batch so a single click stays a bounded amount of Redis work.
const maxActionIDs = 100

// ErrBadRequest marks caller errors (bad action/source pair, ID count, unknown queue, no dead list).
var ErrBadRequest = errors.New("dashboard: bad request")

// allowedSources is the action/source matrix; anything else is ErrBadRequest.
var allowedSources = map[Action][]Source{
	ActionRetry:   {SourceRetry, SourceDead},
	ActionKill:    {SourcePending, SourceRetry},
	ActionDelete:  {SourcePending, SourceRetry, SourceDead},
	ActionRestore: {SourceDeleted},
}

// ActionResult buckets the requested IDs, in request order: Done were moved, NotFound
// are no longer at the source, Skipped are still at the source but cannot be moved
// (no job name, so no worker could decode a requeued job).
type ActionResult struct{ Done, NotFound, Skipped []string }

type actions struct{ rdb redis.UniversalClient }

func newActions(rdb redis.UniversalClient) *actions { return &actions{rdb: rdb} }

// target is one resolved call: where members come from and where they go.
type target struct {
	act                Action
	queue, srcKey      string
	src                Source
	dstKey, deadKey    string
	srcZSet, toPending bool
	actorIP            string
	now                time.Time
}

// outcome is the ActionResult bucket one ID lands in.
type outcome int

const (
	moved outcome = iota
	gone
	skipped
)

// dedupe drops repeated IDs, keeping first-seen order, so no ID lands in two buckets.
func dedupe(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

func badRequest(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrBadRequest}, args...)...)
}

// Do applies act to jobIDs of queue in src. Each ID is one atomic Lua move: the exact
// member is removed from src and only then written to the destination, so a job is never
// in two places or lost. IDs already gone (popped, moved, unknown) land in NotFound. On a
// Redis error the partial result so far is returned with the error.
func (a *actions) Do(ctx context.Context, act Action, queue string, src Source, jobIDs []string, actorIP string) (ActionResult, error) {
	if !slices.Contains(allowedSources[act], src) {
		return ActionResult{}, badRequest("action %q not allowed from %q", act, src)
	}
	if len(jobIDs) == 0 || len(jobIDs) > maxActionIDs {
		return ActionResult{}, badRequest("need 1..%d job ids, got %d", maxActionIDs, len(jobIDs))
	}
	jobIDs = dedupe(jobIDs)
	t, err := a.target(ctx, act, queue, src, actorIP)
	if err != nil {
		return ActionResult{}, err
	}
	members, err := a.resolve(ctx, t, jobIDs)
	if err != nil {
		return ActionResult{}, err
	}
	var res ActionResult
	buckets := map[outcome]*[]string{moved: &res.Done, gone: &res.NotFound, skipped: &res.Skipped}
	for i, id := range jobIDs {
		o, err := a.move(ctx, t, id, members[i])
		if err != nil {
			return res, err
		}
		*buckets[o] = append(*buckets[o], id)
	}
	return res, nil
}

// target validates the queue against the registry and picks source and destination keys.
func (a *actions) target(ctx context.Context, act Action, queue string, src Source, actorIP string) (target, error) {
	deadKey, err := a.rdb.HGet(ctx, gqredis.KeyQueues, queue).Result()
	if errors.Is(err, redis.Nil) {
		return target{}, badRequest("unknown queue %q", queue)
	}
	if err != nil {
		return target{}, err
	}
	if deadKey == "" && (act == ActionKill || src == SourceDead) {
		return target{}, badRequest("queue %q has no Redis dead list", queue)
	}
	t := target{act: act, queue: queue, src: src, deadKey: deadKey, actorIP: actorIP, now: time.Now().UTC()}
	switch src {
	case SourcePending:
		t.srcKey = gqredis.KeyPending(queue)
	case SourceRetry:
		t.srcKey, t.srcZSet = gqredis.KeyRetry(queue), true
	case SourceDead:
		t.srcKey = deadKey
	case SourceDeleted:
		t.srcKey = gqredis.KeyDeleted(queue)
	}
	switch act {
	case ActionRetry, ActionRestore:
		t.dstKey, t.toPending = gqredis.KeyPending(queue), true
	case ActionKill:
		t.dstKey = deadKey
	case ActionDelete:
		t.dstKey = gqredis.KeyDeleted(queue)
	}
	return t, nil
}

// move builds the destination member and runs the atomic move. A member that cannot be
// rebuilt (malformed, or no job_name so no worker could decode it) stays put as skipped.
func (a *actions) move(ctx context.Context, t target, id, member string) (outcome, error) {
	if member == "" {
		return gone, nil
	}
	dst, ok := t.destination(id, member)
	if !ok {
		return skipped, nil
	}
	srcKind, dstKind := "list", "archive"
	if t.srcZSet {
		srcKind = "zset"
	}
	if t.toPending {
		dstKind = "pending"
	}
	n, err := moveScript.Run(ctx, a.rdb, []string{t.srcKey, t.dstKey, gqredis.KeyIndex(t.queue)},
		srcKind, member, dst, dstKind, id, dlq.MaxEntries, int64(dlq.EntryTTL/time.Second)).Int64()
	if err != nil {
		return gone, fmt.Errorf("dashboard: %s %s: %w", t.act, id, err)
	}
	if n != 1 {
		return gone, nil
	}
	return moved, nil
}
