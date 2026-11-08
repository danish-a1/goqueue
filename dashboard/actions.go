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

// ActionResult lists the IDs moved and the IDs no longer at the source, in request order.
type ActionResult struct{ Done, NotFound []string }

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
	t, err := a.target(ctx, act, queue, src, actorIP)
	if err != nil {
		return ActionResult{}, err
	}
	members, err := a.resolve(ctx, t, jobIDs)
	if err != nil {
		return ActionResult{}, err
	}
	var res ActionResult
	for i, id := range jobIDs {
		ok, err := a.move(ctx, t, id, members[i])
		if err != nil {
			return res, err
		}
		if ok {
			res.Done = append(res.Done, id)
		} else {
			res.NotFound = append(res.NotFound, id)
		}
	}
	return res, nil
}

// target validates the queue against the registry and picks source and destination keys.
func (a *actions) target(ctx context.Context, act Action, queue string, src Source, actorIP string) (target, error) {
	deadKey, err := a.rdb.HGet(ctx, gqredis.KeyQueues, queue).Result()
	if errors.Is(err, redis.Nil) {
		// A restore only touches the deleted list and pending; an existing deleted list is
		// enough proof the queue exists even if the registry entry is missing.
		if act != ActionRestore {
			return target{}, badRequest("unknown queue %q", queue)
		}
		n, err := a.rdb.Exists(ctx, gqredis.KeyDeleted(queue)).Result()
		if err != nil {
			return target{}, err
		}
		if n == 0 {
			return target{}, badRequest("unknown queue %q", queue)
		}
	} else if err != nil {
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
// rebuilt (malformed, or no job_name so no worker could decode it) counts as not found.
func (a *actions) move(ctx context.Context, t target, id, member string) (bool, error) {
	if member == "" {
		return false, nil
	}
	dst, ok := t.destination(id, member)
	if !ok {
		return false, nil
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
		return false, fmt.Errorf("dashboard: %s %s: %w", t.act, id, err)
	}
	return n == 1, nil
}
