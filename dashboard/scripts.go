package dashboard

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/move.lua
var moveLua string

// moveScript runs via EVALSHA, falling back to EVAL on NOSCRIPT.
var moveScript = redis.NewScript(moveLua)
