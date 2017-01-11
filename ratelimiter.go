package ratelimiter

import (
	"fmt"
	"strconv"
	"time"

	redigo "github.com/garyburd/redigo/redis"
)

// LimitExceeded checks for the rate limit for a given Redis key.
//
// Returns true if limit is exceeded, false othewise
// also returns false, if an error is not nil to prevent bad checkings.
// Returns an error if there is a problem using redis commands
func LimitExceeded(conn redigo.Conn, key string, d time.Duration, limitOfCalls int) (bool, int64, error) {
	defer conn.Close()
	currentCalls, err := numberOfCalls(conn, key, d)
	if err != nil {
		return false, 0, err
	}

	if currentCalls > int64(limitOfCalls) {
		return true, currentCalls, nil
	}
	return false, currentCalls, nil
}

// Modifies the Redis counter of number of calls, and gets the currenct number of calls
func numberOfCalls(conn redigo.Conn, key string, d time.Duration) (int64, error) {
	var currentCalls int64

	now := time.Now()
	expired := now.Add(-d)

	expiredStr := strconv.FormatInt(expired.UnixNano(), 10)
	nowStr := strconv.FormatInt(now.UnixNano(), 10)

	// Atomic commands, prevents race codition in Redis
	conn.Send("MULTI")

	// Leaky bucket implemented with a sorted set
	// r[0] REM past events
	conn.Send("ZREMRANGEBYSCORE", key, "0", expiredStr)
	// r[1] COUNT all elements
	conn.Send("ZCOUNT", key, "0", nowStr)
	// r[2] ADD now now
	conn.Send("ZADD", key, nowStr, nowStr)
	// r[3] EXPIRE X in seconds
	ttl := fmt.Sprintf("%.0f", d.Seconds())
	conn.Send("EXPIRE", key, ttl)

	r, err := redigo.Values(conn.Do("EXEC"))
	if err != nil {
		return currentCalls, err
	}

	// Debug the result
	// fmt.Printf("[0] ZREM   %v  %T\n", r[0], r[0])
	// fmt.Printf("[1] ZCOUNT %v  %T\n", r[1], r[1])
	// fmt.Printf("[2] ZADD   %v  %T\n", r[2], r[2])
	// fmt.Printf("[3] EXPIRE %v  %T\n", r[3], r[3])

	// r[1] COUNT
	currentCalls = r[1].(int64)

	return currentCalls, nil
}
