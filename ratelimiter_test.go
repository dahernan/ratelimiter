package ratelimiter

import (
	"errors"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/matryer/is"
)

var (
	pool             *redis.Pool
	ErrLimitExceeded = errors.New("Limit has been exceeded")
)

func init() {
	pool = newPool(":6379")
}

func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 5 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

// conn := pool.Get()
//     defer conn.Close()

// function type to test the rate limit
type runFunc func() error

// for example runTimes(10, 40, fn )
// runs fn() 10 times/sec, until 40 fn executions
// returns the number of times that executes fn()
// an error if fn() gives an error
func runTimes(ratePerSec int, limit int, fn runFunc) (int, error) {
	var frequency time.Duration

	frequency = time.Duration(1e9 / ratePerSec)
	throttle := time.Tick(frequency)

	for i := 0; i < limit; i++ {
		<-throttle
		err := fn()
		if err != nil {
			return i, err
		}
	}
	return limit, nil
}

func Test20TimesPerSecondShouldExceedTheLimit(t *testing.T) {
	is := is.New(t)
	// 20 times / sec
	// for 2 seconds
	runs, err := runTimes(20, 40, func() error {
		conn := pool.Get()
		exceeded, _, err := LimitExceeded(conn, "10times_secondExceed", 1*time.Second, 10)
		if exceeded {
			return ErrLimitExceeded
		}
		return err
	})

	is.Equal(err, ErrLimitExceeded)
	is.Equal(runs, 11)

}

func Test5TimesPerSecondShouldRespectTheLimit(t *testing.T) {
	is := is.New(t)
	// 5 times / sec
	// for 1 second
	runs, err := runTimes(5, 10, func() error {
		conn := pool.Get()
		exceeded, _, err := LimitExceeded(conn, "10times_secondOk", 1*time.Second, 10)
		if exceeded {
			return ErrLimitExceeded
		}
		return err
	})
	is.NoErr(err)
	is.Equal(runs, 10)

}

func Test5TimesPer3SecondsShouldRespectTheLimit(t *testing.T) {
	is := is.New(t)
	// 5 times / sec
	// for 3 seconds
	runs, err := runTimes(5, 15, func() error {
		conn := pool.Get()
		exceeded, _, err := LimitExceeded(conn, "5times_3secondOk", 2*time.Second, 10)
		if exceeded {
			return ErrLimitExceeded
		}
		return err
	})
	is.NoErr(err)
	is.Equal(runs, 15)

}

func Test6TimesPer3SecondsShouldExceedTheLimit(t *testing.T) {
	is := is.New(t)
	// 6 times / sec
	// for 3 seconds
	runs, err := runTimes(6, 18, func() error {
		conn := pool.Get()
		exceeded, _, err := LimitExceeded(conn, "6times_3secondExceed", 2*time.Second, 10)
		if exceeded {
			return ErrLimitExceeded
		}
		return err
	})
	is.Equal(err, ErrLimitExceeded)
	is.Equal(runs, 11)

}

func TestRetryAfterLimitExceedShouldAlsoFail(t *testing.T) {
	is := is.New(t)
	// 20 times / sec
	// for 2 seconds
	runs, err := runTimes(20, 40, func() error {
		conn := pool.Get()
		exceeded, _, err := LimitExceeded(conn, "20times_2secondRetryExceed", 1*time.Second, 10)
		if exceeded {
			return ErrLimitExceeded
		}
		return err
	})
	is.Equal(err, ErrLimitExceeded)
	is.Equal(runs, 11)

	// Retry 1
	conn := pool.Get()
	exceeded, _, err := LimitExceeded(conn, "20times_2secondRetryExceed", 1*time.Second, 10)
	is.Equal(exceeded, true)
	is.NoErr(err)

	// Retry 2
	conn = pool.Get()
	exceeded, _, err = LimitExceeded(conn, "20times_2secondRetryExceed", 1*time.Second, 10)
	is.Equal(exceeded, true)
	is.NoErr(err)

}

func TestRetryAfterLimitExceedShouldSuccessIfYouWaitEnough(t *testing.T) {
	is := is.New(t)
	// 20 times / sec
	// for 2 seconds
	runs, err := runTimes(20, 40, func() error {
		conn := pool.Get()
		exceeded, _, err := LimitExceeded(conn, "20times_2secondRetryExceedSuccess", 1*time.Second, 10)
		if exceeded {
			return ErrLimitExceeded
		}
		return err
	})
	is.Equal(err, ErrLimitExceeded)
	is.Equal(runs, 11)

	// Retry 1 Fail
	conn := pool.Get()
	exceeded, _, err := LimitExceeded(conn, "20times_2secondRetryExceedSuccess", 1*time.Second, 10)
	is.Equal(exceeded, true)
	is.NoErr(err)

	// Wait
	time.Sleep(1 * time.Second)

	// Retry 2 Sucess
	conn = pool.Get()
	exceeded, _, err = LimitExceeded(conn, "20times_2secondRetryExceedSuccess", 1*time.Second, 10)
	is.Equal(exceeded, false)
	is.NoErr(err)

}
