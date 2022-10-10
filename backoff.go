package rebounds

import (
	"math"
	"math/rand"
	"sync/atomic"
	"time"
)

var (
	_ RetryStrategy = new(limitedRetry)
	_ RetryStrategy = new(exponentialBackoff)
	_ RetryStrategy = new(linearBackoff)
)

// RetryStrategy 重试策略
type RetryStrategy interface {
	NextBackoff() time.Duration
}

type linearBackoff time.Duration

// LinearBackoff 线性重试策略
func LinearBackoff(backoff time.Duration) RetryStrategy {
	return linearBackoff(backoff)
}

// NoRetry 无间隔重试策略
func NoRetry() RetryStrategy {
	return linearBackoff(0)
}

func (r linearBackoff) NextBackoff() time.Duration {
	return time.Duration(r)
}

type limitedRetry struct {
	s   RetryStrategy
	cnt int64
	max int64
}

// LimitRetry 限制次数重试策略
func LimitRetry(s RetryStrategy, max int) RetryStrategy {
	return &limitedRetry{s: s, max: int64(max)}
}

func (r *limitedRetry) NextBackoff() time.Duration {
	if atomic.LoadInt64(&r.cnt) >= r.max {
		return 0
	}
	atomic.AddInt64(&r.cnt, 1)
	return r.s.NextBackoff()
}

type exponentialBackoff struct {
	cnt uint64

	min, max time.Duration
}

// ExponentialBackoff 指数级间隔时间重试策略
func ExponentialBackoff(min, max time.Duration) RetryStrategy {
	return &exponentialBackoff{min: min, max: max}
}

func (r *exponentialBackoff) NextBackoff() time.Duration {
	cnt := atomic.AddUint64(&r.cnt, 1)
	if d := time.Duration(math.Min(float64(r.min)*math.Pow(1.5, float64(cnt)), float64(r.max)) + float64(rand.Int63n(3))); d < r.min {
		return r.min
	} else if r.max != 0 && d > r.max {
		return r.max
	} else {
		return d
	}
}
