package rebounds

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetry(t *testing.T) {
	a := assert.New(t)
	t.Run("LinearBackoff", func(t *testing.T) {
		retry := LinearBackoff(2 * time.Second)
		a.Equal(2*time.Second, retry.NextBackoff(), "next backoff shoud be 2 seconds")
	})
	t.Run("NoRetry", func(t *testing.T) {
		retry := NoRetry()
		a.Equal(0*time.Second, retry.NextBackoff(), "next backoff shoud be 0 seconds")
	})
	t.Run("LimitRetry", func(t *testing.T) {
		retry := LimitRetry(LinearBackoff(2*time.Second), 2)
		firstTime := retry.NextBackoff()
		secondTime := retry.NextBackoff()
		thirdTime := retry.NextBackoff()
		a.Equal(2*time.Second, firstTime, "first backoff shoud be 2 seconds")
		a.Equal(2*time.Second, secondTime, "second backoff shoud be 2 seconds")
		a.Equal(0*time.Second, thirdTime, "third bakoff shoud be 0 seconds")
	})
	t.Run("ExponentialBackoff", func(t *testing.T) {
		retry := ExponentialBackoff(1*time.Second, 10*time.Second)
		firstTime := retry.NextBackoff()
		secondTime := retry.NextBackoff()
		thirdTime := retry.NextBackoff()
		a.GreaterOrEqual(firstTime, 1*time.Second, "first backoff shoud be greater or equal 1 seconds")
		a.GreaterOrEqual(secondTime, 1*time.Second, "second backoff shoud be greater or equal 1 seconds")
		a.GreaterOrEqual(thirdTime, 1*time.Second, "third backoff shoud be greater or equal 1 seconds")
		a.LessOrEqual(firstTime, 10*time.Second, "first backoff shoud be less or equal 3 seconds")
		a.LessOrEqual(secondTime, 10*time.Second, "second backoff shoud be less or equal 3 seconds")
		a.LessOrEqual(thirdTime, 10*time.Second, "third backoff shoud be less or equal 3 seconds")
		t.Logf("first time is %+v", firstTime)
		t.Logf("second time is %+v", secondTime)
		t.Logf("third time is %+v", thirdTime)
	})
}
