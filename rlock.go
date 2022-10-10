package rebounds

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	_ RLock = new(BaseLock)
)

//go:generate stringer -type=LockType
type LockType int

const (
	BASE LockType = iota + 1

	_defaultLeaseTime = 30
)

var (
	unlockScript = redis.NewScript(
		`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`)

	increaseScript = redis.NewScript(
		`
if redis.call("GET", KEYS[1]) == ARGV[2] then
    local currentExpire = redis.call("PTTL", KEYS[1])
    return redis.call("PEXPIRE", KEYS[1], ARGV[1]+currentExpire)
else
    return 0
end
`)

	renewScript = redis.NewScript(
		`
if redis.call("GET", KEYS[1]) == ARGV[2] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[1])
else
    return 0
end
`)

	ErrOverTime          = errors.New("blocking lock wait over time")
	ErrWrongType         = errors.New("wrong type")
	ErrWrongLockType     = errors.New("wrong lock type")
	ErrUnknownLockType   = errors.New("unknown lock type")
	ErrUnknownActionType = errors.New("unknown action type")
)

// RLock Redis based implementation Go sync.lock
type RLock interface {
	sync.Locker

	ID() string
	Type() string

	LockWithDefaultLeaseTime() (bool, error)
	LockWithLeaseTime(leaseTime float64, leaseUnit time.Duration) (bool, error)
	TryLock(leaseTime float64, leaseUnit time.Duration, waitTime float64, waitUnit time.Duration, retryCnt int) (bool, error)
	ForceUnlock() (bool, error)
	ReleaseLock() (bool, error)

	TTL() (time.Duration, error)
	RenewTTL(renewTime float64, renewUnit time.Duration) (bool, error)
	IncreaseTTL(increaseTime float64, increaseUnit time.Duration) (bool, error)
}

type BaseLock struct {
	redis.Client

	id       string
	uuid     string
	lockType LockType
}

func (b *BaseLock) Lock() {
	b.LockWithDefaultLeaseTime()
}

func (b *BaseLock) Unlock() {
	b.ReleaseLock()
}

func (b *BaseLock) ID() string {
	return b.id
}

func (b *BaseLock) Type() string {
	return b.lockType.String()
}

func (b *BaseLock) LockWithDefaultLeaseTime() (bool, error) {
	return b.LockWithLeaseTime(_defaultLeaseTime, time.Second)
}

func (b *BaseLock) LockWithLeaseTime(leaseTime float64, leaseUnit time.Duration) (bool, error) {
	return b.SetNX(b.Context(), b.id, b.uuid, time.Duration(leaseTime)*leaseUnit).Result()
}

func (b *BaseLock) TryLock(leaseTime float64, leaseUnit time.Duration, waitTime float64, waitUnit time.Duration, retryCnt int) (bool, error) {
	ctx, cancel := context.WithDeadline(b.Context(), time.Now().Add(time.Duration(waitTime)*waitUnit*time.Duration(retryCnt+1)).Add(1*time.Second))
	defer cancel()
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	ticker := time.NewTicker(time.Duration(waitTime) * waitUnit)
	for i := 0; i <= retryCnt; i++ {
	LOOP:
		select {
		case <-ctx.Done():
			return false, ErrOverTime
		default:
			retry := LinearBackoff(100 * time.Millisecond)
			for {
				flag, err := b.LockWithLeaseTime(leaseTime, leaseUnit)
				if err != nil {
					return false, err
				}
				if flag {
					return true, nil
				}
				backoff := retry.NextBackoff()
				if timer == nil {
					timer = time.NewTimer(backoff)
				} else {
					timer.Reset(backoff)
				}
				select {
				case <-ctx.Done():
					return false, ErrOverTime
				case <-timer.C:
				case <-ticker.C:
					break LOOP
				}
			}
		}
	}
	return false, nil
}

func (b *BaseLock) ForceUnlock() (bool, error) {
	result, err := b.Del(b.Context(), b.id).Result()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (b *BaseLock) ReleaseLock() (bool, error) {
	keys := []string{
		b.id,
	}
	result, err := unlockScript.Run(b.Context(), b, keys, b.uuid).Result()
	if err != nil {
		return false, err
	}
	if flag, ok := result.(int64); ok {
		return flag == 1, nil
	} else {
		return false, ErrWrongType
	}
}

func (b *BaseLock) TTL() (time.Duration, error) {
	return b.PTTL(b.Context(), b.id).Result()
}

func (b *BaseLock) RenewTTL(renewTime float64, renewUnit time.Duration) (bool, error) {
	keys := []string{
		b.id,
	}
	result, err := renewScript.Run(b.Context(), b, keys, (time.Duration(renewTime) * renewUnit).Milliseconds(), b.uuid).Result()
	if err != nil {
		return false, err
	}
	if flag, ok := result.(int64); ok {
		return flag == 1, nil
	} else {
		return false, ErrWrongType
	}
}

func (b *BaseLock) IncreaseTTL(increaseTime float64, increaseUnit time.Duration) (bool, error) {
	keys := []string{
		b.id,
	}
	result, err := increaseScript.Run(b.Context(), b, keys, (time.Duration(increaseTime) * increaseUnit).Milliseconds(), b.uuid).Result()
	if err != nil {
		return false, err
	}
	if flag, ok := result.(int64); ok {
		return flag == 1, nil
	} else {
		return false, ErrWrongType
	}
}
