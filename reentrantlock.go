package rebounds

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	_ RLock = new(ReentrantLock)

	rlockScript = redis.NewScript(
		`
if (redis.call("EXISTS", KEYS[1]) == 0) then
    redis.call("HINCRBY", KEYS[1], ARGV[2], 1)
    redis.call("PEXPIRE", KEYS[1], ARGV[1])
    return 1
end
if (redis.call("HEXISTS", KEYS[1], ARGV[2]) == 1) then
    redis.call("HINCRBY", KEYS[1], ARGV[2], 1)
    return 1
end
return 0
`)

	unrlockScript = redis.NewScript(
		`
if (redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0) then
    return 0
end
local counter = redis.call("HINCRBY", KEYS[1], ARGV[1], -1)
if (counter > 0) then
    return 1
else
    redis.call("DEL", KEYS[1])
    return 1
end
return 0
`)

	rrenewScript = redis.NewScript(
		`
if (redis.call("HEXISTS", KEYS[1], ARGV[2]) == 1) then
    return redis.call("PEXPIRE", KEYS[1], ARGV[1])
else
    return 0
end
`)

	rincreaseScript = redis.NewScript(
		`
if (redis.call("HEXISTS", KEYS[1], ARGV[2]) == 1) then
    local currentExpire = redis.call("PTTL", KEYS[1])
    return redis.call("PEXPIRE", KEYS[1], currentExpire + ARGV[1])
else
    return 0
end
`)
)

type ReentrantLock struct {
	redis.Client

	id       string
	uuid     string
	lockType LockType
}

func (r *ReentrantLock) Lock() {
	r.LockWithDefaultLeaseTime()
}

func (r *ReentrantLock) Unlock() {
	r.ReleaseLock()
}

func (r *ReentrantLock) ID() string {
	return r.id
}

func (r *ReentrantLock) Type() string {
	return r.lockType.String()
}

func (r *ReentrantLock) LockWithDefaultLeaseTime() (bool, error) {
	return r.LockWithLeaseTime(_defaultLeaseTime, time.Second)
}

func (r *ReentrantLock) LockWithLeaseTime(leaseTime float64, leaseUnit time.Duration) (bool, error) {
	keys := []string{
		r.id,
	}
	result, err := rlockScript.Run(r.Context(), r, keys, (30 * time.Second).Milliseconds(), r.uuid).Result()
	if err != nil {
		return false, err
	}
	if flag, ok := result.(int64); ok {
		return flag == 1, nil
	} else {
		return false, ErrWrongType
	}
}

func (r *ReentrantLock) TryLock(leaseTime float64, leaseUnit time.Duration, waitTime float64, waitUnit time.Duration, retryCnt int) (bool, error) {
	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(time.Duration(waitTime)*waitUnit*time.Duration(retryCnt+1)).Add(1*time.Second))
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
				flag, err := r.LockWithLeaseTime(leaseTime, leaseUnit)
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

func (r *ReentrantLock) ForceUnlock() (bool, error) {
	result, err := r.Del(r.Context(), r.id).Result()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (r *ReentrantLock) ReleaseLock() (bool, error) {
	keys := []string{
		r.id,
	}
	result, err := unrlockScript.Run(r.Context(), r, keys, r.uuid).Result()
	if err != nil {
		return false, err
	}
	if flag, ok := result.(int64); ok {
		return flag == 1, nil
	} else {
		return false, ErrWrongType
	}
}

func (r *ReentrantLock) TTL() (time.Duration, error) {
	return r.PTTL(r.Context(), r.id).Result()
}

func (r *ReentrantLock) RenewTTL(renewTime float64, renewUnit time.Duration) (bool, error) {
	keys := []string{
		r.id,
	}
	result, err := rrenewScript.Run(r.Context(), r, keys, (time.Duration(renewTime) * renewUnit).Milliseconds(), r.uuid).Result()
	if err != nil {
		return false, err
	}
	if flag, ok := result.(int64); ok {
		return flag == 1, nil
	} else {
		return false, ErrWrongType
	}
}

func (r *ReentrantLock) IncreaseTTL(increaseTime float64, increaseUnit time.Duration) (bool, error) {
	keys := []string{
		r.id,
	}
	result, err := rincreaseScript.Run(r.Context(), r, keys, (time.Duration(increaseTime) * increaseUnit).Milliseconds(), r.uuid).Result()
	if err != nil {
		return false, err
	}
	if flag, ok := result.(int64); ok {
		return flag == 1, nil
	} else {
		return false, ErrWrongType
	}
}
