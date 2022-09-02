package nodes

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ash2k/stager/wait"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/tilinna/clock"

	"github.com/atlassian/gostatsd/internal/fixtures"
)

func TestRedisNodeTrackerSelf(t *testing.T) {
	t.Parallel()

	ctxTest, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()

	// We use a time in the future, because the redis client uses ctx.Deadline(),
	// which returns the mock time, but then uses it in computation with real time.
	clck := clock.NewMock(time.Unix(2147480000, 0))
	ctxClock := clock.Context(ctxTest, clck)

	mr := miniredis.RunT(t)

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
		DB:   0,
	})
	defer redisClient.Close()

	ctxRunner, cancel := context.WithCancel(ctxClock)

	var addCalled atomic.Bool
	removeCalled := false
	rnt := NewRedisNodeTracker(
		logrus.New(),
		&fixtures.MockNodePicker{TB: t,
			FnAdd: func(node string) {
				assert.Equal(t, "me", node)
				addCalled.Store(true)
			},
			FnRemove: func(node string) {
				assert.Equal(t, "me", node)
				removeCalled = true
			},
		},
		redisClient,
		"foo",
		"me",
		1*time.Second,
		2*time.Second,
	)
	var wg wait.Group
	wg.StartWithContext(ctxRunner, rnt.Run)

	fixtures.EnsureAttachedTimers(t, clck, 1, 1*time.Second)
	// Make sure Add() is called before we cancel, otherwise we'll race against the ctx
	assert.Eventually(t, addCalled.Load, 1*time.Second, time.Millisecond)
	cancel()
	wg.Wait()
	assert.True(t, removeCalled)
}

func TestRedisNodeTrackerOther(t *testing.T) {
	t.Parallel()

	ctxTest, cancelTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTest()

	// We use a time in the future, because the redis client uses ctx.Deadline(),
	// which returns the mock time, but then uses it in computation with real time.
	clck := clock.NewMock(time.Unix(2147480000, 0))
	ctxClock := clock.Context(ctxTest, clck)

	mr := miniredis.RunT(t)

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
		DB:   0,
	})
	defer redisClient.Close()

	ctxRunner, cancel := context.WithCancel(ctxClock)

	var addMeCalled atomic.Bool
	addDerpCalled := false
	removeMeCalled := false
	removeDerpCalled := false

	rnt := NewRedisNodeTracker(
		logrus.New(),
		&fixtures.MockNodePicker{TB: t,
			FnAdd: func(node string) {
				switch node {
				case "me":
					addMeCalled.Store(true)
				case "derp":
					addDerpCalled = true
				default:
					assert.Fail(t, "unexpected node", node)
				}
			},
			FnRemove: func(node string) {
				switch node {
				case "me":
					removeMeCalled = true
				case "derp":
					removeDerpCalled = true
					cancel() // Terminate the test when derp is removed
				default:
					assert.Fail(t, "unexpected node", node)
				}
			},
		},
		redisClient,
		"foo",
		"me",
		1*time.Second,
		2*time.Second,
	)
	var wg wait.Group
	wg.StartWithContext(ctxRunner, rnt.Run)

	fixtures.EnsureAttachedTimers(t, clck, 1, 1*time.Second)
	assert.Eventually(t, addMeCalled.Load, 1*time.Second, time.Millisecond)

	redisClient.Publish(ctxTest, "foo", "+derp")
	redisClient.Publish(ctxTest, "foo", "-derp")

	wg.Wait()
	assert.True(t, addDerpCalled)
	assert.True(t, removeMeCalled)
	assert.True(t, removeDerpCalled)
}
