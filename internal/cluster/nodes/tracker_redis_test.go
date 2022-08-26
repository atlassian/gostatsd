package nodes

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ash2k/stager/wait"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tilinna/clock"

	"github.com/atlassian/gostatsd/internal/fixtures"
)

func TestRedisNodeTrackerSelf(t *testing.T) {
	t.Parallel()

	ctxTest, cancelTest := context.WithTimeout(context.Background(), 1*time.Second) // Ensure everything we want to do is done in under 1 second.
	defer cancelTest()

	clck := clock.NewMock(time.Unix(10, 0))
	ctxClock := clock.Context(ctxTest, clck)

	mr, err := miniredis.Run()
	require.NoError(t, err)
	require.NotNil(t, mr)
	defer mr.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
		DB:   0,
	})

	ctxRunner, cancel := context.WithCancel(ctxClock)

	addCalled := false
	removeCalled := false
	rnt := NewRedisNodeTracker(
		logrus.New(),
		&fixtures.MockNodePicker{TB: t,
			FnAdd: func(node string) {
				assert.Equal(t, "me", node)
				addCalled = true
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

	fixtures.EnsureAttachedTimers(t, clck, 1, 100*time.Millisecond)
	cancel()

	wg.Wait()
	assert.True(t, addCalled)
	assert.True(t, removeCalled)
}

func TestRedisNodeTrackerOther(t *testing.T) {
	t.Parallel()

	ctxTest, cancelTest := context.WithTimeout(context.Background(), 1*time.Second) // Ensure everything we want to do is done in under 1 second.
	defer cancelTest()

	clck := clock.NewMock(time.Unix(10, 0))
	ctxClock := clock.Context(ctxTest, clck)

	mr, err := miniredis.Run()
	require.NoError(t, err)
	require.NotNil(t, mr)
	defer mr.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
		DB:   0,
	})

	ctxRunner, cancel := context.WithCancel(ctxClock)

	addMeCalled := false
	addDerpCalled := false
	removeMeCalled := false
	removeDerpCalled := false

	rnt := NewRedisNodeTracker(
		logrus.New(),
		&fixtures.MockNodePicker{TB: t,
			FnAdd: func(node string) {
				switch node {
				case "me":
					addMeCalled = true
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
				default:
					assert.Fail(t, "unexpected node", node)
				}
				cancel()
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

	fixtures.EnsureAttachedTimers(t, clck, 1, 100*time.Millisecond)
	redisClient.Publish(ctxTest, "foo", "+derp")
	redisClient.Publish(ctxTest, "foo", "-derp")

	wg.Wait()
	assert.True(t, addMeCalled)
	assert.True(t, addDerpCalled)
	assert.True(t, removeMeCalled)
	assert.True(t, removeDerpCalled)
}
