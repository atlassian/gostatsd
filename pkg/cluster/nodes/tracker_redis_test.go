package nodes

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TODO: Replace with github.com/tilinna/clock
type fakeTime struct {
	samples []time.Time
	next    int
}

func (ft *fakeTime) Now() time.Time {
	t := ft.samples[ft.next]
	ft.next = (ft.next + 1) % len(ft.samples)
	fmt.Printf("%v\n", t)
	return t
}

func TestLearnNode(t *testing.T) {
	t.Parallel()

	now := time.Now()

	bp := newBasicPicker()

	rnt := &redisNodeTracker{
		picker:         bp,
		updateInterval: 1 * time.Second,
		expiryInterval: 5 * time.Second,
		nodes:          map[string]time.Time{},
		now: (&fakeTime{
			samples: []time.Time{
				now.Add(time.Second),
			},
		}).Now,
	}

	// Empty
	require.Equal(t, 0, len(bp.nodes))

	// Update internal tracking
	rnt.refreshNode("127.0.0.1:80")

	// Present
	require.Equal(t, 1, len(bp.nodes))
	require.True(t, bp.contains("127.0.0.1:80"))
}

func TestLearnMultipleNodes(t *testing.T) {
	t.Parallel()

	now := time.Now()

	bp := newBasicPicker()

	rnt := &redisNodeTracker{
		picker:         bp,
		updateInterval: 1 * time.Second,
		expiryInterval: 5 * time.Second,
		nodes:          map[string]time.Time{},
		now: (&fakeTime{
			samples: []time.Time{
				now.Add(time.Second),
			},
		}).Now,
	}

	// Empty
	require.Equal(t, 0, len(bp.nodes))

	// Update internal tracking
	rnt.refreshNode("127.0.0.2:80")
	rnt.refreshNode("127.0.0.1:80")

	// Present
	require.Equal(t, 2, len(bp.nodes))
	require.True(t, bp.contains("127.0.0.1:80"))
	require.True(t, bp.contains("127.0.0.2:80"))
}

func TestForgetNodes(t *testing.T) {
	t.Parallel()

	now := time.Now()

	bp := newBasicPicker()

	rnt := &redisNodeTracker{
		picker:         bp,
		updateInterval: 1 * time.Second,
		expiryInterval: 5 * time.Second,
		nodes:          map[string]time.Time{},
		now: (&fakeTime{
			samples: []time.Time{
				now,
				now.Add(1000 * time.Millisecond),
				now.Add(5500 * time.Millisecond),
			},
		}).Now,
	}

	// Empty
	require.Equal(t, 0, len(bp.nodes))

	// Update internal tracking with multiple nodes
	rnt.refreshNode("127.0.0.1:80") // time.sample[0] - will expire
	rnt.refreshNode("127.0.0.2:80") // time.sample[1] - will not expire
	rnt.expireNodes()               // time.sample[2]

	// Check
	require.Equal(t, 1, len(bp.nodes))
	require.False(t, bp.contains("127.0.0.1:80"))
	require.True(t, bp.contains("127.0.0.2:80"))
}

func TestUpdateExistingNode(t *testing.T) {
	t.Parallel()

	now := time.Now()

	bp := newBasicPicker()

	rnt := &redisNodeTracker{
		picker:         bp,
		updateInterval: 1 * time.Second,
		expiryInterval: 5 * time.Second,
		nodes:          map[string]time.Time{},
		now: (&fakeTime{
			samples: []time.Time{
				now,
				now.Add(1000 * time.Millisecond),
				now.Add(5500 * time.Millisecond),
			},
		}).Now,
	}

	// Empty
	require.Equal(t, 0, len(bp.nodes))

	// Update internal tracking with multiple nodes
	rnt.refreshNode("127.0.0.1:80") // time.sample[0] - now
	rnt.refreshNode("127.0.0.1:80") // time.sample[1] - now+1 - refreshes timestamp
	rnt.expireNodes()               // time.sample[2] - now+5.5

	// Check
	require.Equal(t, 1, len(bp.nodes))
	require.True(t, bp.contains("127.0.0.1:80"))

}
