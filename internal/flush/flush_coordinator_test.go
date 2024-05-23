package flush

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockFlushable struct {
	flushCount int
	s          Flushable
}

func (mockFlushable *mockFlushable) Flush() {
	mockFlushable.flushCount += 1
}

func TestFlush(t *testing.T) {
	t.Parallel()

	f := mockFlushable{}
	fm := NewFlushCoordinator()
	fm.RegisterFlushable(&f)

	fm.Flush()
	assert.Equal(t, 1, f.flushCount)
}

func TestWaitForFlush(t *testing.T) {
	t.Parallel()

	f := mockFlushable{}
	fm := NewFlushCoordinator()
	fm.RegisterFlushable(&f)

	fm.Flush()
	go func() {
		<-time.NewTimer(100 * time.Millisecond).C
		fm.NotifyFlush()
	}()
	fm.WaitForFlush()
}
