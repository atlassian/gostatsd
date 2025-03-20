package flush

import (
	"sync"
)

type Coordinator interface {
	Flushable
	NotifyFlush()
	RegisterFlushable(f Flushable)
	WaitForFlush()
}

type Flushable interface {
	Flush()
}

var _ Coordinator = (*coordinator)(nil)

type coordinator struct {
	flushChan chan struct{}

	mu sync.RWMutex
	t  Flushable
}

func NewFlushCoordinator() Coordinator {
	// TODO: Because of the overall architecture, we don't have the Flusher
	//       at the time we create the Coordinator.  It would be good to
	//       re-architect the world for proper dependency injection, however
	//       that is a non-trivial chunk of work.  So for now, it's just TODO.
	return &coordinator{
		flushChan: make(chan struct{}, 1),
		t:         noopFlusher{},
	}
}

func (fm *coordinator) NotifyFlush() {
	fm.flushChan <- struct{}{}
}

func (fm *coordinator) Flush() {
	if t := fm.getTarget(); t != nil {
		t.Flush()
	}
}

func (fm *coordinator) WaitForFlush() {
	<-fm.flushChan
}

func (fm *coordinator) getTarget() Flushable {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.t
}

func (fm *coordinator) RegisterFlushable(t Flushable) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.t = t
}

func NewNoopFlushCoordinator() Coordinator {
	return &noopFlushCoordinator{}
}

type noopFlushCoordinator struct{}

func (n *noopFlushCoordinator) Flush() {}

func (n *noopFlushCoordinator) NotifyFlush() {}

func (n *noopFlushCoordinator) RegisterFlushable(Flushable) {}

func (n *noopFlushCoordinator) WaitForFlush() {}

type noopFlusher struct{}

func (noopFlusher) Flush() {}
