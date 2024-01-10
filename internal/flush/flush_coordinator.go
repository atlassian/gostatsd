package flush

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
	t         Flushable
	flushChan chan struct{}
}

func NewFlushCoordinator() Coordinator {
	return &coordinator{
		flushChan: make(chan struct{}, 1),
	}
}

func (fm *coordinator) NotifyFlush() {
	fm.flushChan <- struct{}{}
}

func (fm *coordinator) Flush() {
	fm.t.Flush()
}

func (fm *coordinator) WaitForFlush() {
	<-fm.flushChan
}

func (fm *coordinator) RegisterFlushable(t Flushable) {
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
