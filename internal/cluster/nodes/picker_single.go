package nodes

import (
	"context"
)

// singleNodePicker is a NodePicker which holds and returns a single node.  Ignores any attempts to update it.
type singleNodePicker struct {
	node string
	self bool
}

func NewSingleNodePicker(node string, self bool) NodePicker {
	return &singleNodePicker{
		node: node,
		self: self,
	}
}

func (snp *singleNodePicker) Run(ctx context.Context) {
	<-ctx.Done()
}

func (snp *singleNodePicker) List() []string {
	return []string{snp.node}
}

func (snp *singleNodePicker) Select(key string) (string, bool, error) {
	return snp.node, snp.self, nil
}

func (snp *singleNodePicker) Add(node string)    {}
func (snp *singleNodePicker) Remove(node string) {}
