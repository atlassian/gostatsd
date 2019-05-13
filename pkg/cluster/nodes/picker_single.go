package nodes

import (
	"context"
)

// singleNodePicker is a NodePicker which holds and returns a single node.  Ignores any attempts to update it.
//
// The main benefit is no lock compared to the Consistent variety.
type singleNodePicker struct {
	node string
}

func NewSingleNodePicker(node string) NodePicker {
	return &singleNodePicker{
		node: node,
	}
}

func (snp *singleNodePicker) Run(ctx context.Context) {}

func (snp *singleNodePicker) List() []string {
	return []string{snp.node}
}

func (snp *singleNodePicker) Select(key string) (string, error) {
	return snp.node, nil
}

func (snp *singleNodePicker) Add(node string)    {}
func (snp *singleNodePicker) Remove(node string) {}
