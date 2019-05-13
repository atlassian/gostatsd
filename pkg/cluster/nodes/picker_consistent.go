package nodes

import (
	"context"

	"stathat.com/c/consistent"
)

type consistentNodePicker struct {
	self   string
	hasher *consistent.Consistent
}

func NewConsistentNodePicker(self string, numReplicas int) NodePicker {
	c := consistent.New()
	c.NumberOfReplicas = numReplicas
	return &consistentNodePicker{
		self:   self,
		hasher: c,
	}
}

func (cnp *consistentNodePicker) Run(ctx context.Context) {}

func (cnp *consistentNodePicker) List() []string {
	return cnp.hasher.Members()
}

func (cnp *consistentNodePicker) Select(key string) (string, error) {
	host, err := cnp.hasher.Get(key)
	if err == nil && host == cnp.self {
		return NodeNameSelf, nil
	}
	return host, err
}

func (cnp *consistentNodePicker) Add(node string) {
	cnp.hasher.Add(node)
}

func (cnp *consistentNodePicker) Remove(node string) {
	cnp.hasher.Remove(node)
}
