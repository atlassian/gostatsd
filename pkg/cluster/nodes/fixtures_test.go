package nodes

import (
	"context"
	"errors"
)

type basicPicker struct {
	nodes map[string]struct{}
}

func newBasicPicker() *basicPicker {
	return &basicPicker{
		nodes: map[string]struct{}{},
	}
}

func (bp *basicPicker) Run(ctx context.Context) {
}

func (bp *basicPicker) List() []string {
	n := make([]string, 0, len(bp.nodes))
	for node := range bp.nodes {
		n = append(n, node)
	}
	return n
}

func (bp *basicPicker) Select(key string) (string, error) {
	return "", errors.New("not implemented")
}

func (bp *basicPicker) Add(node string) {
	bp.nodes[node] = struct{}{}

}

func (bp *basicPicker) Remove(node string) {
	delete(bp.nodes, node)
}

func (bp *basicPicker) contains(node string) bool {
	_, ok := bp.nodes[node]
	return ok
}
