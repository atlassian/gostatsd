package fixtures

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// MockNodePicker implements a mock nodes.NodePicker from github.com/atlassian/gostatsd/internal/cluster/nodes
type MockNodePicker struct {
	TB testing.TB

	FnAdd    func(node string)
	FnList   func() []string
	FnRemove func(node string)
	FnRun    func(ctx context.Context)
	FnSelect func(key string) (string, bool, error)
}

func (m *MockNodePicker) Add(node string) {
	if m.FnAdd != nil {
		m.FnAdd(node)
	} else {
		assert.Fail(m.TB, "NodePicker.Add must not be called")
	}
}

func (m *MockNodePicker) List() (p0 []string) {
	if m.FnList != nil {
		return m.FnList()
	}
	assert.Fail(m.TB, "NodePicker.List must not be called")
	return
}

func (m *MockNodePicker) Remove(node string) {
	if m.FnRemove != nil {
		m.FnRemove(node)
	} else {
		assert.Fail(m.TB, "NodePicker.Remove must not be called")
	}
}

func (m *MockNodePicker) Run(ctx context.Context) {
	if m.FnRun != nil {
		m.FnRun(ctx)
	} else {
		assert.Fail(m.TB, "NodePicker.Run must not be called")
	}
}

func (m *MockNodePicker) Select(key string) (p0 string, p1 bool, p2 error) {
	if m.FnSelect != nil {
		return m.FnSelect(key)
	}
	assert.Fail(m.TB, "NodePicker.Select must not be called")
	return
}
