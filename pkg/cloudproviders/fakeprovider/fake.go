package fakeprovider

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/atlassian/gostatsd"
)

type Counting struct {
	mu          sync.Mutex
	ips         []gostatsd.IP
	invocations uint64
}

func (fp *Counting) IPs() []gostatsd.IP {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	result := make([]gostatsd.IP, len(fp.ips))
	copy(result, fp.ips)
	return result
}

func (fp *Counting) EstimatedTags() int {
	return 0
}

func (fp *Counting) MaxInstancesBatch() int {
	return 16
}

func (fp *Counting) Invocations() uint64 {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.invocations
}

func (fp *Counting) count(ips ...gostatsd.IP) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.ips = append(fp.ips, ips...)
	fp.invocations++
}

func (fp *Counting) SelfIP() (gostatsd.IP, error) {
	return gostatsd.UnknownIP, nil
}

type IP struct {
	Counting
	Region string
	Tags   gostatsd.Tags
}

func (fp *IP) Name() string {
	return "FakeProviderIP"
}

func (fp *IP) Instance(ctx context.Context, ips ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	fp.count(ips...)
	instances := make(map[gostatsd.IP]*gostatsd.Instance, len(ips))
	for _, ip := range ips {
		instances[ip] = &gostatsd.Instance{
			ID:   "i-" + string(ip),
			Tags: fp.Tags,
		}
	}
	return instances, nil
}

type NotFound struct {
	Counting
}

func (fp *NotFound) Name() string {
	return "FakeProviderNotFound"
}

func (fp *NotFound) Instance(ctx context.Context, ips ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	fp.count(ips...)
	return nil, nil
}

type Failing struct {
	Counting
}

func (fp *Failing) Name() string {
	return "FakeFailingProvider"
}

func (fp *Failing) Instance(ctx context.Context, ips ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	fp.count(ips...)
	return nil, errors.New("clear skies, no clouds available")
}

type Transient struct {
	call        uint64
	FailureMode []int
}

func (fpt *Transient) Name() string {
	return "FakeProviderTransient"
}

func (fpt *Transient) EstimatedTags() int {
	return 1
}

func (fpt *Transient) MaxInstancesBatch() int {
	return 1
}

func (fpt *Transient) SelfIP() (gostatsd.IP, error) {
	return gostatsd.UnknownIP, nil
}

// Instance emulates a lookup based on the supplied criteria.
// A failure mode of 0 is a successful lookup
// A failure mode of 1 is nil instance, no error (lookup failure)
// A failure mode of 2 is nil instance, with error
// Repeats the last specified failure mode
func (fpt *Transient) Instance(ctx context.Context, ips ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	r := make(map[gostatsd.IP]*gostatsd.Instance)

	c := atomic.AddUint64(&fpt.call, 1) - 1
	if c >= uint64(len(fpt.FailureMode)) {
		c = uint64(len(fpt.FailureMode) - 1)
	}
	switch fpt.FailureMode[c] {
	case 0:
		for _, ip := range ips {
			r[ip] = &gostatsd.Instance{
				ID:   string(ip),
				Tags: gostatsd.Tags{"tag:value"},
			}
		}
		return r, nil
	case 1:
		for _, ip := range ips {
			r[ip] = nil
		}
		return r, nil
	case 2:
		for _, ip := range ips {
			r[ip] = nil
		}
		return r, errors.New("failure mode 2")
	default:
		panic("fake misuse")
	}
}
