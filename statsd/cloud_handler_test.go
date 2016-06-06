package statsd

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	cloudTypes "github.com/atlassian/gostatsd/cloudprovider/types"
	"github.com/atlassian/gostatsd/types"

	"golang.org/x/net/context"
	"golang.org/x/time/rate"
)

func TestCloudHandlerExpirationAndRefresh(t *testing.T) {
	testExpire(t, []types.IP{"4.3.2.1", "4.3.2.1"}, func(h Handler) error {
		e := se1()
		return h.DispatchEvent(context.Background(), &e)
	})
	testExpire(t, []types.IP{"1.2.3.4", "1.2.3.4"}, func(h Handler) error {
		m := sm1()
		return h.DispatchMetric(context.Background(), &m)
	})
}

func testExpire(t *testing.T, expectedIps []types.IP, f func(Handler) error) {
	fp := &fakeProviderIP{
		Region: "us-west-3",
	}
	counting := &countingHandler{}
	ch := NewCloudHandler(fp, counting, rate.NewLimiter(100, 120), &CacheOptions{
		CacheRefreshPeriod:        100 * time.Millisecond,
		CacheEvictAfterIdlePeriod: 700 * time.Millisecond,
		CacheTTL:                  500 * time.Millisecond,
		CacheNegativeTTL:          500 * time.Millisecond,
	})
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if handlerErr := ch.Run(ctx); handlerErr != nil && handlerErr != context.Canceled {
			t.Errorf("Cloud handler quit unexpectedly: %v", handlerErr)
		}
	}()
	if err := f(ch); err != nil {
		t.Fatal(err)
	}
	time.Sleep(900 * time.Millisecond) // Should be refreshed couple of times and evicted.

	cancelFunc()
	wg.Wait()

	if !reflect.DeepEqual(fp.ips, expectedIps) {
		t.Errorf("%+v is not equal to the expected ips %+v", fp.ips, expectedIps)
	}
	if len(ch.(*cloudHandler).cache) > 0 {
		t.Errorf("cache should be empty %s", ch.(*cloudHandler).cache)
	}
}

func TestCloudHandlerDispatch(t *testing.T) {
	fp := &fakeProviderIP{
		Region: "us-west-3",
		Tags:   types.Tags{"tag1", "tag2:234"},
	}
	counting := &countingHandler{}

	expectedIps := []types.IP{"1.2.3.4", "4.3.2.1"}
	expectedMetrics := []types.Metric{
		{
			Name:     "t1",
			Value:    42.42,
			Tags:     types.Tags{"a1", "region:us-west-3", "tag1", "tag2:234"},
			Hostname: "i-1.2.3.4",
			SourceIP: "1.2.3.4",
			Type:     types.COUNTER,
		},
		{
			Name:     "t1",
			Value:    45.45,
			Tags:     types.Tags{"a4", "region:us-west-3", "tag1", "tag2:234"},
			Hostname: "i-1.2.3.4",
			SourceIP: "1.2.3.4",
			Type:     types.COUNTER,
		},
	}
	expectedEvents := types.Events{
		types.Event{
			Title:    "t12",
			Text:     "asrasdfasdr",
			Tags:     types.Tags{"a2", "region:us-west-3", "tag1", "tag2:234"},
			Hostname: "i-4.3.2.1",
			SourceIP: "4.3.2.1",
		},
		types.Event{
			Title:    "t1asdas",
			Text:     "asdr",
			Tags:     types.Tags{"a2-35", "region:us-west-3", "tag1", "tag2:234"},
			Hostname: "i-4.3.2.1",
			SourceIP: "4.3.2.1",
		},
	}
	doCheck(t, fp, counting, sm1(), se1(), sm2(), se2(), &fp.ips, expectedIps, expectedMetrics, expectedEvents)
}

func TestCloudHandlerInstanceNotFound(t *testing.T) {
	fp := &fakeProviderNotFound{}
	counting := &countingHandler{}
	expectedIps := []types.IP{"1.2.3.4", "4.3.2.1"}
	expectedMetrics := []types.Metric{
		sm1(),
		sm2(),
	}
	expectedEvents := types.Events{
		se1(),
		se2(),
	}
	doCheck(t, fp, counting, sm1(), se1(), sm2(), se2(), &fp.ips, expectedIps, expectedMetrics, expectedEvents)
}

func TestCloudHandlerFailingProvider(t *testing.T) {
	fp := &fakeFailingProvider{}
	counting := &countingHandler{}
	expectedIps := []types.IP{"1.2.3.4", "4.3.2.1"}
	expectedMetrics := []types.Metric{
		sm1(),
		sm2(),
	}
	expectedEvents := types.Events{
		se1(),
		se2(),
	}
	doCheck(t, fp, counting, sm1(), se1(), sm2(), se2(), &fp.ips, expectedIps, expectedMetrics, expectedEvents)
}

func doCheck(t *testing.T, cloud cloudTypes.Interface, counting *countingHandler, m1 types.Metric, e1 types.Event, m2 types.Metric, e2 types.Event, ips *[]types.IP, expectedIps []types.IP, expectedM []types.Metric, expectedE types.Events) {
	ch := NewCloudHandler(cloud, counting, rate.NewLimiter(100, 120), nil)
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if handlerErr := ch.Run(ctx); handlerErr != nil && handlerErr != context.Canceled {
			t.Errorf("Cloud handler quit unexpectedly: %v", handlerErr)
		}
	}()
	if err := ch.DispatchMetric(ctx, &m1); err != nil {
		t.Fatal(err)
	}
	if err := ch.DispatchEvent(ctx, &e1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	if err := ch.DispatchMetric(ctx, &m2); err != nil {
		t.Fatal(err)
	}
	if err := ch.DispatchEvent(ctx, &e2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	cancelFunc()
	wg.Wait()

	if !reflect.DeepEqual(*ips, expectedIps) {
		t.Errorf("%+v is not equal to the expected ips %+v", *ips, expectedIps)
	}
	if !reflect.DeepEqual(counting.metrics, expectedM) {
		t.Errorf("%+v is not equal to the expected metrics %+v", counting.metrics, expectedM)
	}
	if !reflect.DeepEqual(counting.events, expectedE) {
		t.Errorf("%+v is not equal to the expected events %+v", counting.events, expectedE)
	}
}

func sm1() types.Metric {
	return types.Metric{
		Name:     "t1",
		Value:    42.42,
		Tags:     types.Tags{"a1"},
		Hostname: "somehost",
		SourceIP: "1.2.3.4",
		Type:     types.COUNTER,
	}
}

func sm2() types.Metric {
	return types.Metric{
		Name:     "t1",
		Value:    45.45,
		Tags:     types.Tags{"a4"},
		Hostname: "somehost",
		SourceIP: "1.2.3.4",
		Type:     types.COUNTER,
	}
}

func se1() types.Event {
	return types.Event{
		Title:    "t12",
		Text:     "asrasdfasdr",
		Tags:     types.Tags{"a2"},
		Hostname: "some_random_host",
		SourceIP: "4.3.2.1",
	}
}

func se2() types.Event {
	return types.Event{
		Title:    "t1asdas",
		Text:     "asdr",
		Tags:     types.Tags{"a2-35"},
		Hostname: "some_random_host",
		SourceIP: "4.3.2.1",
	}
}

type countingHandler struct {
	mu      sync.Mutex
	metrics []types.Metric
	events  types.Events
}

func (ch *countingHandler) DispatchMetric(ctx context.Context, m *types.Metric) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.metrics = append(ch.metrics, *m)
	return nil
}

func (ch *countingHandler) DispatchEvent(ctx context.Context, e *types.Event) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.events = append(ch.events, *e)
	return nil
}

type fakeCountingProvider struct {
	mu  sync.Mutex
	ips []types.IP
}

func (fp *fakeCountingProvider) count(ip types.IP) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.ips = append(fp.ips, ip)
}

func (fp *fakeCountingProvider) SampleConfig() string {
	return ""
}

type fakeProviderIP struct {
	fakeCountingProvider
	Region string
	Tags   types.Tags
}

func (fp *fakeProviderIP) ProviderName() string {
	return "fakeProviderIP"
}

func (fp *fakeProviderIP) Instance(ip types.IP) (*cloudTypes.Instance, error) {
	fp.count(ip)
	return &cloudTypes.Instance{
		ID:     "i-" + string(ip),
		Region: fp.Region,
		Tags:   fp.Tags,
	}, nil
}

type fakeProviderNotFound struct {
	fakeCountingProvider
}

func (fp *fakeProviderNotFound) ProviderName() string {
	return "fakeProviderNotFound"
}

func (fp *fakeProviderNotFound) Instance(ip types.IP) (*cloudTypes.Instance, error) {
	fp.count(ip)
	return nil, nil
}

type fakeFailingProvider struct {
	fakeCountingProvider
}

func (fp *fakeFailingProvider) ProviderName() string {
	return "fakeFailingProvider"
}

func (fp *fakeFailingProvider) Instance(ip types.IP) (*cloudTypes.Instance, error) {
	fp.count(ip)
	return nil, errors.New("clear skies, no clouds available")
}
