package extension

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ash2k/stager/wait"
	"github.com/gorilla/mux"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/atlassian/gostatsd/internal/awslambda/extension/api"
	"github.com/atlassian/gostatsd/internal/awslambda/extension/telemetry"
	"github.com/atlassian/gostatsd/internal/flush"
	"github.com/atlassian/gostatsd/pb"
	"github.com/atlassian/gostatsd/pkg/statsd"
	"github.com/atlassian/gostatsd/pkg/transport"
)

func TestManagerGostatsDIntegrationTest(t *testing.T) {
	t.Parallel()

	var forwarderCallReceived uint64

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	readyCh := make(chan struct{}, 1)

	apiMux := mux.NewRouter()
	apiMux.Handle("/v2/raw", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&forwarderCallReceived, 1)
		buf, errr := io.ReadAll(r.Body)
		require.NoError(t, errr, "Must not error when reading request")

		var data pb.RawMessageV2
		require.NoError(t, proto.Unmarshal(buf, &data))

		if len(data.GetCounters()) == 0 {
			readyCh <- struct{}{}
			close(readyCh)
		}
		for _, v := range data.GetCounters() {
			for tag, m := range v.TagMap {
				assert.EqualValues(t, 42, m.Value)
				assert.True(t, strings.Contains(tag, "fruit:pineapple"))
				cancel()
			}
		}
	}))
	apiMux.Handle("/v2/event", http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))

	gsdServer := httptest.NewServer(apiMux)
	t.Cleanup(gsdServer.Close)

	log := logrus.New()
	fc := flush.NewFlushCoordinator()

	statsDServer, err := createStatsDServer(gsdServer.URL, fc, log)

	// setup lambda server
	lambdaServer := createLambdaExtensionServer(t)
	t.Cleanup(lambdaServer.Close)

	teleAddr := availableAddr()
	u, err := url.Parse(lambdaServer.URL)
	require.NoError(t, err)
	m := NewManager(u.Host, "test", log, WithManualFlushEnabled(fc), WithCustomTelemetryServerAddr(teleAddr))

	wg := wait.Group{}
	wg.StartWithContext(ctx, func(ctx context.Context) {
		err := m.Run(ctx, statsDServer)
		assert.ErrorIs(t, err, ctx.Err())
	})

	<-readyCh

	// send udp metric
	err = sendUdpMetric(statsDServer.MetricsAddr, []byte("test.metrics:42|c|#fruit:pineapple"))
	require.NoError(t, err, "failed to send udp metrics to statsDServer")

	// send telemetry done message
	<-time.After(time.Duration(telemetry.MinBufferingTimeoutMs) * time.Millisecond)
	res, err := sendRuntimeDoneMsg(teleAddr)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// wait for shutdown
	wg.Wait()
	assert.EqualValues(t, forwarderCallReceived, 2)
}

func createStatsDServer(apiAddr string,
	fc flush.Coordinator,
	log logrus.FieldLogger) (s *statsd.Server, err error) {

	// setup statsd server
	l, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return
	}
	l.Close()

	v := viper.New()
	v.Set("http-transport", map[string]interface{}{
		"api-endpoint":       apiAddr,
		"consolidator-slots": 1,
		"compress":           false,
	})
	s = &statsd.Server{
		ForwarderFlushCoordinator: fc,
		FlushInterval:             5 * time.Minute,
		FlushOffset:               0,
		FlushAligned:              false,
		MaxReaders:                1,
		MaxParsers:                1,
		MaxWorkers:                1,
		ReceiveBatchSize:          50,
		MetricsAddr:               l.LocalAddr().String(),
		ServerMode:                "forwarder",
		Viper:                     v,
		TransportPool:             transport.NewTransportPool(log, v),
	}
	return
}

func createLambdaExtensionServer(t testing.TB) *httptest.Server {
	r := mux.NewRouter()
	r.Handle(api.RegisterEndpoint.String(), InitHandler(t, http.StatusOK))
	r.Handle(telemetry.SubscribeEndpoint.String(), TelemetryHandler(t, http.StatusOK))
	r.Handle(api.EventEndpoint.String(), EventNextHandler(t, http.StatusOK, 1))

	return httptest.NewServer(r)
}

func sendRuntimeDoneMsg(addr string) (*http.Response, error) {
	platformDoneMsg := []telemetry.Event{{telemetry.RuntimeDone}}
	b, err := jsoniter.Marshal(platformDoneMsg)
	if err != nil {
		return nil, err
	}

	res, err := http.Post(fmt.Sprintf("http://%s/telemetry", addr), "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return res, nil
}

func sendUdpMetric(addr string, msg []byte) error {
	conn, err := net.Dial("udp", addr)
	defer conn.Close()
	if err != nil {
		return err
	}
	_, err = conn.Write(msg)
	if err != nil {
		return err
	}

	return nil
}
