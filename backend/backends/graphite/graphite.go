package graphite

import (
	"bytes"
	"fmt"
	"net"
	"regexp"
	"time"

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

const backendName = "graphite"

func init() {
	viper.SetDefault("graphite.address", "localhost:2003")
	backend.RegisterBackend(backendName, func() (backend.MetricSender, error) {
		return NewGraphiteClient()
	})
}

// Regular expressions used for bucket name normalization
var (
	regSpaces  = regexp.MustCompile("\\s+")
	regSlashes = regexp.MustCompile("\\/")
	regInvalid = regexp.MustCompile("[^a-zA-Z_\\-0-9\\.]")
)

// normalizeBucketName cleans up a bucket name by replacing or translating invalid characters
func normalizeBucketName(name string) string {
	nospaces := regSpaces.ReplaceAllString(name, "_")
	noslashes := regSlashes.ReplaceAllString(nospaces, "-")
	return regInvalid.ReplaceAllString(noslashes, "")
}

// GraphiteClient is an object that is used to send messages to a Graphite server's UDP interface
type GraphiteClient struct {
	conn *net.Conn
}

// SendMetrics sends the metrics in a MetricsMap to the Graphite server
func (client *GraphiteClient) SendMetrics(metrics types.MetricMap) error {
	buf := new(bytes.Buffer)
	now := time.Now().Unix()
	for k, v := range metrics {
		nk := normalizeBucketName(k)
		fmt.Fprintf(buf, "%s %f %d\n", nk, v, now)
		log.Debugf("Graphite payload %s %f %d", nk, v, now)
	}
	_, err := buf.WriteTo(*client.conn)
	if err != nil {
		return fmt.Errorf("error sending to graphite: %s", err)
	}
	return nil
}

// NewGraphiteClient constructs a GraphiteClient object by connecting to an address
func NewGraphiteClient() (backend.MetricSender, error) {
	conn, err := net.Dial("tcp", viper.GetString("graphite.address"))
	if err != nil {
		return nil, err
	}
	return &GraphiteClient{&conn}, nil
}

func (client *GraphiteClient) Name() string {
	return backendName
}
