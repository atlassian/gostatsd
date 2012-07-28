package statsd

import (
	"bytes"
	"fmt"
	"net"
	"time"
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
func (client *GraphiteClient) SendMetrics(metrics MetricMap) (err error) {
	buf := new(bytes.Buffer)
	now := time.Now().Unix()
	for k, v := range metrics {
		nk := normalizeBucketName(k)
		fmt.Fprintf(buf, "%s %f %d\n", nk, v, now)
	}
	_, err = buf.WriteTo(*client.conn)
	if err != nil {
		return err
	}
	return nil
}

// NewGraphiteClient constructs a GraphiteClient object by connecting to an address
func NewGraphiteClient(addr string) (client GraphiteClient, err error) {
	conn, err := net.Dial("tcp", addr)
	client = GraphiteClient{&conn}
	return
}
