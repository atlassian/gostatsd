package statsd

import (
	"bytes"
	"fmt"
	"net"
	"time"
)

// Normalize a bucket name by replacing or translating invalid characters
func normalizeBucketName(name string) string {
	nospaces := regSpaces.ReplaceAllString(name, "_")
	noslashes := regSlashes.ReplaceAllString(nospaces, "-")
	return regInvalid.ReplaceAllString(noslashes, "")
}

type GraphiteClient struct {
	conn *net.Conn
}

func NewGraphiteClient(addr string) (client GraphiteClient, err error) {
	conn, err := net.Dial("tcp", addr)
	client = GraphiteClient{&conn}
	return
}

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
