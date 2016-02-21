package stdout

import (
	"bytes"
	"fmt"
	"regexp"
	"time"

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
)

const backendName = "stdout"

func init() {
	backend.RegisterBackend(backendName, func() (backend.MetricSender, error) {
		return NewStdoutClient()
	})
}

// StdoutClient is an object that is used to send messages to stdout
type StdoutClient struct{}

// NewStdoutClient constructs a StdoutClient object
func NewStdoutClient() (backend.MetricSender, error) {
	return &StdoutClient{}, nil
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

// SendMetrics sends the metrics in a MetricsMap to the Graphite server
func (client *StdoutClient) SendMetrics(metrics types.MetricMap) error {
	buf := new(bytes.Buffer)
	now := time.Now().Unix()
	for k, v := range metrics {
		nk := normalizeBucketName(k)
		fmt.Fprintf(buf, "%s %f %d\n", nk, v, now)
	}
	_, err := buf.WriteTo(log.StandardLogger().Writer())
	if err != nil {
		return err
	}
	return nil
}

func (client *StdoutClient) Name() string {
	return backendName
}
