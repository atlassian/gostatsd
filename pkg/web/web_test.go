package web_test

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/atlassian/gostatsd/pkg/web"
)

func TestHttpServerShutsdown(t *testing.T) {
	ctxTest, completed := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer completed()

	hs, err := web.NewHttpServer(
		logrus.StandardLogger(),
		nil,
		"TestHttpServerShutsdown",
		"127.0.0.1:0", // should pick a random port to bind to
		false,
		false,
		false,
		true,
		nil,
		nil,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(ctxTest)
	chDone := make(chan struct{}, 1)
	go func() {
		hs.Run(ctx)
		select {
		case <-ctxTest.Done():
		case chDone <- struct{}{}:
		}
	}()

	cancel()
	select {
	case <-ctxTest.Done():
		t.FailNow()
	case <-chDone:
	}
}
