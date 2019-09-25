package transport

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestNewHttpTransportEnforceRanges(t *testing.T) {
	t.Parallel()
	for _, config := range []struct {
		param string
		value interface{}
		valid bool
	}{
		{paramHttpDialerKeepAlive, -1 * time.Second, false},
		{paramHttpDialerKeepAlive, 0 * time.Second, true},
		{paramHttpDialerKeepAlive, 1 * time.Second, true},
		{paramHttpDialerTimeout, -1 * time.Second, false},
		{paramHttpDialerTimeout, 0 * time.Second, true},
		{paramHttpDialerTimeout, 1 * time.Second, true},
		{paramHttpEnableHttp2, true, true},
		{paramHttpEnableHttp2, false, true},
		{paramHttpIdleConnectionTimeout, -1 * time.Second, false},
		{paramHttpIdleConnectionTimeout, 0 * time.Second, true},
		{paramHttpIdleConnectionTimeout, 1 * time.Second, true},
		{paramHttpMaxIdleConnections, -1, false},
		{paramHttpMaxIdleConnections, 0, true},
		{paramHttpMaxIdleConnections, 1, true},
		{paramHttpTLSHandshakeTimeout, -1, false},
		{paramHttpTLSHandshakeTimeout, 0, true},
		{paramHttpTLSHandshakeTimeout, 1, true},
	} {
		v := viper.New()
		v.Set("transport.test."+config.param, config.value)
		p := NewTransportPool(logrus.New(), v)
		c, err := p.Get("test")
		if config.valid {
			require.NoError(t, err, "param: %s, value: %#v", config.param, config.value)
			require.NotNil(t, c, "param: %s, value: %#v", config.param, config.value)
		} else {
			require.Error(t, err, "param: %s, value: %#v", config.param, config.value)
			require.Nil(t, c, "param: %s, value: %#v", config.param, config.value)
		}
	}
}
