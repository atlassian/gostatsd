package transport

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestNewTransportPool(t *testing.T) {
	t.Parallel()

	p := NewTransportPool(logrus.New(), viper.New())
	require.NotNil(t, p)
}

func TestGetWithDefault(t *testing.T) {
	t.Parallel()

	p := NewTransportPool(logrus.New(), viper.New())
	c, err := p.Get("default")
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestGetWithUnknown(t *testing.T) {
	t.Parallel()

	p := NewTransportPool(logrus.New(), viper.New())
	c, err := p.Get("x")
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestGetReusesClient(t *testing.T) {
	t.Parallel()

	p := NewTransportPool(logrus.New(), viper.New())
	c1, err := p.Get("default")
	require.NoError(t, err)
	require.NotNil(t, c1)
	c2, err := p.Get("default")
	require.NoError(t, err)
	require.NotNil(t, c2)

	require.Equal(t, c1, c2)
}

func TestGetMakesNewClient(t *testing.T) {
	t.Parallel()

	v := viper.New()
	v.Set("transport.x", map[string]interface{}{})
	p := NewTransportPool(logrus.New(), v)

	c1, err := p.Get("default")
	require.NoError(t, err)
	require.NotNil(t, c1)

	c2, err := p.Get("x")
	require.NoError(t, err)
	require.NotNil(t, c2)

	require.NotEqual(t, c1, c2)
}

func TestGetEnforceTimeout(t *testing.T) {
	t.Parallel()

	v := viper.New()
	v.Set("transport.neg.client-timeout", -1*time.Second)
	v.Set("transport.zero.client-timeout", 0*time.Second)
	v.Set("transport.pos.client-timeout", 1*time.Second)
	p := NewTransportPool(logrus.New(), v)

	c, err := p.Get("neg")
	require.Error(t, err)
	require.Nil(t, c)

	c, err = p.Get("zero")
	require.NoError(t, err)
	require.NotNil(t, c)

	c, err = p.Get("pos")
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestGetUnknownType(t *testing.T) {
	t.Parallel()

	v := viper.New()
	v.Set("transport.http.type", "http")
	v.Set("transport.unknown.type", "unknown")
	p := NewTransportPool(logrus.New(), v)

	c, err := p.Get("http")
	require.NoError(t, err)
	require.NotNil(t, c)

	c, err = p.Get("unknown")
	require.Error(t, err)
	require.Nil(t, c)
}
