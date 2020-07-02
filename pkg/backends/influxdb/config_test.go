package influxdb

import (
	"net/url"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestViperV1(t *testing.T) {
	t.Parallel()
	v := viper.New()
	v.Set(paramApiVersion, 1)
	v.Set(paramConsistency, "one")
	v.Set(paramDatabase, "test-db")
	v.Set(paramRetentionPolicy, "test-rp")
	cfg, err := newConfigFromViper(v, logrus.New())

	require.NoError(t, err)
	require.IsType(t, configV1{}, cfg)
	q := url.Values{}
	cfg.Build(q)
	require.Len(t, q, 3)
	require.Equal(t, "one", q.Get(queryConsistency))
	require.Equal(t, "test-db", q.Get(queryDatabase))
	require.Equal(t, "test-rp", q.Get(queryRetentionPolicy))
}

func TestViperV1MissingKeys(t *testing.T) {
	t.Parallel()
	v := viper.New()
	v.Set(paramApiVersion, 1)
	cfg, err := newConfigFromViper(v, logrus.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), paramDatabase)
	require.Nil(t, cfg)
}

func TestViperV2(t *testing.T) {
	t.Parallel()
	v := viper.New()
	v.Set(paramApiVersion, 2)
	v.Set(paramBucket, "test-bucket")
	v.Set(paramOrg, "test-org")
	cfg, err := newConfigFromViper(v, logrus.New())

	require.NoError(t, err)
	require.IsType(t, configV2{}, cfg)
	q := url.Values{}
	cfg.Build(q)
	require.Len(t, q, 2)
	require.Equal(t, "test-bucket", q.Get(queryBucket))
	require.Equal(t, "test-org", q.Get(queryOrg))
}

func TestViperV2MissingKeys(t *testing.T) {
	t.Parallel()
	v := viper.New()
	v.Set(paramApiVersion, 2)

	cfg, err := newConfigFromViper(v, logrus.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), paramBucket)
	require.Nil(t, cfg)
	v.Set(paramBucket, "test-bucket")

	cfg, err = newConfigFromViper(v, logrus.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), paramOrg)
	require.Nil(t, cfg)
}

func TestViperInvalidVersion(t *testing.T) {
	t.Parallel()
	v := viper.New()
	v.Set(paramApiVersion, "derp")
	cfg, err := newConfigFromViper(v, logrus.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), paramApiVersion)
	require.Nil(t, cfg)
}
