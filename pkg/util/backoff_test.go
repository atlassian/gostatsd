package util

import (
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// Note: This test suite doesn't validate retry-max-time, as that requires a mock clock to be injected, or real time.

func newByViper(policy string, interval, maxTime time.Duration, maxCount int64) (BackoffFactory, error) {
	v := viper.New()
	v.Set(paramRetryInterval, interval)
	v.Set(paramRetryMaxCount, maxCount)
	v.Set(paramRetryMaxTime, maxTime)
	v.Set(paramRetryPolicy, policy)
	return GetRetryFromViper(v)
}

func TestDisabledRetries(t *testing.T) {
	t.Parallel()
	f, err := newByViper(policyDisabled, 10*time.Second, 10*time.Second, 10)
	require.NoError(t, err)
	require.NotNil(t, f)

	bo := f()
	require.Equal(t, backoff.Stop, bo.NextBackOff())
}

func TestConstantInterval(t *testing.T) {
	t.Parallel()
	f, err := newByViper(policyConstant, 1*time.Second, 10*time.Second, 0)
	require.NoError(t, err)
	require.NotNil(t, f)

	bo := f()
	for i := 0; i < 10; i++ {
		// Ensure it doesn't start growing
		d := bo.NextBackOff()
		require.LessOrEqual(t, uint64(d), uint64(time.Second*2))
		require.GreaterOrEqual(t, uint64(d), uint64(time.Second/2))
	}
}

func TestConstantIntervalMaxCount(t *testing.T) {
	t.Parallel()
	f, err := newByViper(policyConstant, 1*time.Second, 10*time.Second, 10)
	require.NoError(t, err)
	require.NotNil(t, f)

	bo := f()
	for i := 0; i < 10; i++ {
		d := bo.NextBackOff()
		require.NotEqual(t, backoff.Stop, d)
	}
	d := bo.NextBackOff()
	require.Equal(t, backoff.Stop, d)
}

func TestExponentialInterval(t *testing.T) {
	t.Parallel()
	f, err := newByViper(policyExponential, 1*time.Second, 10*time.Second, 0)
	require.NoError(t, err)
	require.NotNil(t, f)

	bo := f()
	prevInterval := time.Duration(0)
	for i := 0; i < 10; i++ {
		// Ensure it grows.  We need the scaling factor to account for the randomization in the interval.
		d := bo.NextBackOff()
		require.GreaterOrEqual(t, uint64(d), uint64(prevInterval/2))
		prevInterval = d
	}
}

func TestExponentialIntervalMaxCount(t *testing.T) {
	t.Parallel()
	f, err := newByViper(policyExponential, 1*time.Second, 10*time.Second, 10)
	require.NoError(t, err)
	require.NotNil(t, f)

	bo := f()
	prevInterval := time.Duration(0)
	for i := 0; i < 10; i++ {
		// Ensure it grows.  We need the scaling factor to account for the randomization in the interval.
		d := bo.NextBackOff()
		require.GreaterOrEqual(t, uint64(d), uint64(prevInterval/2))
		prevInterval = d
	}
	d := bo.NextBackOff()
	require.Equal(t, backoff.Stop, d)
}

func TestInvalidConfigurations(t *testing.T) {
	tests := []struct {
		interval time.Duration
		maxCount int64
		maxTime  time.Duration
		policy   string
		failure  string
	}{
		{-1 * time.Second, 0, 1 * time.Second, policyConstant, paramRetryInterval},
		{0, 0, 1 * time.Second, policyConstant, paramRetryInterval},
		{1 * time.Second, -1, 1 * time.Second, policyConstant, paramRetryMaxCount},
		{1 * time.Second, 0, -1 * time.Second, policyConstant, paramRetryMaxTime},
		{1 * time.Second, 0, 0, policyConstant, paramRetryMaxTime},
		{1 * time.Second, 1, 1 * time.Second, "invalid", paramRetryPolicy},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			f, err := newByViper(test.policy, test.interval, test.maxTime, test.maxCount)
			require.Nil(t, f)
			require.Error(t, err)
			require.Contains(t, err.Error(), test.failure)
		})
	}
}
