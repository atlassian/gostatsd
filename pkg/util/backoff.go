package util

import (
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/spf13/viper"
)

const (
	paramRetryInterval = "retry-interval"  // constant
	paramRetryMaxCount = "retry-max-count" // constant + exponential
	paramRetryMaxTime  = "retry-max-time"  // constant + exponential
	paramRetryPolicy   = "retry-policy"

	defaultRetryInterval = 1 * time.Second  // constant
	defaultRetryMaxCount = 0                // constant + exponential
	defaultRetryMaxTime  = 15 * time.Second // constant + exponential
	defaultRetryPolicy   = policyExponential

	policyConstant    = "constant"
	policyDisabled    = "disabled"
	policyExponential = "exponential"
)

type BackoffFactory func() backoff.BackOff

// NewBackoffFactory creates a new BackoffFactory based on a backoff.ExponentialBackoff
//
// backoff.ConstantBackoff appears to be more of a debug/testing backoff policy, rather than a real
// implementation.  It lacks features such as randomization of interval, and a maximum duration. Therefore,
// we use a backoff.ExponentialBackOff with a Multiplier of 1.0 as a replacement.
func NewBackoffFactory(multiplier float64, maxElapsedTime, interval time.Duration, maxRetries uint64) BackoffFactory {
	return func() backoff.BackOff {
		bo := backoff.NewExponentialBackOff()
		bo.Multiplier = multiplier
		bo.MaxElapsedTime = maxElapsedTime
		bo.InitialInterval = interval
		bo.Reset() // Reset is required to make the InitialInterval change take effect.
		if maxRetries == 0 {
			return bo
		}
		return backoff.WithMaxRetries(bo, maxRetries)
	}
}

func GetRetryFromViper(v *viper.Viper) (BackoffFactory, error) {
	v.SetDefault(paramRetryInterval, defaultRetryInterval) // constant
	v.SetDefault(paramRetryMaxCount, defaultRetryMaxCount) // constant + exponential
	v.SetDefault(paramRetryMaxTime, defaultRetryMaxTime)   // constant + exponential
	v.SetDefault(paramRetryPolicy, defaultRetryPolicy)

	retryInterval := v.GetDuration(paramRetryInterval) // constant
	retryMaxCount := v.GetInt64(paramRetryMaxCount)    // constant + exponential
	retryMaxTime := v.GetDuration(paramRetryMaxTime)   // constant + exponential
	retryPolicy := v.GetString(paramRetryPolicy)

	if retryInterval <= 0 {
		return nil, errors.New(paramRetryInterval + " must be positive")
	}

	if retryMaxCount < 0 {
		return nil, errors.New(paramRetryMaxCount + " must be zero or positive")
	}

	if retryMaxTime <= 0 {
		return nil, errors.New(paramRetryMaxTime + " must be positive")
	}

	switch retryPolicy {
	case policyDisabled:
		return func() backoff.BackOff { return &backoff.StopBackOff{} }, nil
	case policyExponential:
		return NewBackoffFactory(backoff.DefaultMultiplier, retryMaxTime, backoff.DefaultInitialInterval, uint64(retryMaxCount)), nil
	case policyConstant:
		return NewBackoffFactory(1.0, retryMaxTime, retryInterval, uint64(retryMaxCount)), nil
	default:
		return nil, fmt.Errorf("%s (%s) not one of %s, %s, or %s", paramRetryPolicy, retryPolicy, policyDisabled, policyConstant, policyExponential)
	}
}
