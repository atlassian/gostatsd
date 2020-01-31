package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/atlassian/gostatsd/pkg/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	core_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core_v1inf "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/util/flowcontrol"
)

const (
	// ProviderName is the name of Kubernetes cloud provider.
	ProviderName        = "kubernetes"
	defaultResyncPeriod = 20 * time.Minute
	defaultAPIQPS       = 5
	apiQPSBurstFactor   = 1.5

	configFromFile      = "file"
	configFromInCluster = "in-cluster"

	resyncPeriodOption         = "resync_period"
	apiQPSOption               = "api_qps"
	clientConfigFromOption     = "client_config_from"
	clientConfigFileNameOption = "client_config_file_name"
	clientConfigContextOption  = "client_config_context"
)

// Provider represents a Kubernetes provider.
type Provider struct {
	//describeInstanceCount     uint64 // The cumulative number of times DescribeInstancesPagesWithContext has been called
	//describeInstanceInstances uint64 // The cumulative number of instances which have been fed in to DescribeInstancesPagesWithContext
	//describeInstancePages     uint64 // The cumulative number of pages from DescribeInstancesPagesWithContext
	//describeInstanceErrors    uint64 // The cumulative number of errors seen from DescribeInstancesPagesWithContext
	//describeInstanceFound     uint64 // The cumulative number of instances successfully found via DescribeInstancesPagesWithContext
	podByIp *BlockingIndex
	logger  logrus.FieldLogger
}

func (p *Provider) EstimatedTags() int {
	return 10 + 1 // 10 for EC2 tags, 1 for the region
}

func (p *Provider) RunMetrics(ctx context.Context, statser stats.Statser) {
	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			// These are namespaced not tagged because they're very specific
			//statser.Gauge("cloudprovider.kubernetes.describeinstancecount", float64(atomic.LoadUint64(&p.describeInstanceCount)), nil)
			//statser.Gauge("cloudprovider.kubernetes.describeinstanceinstances", float64(atomic.LoadUint64(&p.describeInstanceInstances)), nil)
			//statser.Gauge("cloudprovider.kubernetes.describeinstancepages", float64(atomic.LoadUint64(&p.describeInstancePages)), nil)
			//statser.Gauge("cloudprovider.kubernetes.describeinstanceerrors", float64(atomic.LoadUint64(&p.describeInstanceErrors)), nil)
			//statser.Gauge("cloudprovider.kubernetes.describeinstancefound", float64(atomic.LoadUint64(&p.describeInstanceFound)), nil)
		}
	}
}

// Instance returns instances details from Kubernetes.
// ip -> nil pointer if instance was not found.
// map is returned even in case of errors because it may contain partial data.
func (p *Provider) Instance(ctx context.Context, IP ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	// TODO
	return nil, nil
}

// MaxInstancesBatch returns maximum number of instances that could be requested via the Instance method.
func (p *Provider) MaxInstancesBatch() int {
	return 64 // this is arbitrary and could be more, but would not make sense.
}

// Name returns the name of the provider.
func (p *Provider) Name() string {
	return ProviderName
}

// SelfIP returns host's IPv4 address.
func (p *Provider) SelfIP() (gostatsd.IP, error) {
	// TODO use downward api to get the IP
	return gostatsd.UnknownIP, nil
}

// NewProviderFromOptions returns a new Kubernetes provider.
func NewProviderFromOptions(options gostatsd.Options) (gostatsd.CloudProvider, error) {
	v := util.GetSubViper(options.Viper, "kubernetes")
	v.SetDefault(resyncPeriodOption, defaultResyncPeriod)
	v.SetDefault(clientConfigFromOption, configFromInCluster)
	v.SetDefault(apiQPSOption, defaultAPIQPS)

	resyncPeriod := v.GetDuration(resyncPeriodOption)
	if resyncPeriod <= 0 {
		return nil, errors.New("resync period must be positive")
	}
	restConfig, err := loadRestClientConfig(v, options.UserAgent)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	podsInf := core_v1inf.NewPodInformer(clientset, meta_v1.NamespaceAll, resyncPeriod, cache.Indexers{})
	podByIp, err := NewBlockingGetByIndex(podsInf, podByIpIndexFunc)
	if err != nil {
		return nil, err
	}
	return &Provider{
		podByIp: podByIp,
		logger:  options.Logger,
	}, nil
}

func podByIpIndexFunc(obj interface{}) ([]string, error) {
	pod := obj.(*core_v1.Pod)
	ip := pod.Status.PodIP
	if ip == "" ||
		pod.Status.Phase == core_v1.PodSucceeded ||
		pod.Status.Phase == core_v1.PodFailed ||
		pod.DeletionTimestamp != nil {
		// Do not index irrelevant Pods
		return nil, nil
	}
	return []string{ip}, nil
}

func loadRestClientConfig(v *viper.Viper, userAgent string) (*rest.Config, error) {
	var config *rest.Config
	var err error

	clientConfigFrom := v.GetString(clientConfigFromOption)
	switch clientConfigFrom {
	case configFromInCluster:
		config, err = rest.InClusterConfig()
	case configFromFile:
		var configAPI *clientcmdapi.Config
		clientConfigFileName := v.GetString(clientConfigFileNameOption)
		if clientConfigFileName == "" {
			return nil, errors.New("'client config file' parameter must be set to a valid file name")
		}
		clientContext := v.GetString(clientConfigContextOption)
		if clientContext == "" {
			return nil, errors.New("'client config context' parameter must be set to a valid context name")
		}
		configAPI, err = clientcmd.LoadFromFile(clientConfigFileName)
		if err != nil {
			return nil, fmt.Errorf("failed to load REST client configuration from file %q: %v", clientConfigFileName, err)
		}
		config, err = clientcmd.NewDefaultClientConfig(*configAPI, &clientcmd.ConfigOverrides{
			CurrentContext: clientContext,
		}).ClientConfig()
	default:
		err = errors.New("invalid value for 'client config from' parameter")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load REST client configuration from %q: %v", clientConfigFrom, err)
	}

	apiQPS := v.GetFloat64(apiQPSOption)
	config.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(float32(apiQPS), int(apiQPS*apiQPSBurstFactor))
	config.UserAgent = userAgent
	return config, nil
}
