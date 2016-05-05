package cloudprovider

import (
	"fmt"
	"sync"
	"time"

	"github.com/atlassian/gostatsd/cloudprovider/providers/aws"
	cloudTypes "github.com/atlassian/gostatsd/cloudprovider/types"

	log "github.com/Sirupsen/logrus"
	"github.com/koding/cache"
	"github.com/spf13/viper"
)

// All registered cloud providers.
var providers = map[string]cloudTypes.Factory{
	aws.ProviderName: aws.NewProviderFromViper,
}

// GetCloudProvider creates an instance of the named provider, or nil if
// the name is not known.  The error return is only used if the named provider
// was known but failed to initialize.
func GetCloudProvider(name string, v *viper.Viper) (cloudTypes.Interface, error) {
	f, found := providers[name]
	if !found {
		return nil, nil
	}
	return f(v)
}

// InitCloudProvider creates an instance of the named cloud provider.
func InitCloudProvider(name string, v *viper.Viper) (cloudTypes.Interface, error) {
	if name == "" {
		log.Info("No cloud provider specified.")
		return nil, nil
	}

	provider, err := GetCloudProvider(name, v)
	if err != nil {
		return nil, fmt.Errorf("could not init cloud provider %q: %v", name, err)
	}
	if provider == nil {
		return nil, fmt.Errorf("unknown cloud provider %q", name)
	}
	log.Infof("Initialised cloud provider %q", name)

	return provider, nil
}

// TODO: review mutex e.g. RWMutex
var runningMutex sync.Mutex
var running = make(map[string]time.Time)
var instances = cache.NewMemoryWithTTL(1 * time.Hour)
var failed = cache.NewMemoryWithTTL(1 * time.Minute)

// GetInstance returns an instance from the cache or from the cloud provider.
func GetInstance(cloud cloudTypes.Interface, IP string) (instance *cloudTypes.Instance, err error) {
	iface, err := instances.Get(IP)
	if err == nil {
		instance = iface.(*cloudTypes.Instance)
		return instance, nil
	}

	if err != cache.ErrNotFound {
		// Better returning an error than hitting the cloud provider thousands of times per second
		return nil, err
	}

	cachedErr, err := failed.Get(IP)
	if err == nil {
		// We have a cached failure
		return nil, cachedErr.(error)
	}

	if err != cache.ErrNotFound {
		// Some error getting it from cache?
		return nil, err
	}

	runningMutex.Lock()
	last, ok := running[IP]
	runningMutex.Unlock()
	if ok {
		if last.Add(60 * time.Second).After(time.Now()) {
			time.Sleep(100 * time.Microsecond)
			return GetInstance(cloud, IP)
		}
	}
	runningMutex.Lock()
	running[IP] = time.Now()
	runningMutex.Unlock()
	defer func() {
		runningMutex.Lock()
		defer runningMutex.Unlock()
		delete(running, IP)
	}()

	if instance, err = cloud.Instance(IP); err != nil {
		failed.Set(IP, fmt.Errorf("Cached failure: %v for %s", err, IP))
		return nil, err
	}
	instances.Set(IP, instance)

	return instance, nil
}

func init() {
	instances.StartGC(1 * time.Minute)
	failed.StartGC(1 * time.Minute)
}
