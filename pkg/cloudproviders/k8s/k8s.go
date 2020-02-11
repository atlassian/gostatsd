package k8s

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/util"

	"github.com/sirupsen/logrus"
	core_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core_v1inf "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"
)

var (
	// DefaultAnnotationTagWhitelist is the default ParamAnnotationTagWhitelist. Everything beginning with a standard
	// gostatsd string is included by default.
	DefaultAnnotationTagWhitelist = []string{"^" + regexp.QuoteMeta(AnnotationPrefix)}
	// DefaultLabelTagWhitelist is the default ParamLabelTagWhitelist. Every label is ignored by default.
	DefaultLabelTagWhitelist []string
)

const (
	// ProviderName is the name of the k8s cloud provider.
	ProviderName = "k8s"
	// PodsByIPIndexName is the name of the index function storing pods by IP.
	PodsByIPIndexName = "PodByIP"
	// AnnotationPrefix is the annotation prefix that is turned into tags by default.
	AnnotationPrefix = "gostatsd.atlassian.com/"

	// ParamAPIQPS is the maximum amount of queries per second we allow to the Kubernetes API server. This is
	// so we don't overwhelm the server with requests under load.
	ParamAPIQPS = "kube-api-qps"
	// ParamAPIQPSBurstFactor is the amount of queries per second we can burst above ParamAPIQPS.
	ParamAPIQPSBurstFactor = "kube-api-burst"
	// ParamAnnotationTagWhitelist is a list of regexes to check annotations against. Any pod annotations matching
	// this pattern will be included as tags on metrics emitted by that pod.
	ParamAnnotationTagWhitelist = "annotation-tag-whitelist"
	// ParamLabelTagWhitelist is a list of regexes to check labels against. Any pod labels matching this
	// pattern will be included as tags on metrics emitted by that pod.
	ParamLabelTagWhitelist = "label-tag-whitelist"
	// KubeconfigContextis the name of the context to use inside a provided ParamKubeconfigPath. If ParamKubeconfigPath
	// is unset this has no effect.
	ParamKubeconfigContext = "kubeconfig-context"
	// ParamKubeconfigPath is the path to the kubeconfig file to use for auth, or "" if using in-cluster auth.
	ParamKubeconfigPath = "kubeconfig-path"
	// ParamResyncPeriod is the resync period for the pod cache as a Duration.
	ParamResyncPeriod = "resync-period"
	// ParamWatchCluster is true if we should watch pods in the entire cluster, false if we should watch pods on our
	// own node.
	ParamWatchCluster = "watch-cluster"
	// UserAgent is the base user agent used when talking to the k8s API. A version is appended to this at runtime.
	UserAgent = "gostatsd"

	// DefaultAPIQPS is the default maximum amount of queries per second we allow to the Kubernetes API server.
	DefaultAPIQPS = 5
	// DefaultAPIQPSBurstFactor is the default amount of queries per second we can burst above the maximum QPS.
	DefaultAPIQPSBurstFactor = 1.5
	// DefaultKubeconfigContext is the default context to use inside the kubeconfig file. "" means use the current
	// context without switching.
	DefaultKubeconfigContext = ""
	// DefaultKubeconfigPath is the default path to the kubeconfig file. "" means use in-cluster auth.
	DefaultKubeconfigPath = ""
	// DefaultResyncPeriod is the default resync period for the pod cache.
	DefaultResyncPeriod = 5 * time.Minute
	// DefaultWatchCluster is the default watch mode for pods. By default we watch the entire cluster.
	DefaultWatchCluster = true
)

// Provider represents a k8s provider.
type Provider struct {
	logger logrus.FieldLogger

	podsInf             cache.SharedIndexInformer
	annotationWhitelist *regexp.Regexp
	labelWhitelist      *regexp.Regexp
}

func (p *Provider) EstimatedTags() int {
	// There is no real way to estimate this for k8s provider as any pod can have arbitrary labels/annotations
	return 0
}

// Instance returns pod details from k8s API server watches.
// ip -> nil pointer if pod was not found.
// map is returned even in case of errors because it may contain partial data.
// An "instance", as far as k8s cloud provider is concerned, is an individual pod running in the cluster
func (p *Provider) Instance(ctx context.Context, IP ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	instanceIPs := make(map[gostatsd.IP]*gostatsd.Instance, len(IP))
	var returnErr error

	// Lookup via the pod cache
	for _, lookupIP := range IP {
		if lookupIP == gostatsd.UnknownIP {
			instanceIPs[lookupIP] = nil
			continue
		}

		p.logger.WithField("ip", lookupIP).Debug("Looking up pod ip")
		objs, err := p.podsInf.GetIndexer().ByIndex(PodsByIPIndexName, string(lookupIP))
		if err != nil {
			instanceIPs[lookupIP] = nil
			returnErr = err
			continue
		}
		if len(objs) < 1 {
			p.logger.Debugf("Could not find IP in cache, continuing")
			instanceIPs[lookupIP] = nil
			continue
		}
		if len(objs) > 1 {
			p.logger.WithField("ip", lookupIP).Warn("More than one pod in cache. Using first stored")
		}
		pod := objs[0].(*core_v1.Pod)

		// Turn the pod metadata into tags
		var tags gostatsd.Tags
		// TODO: deduplicate labels and annotations in their tag format, rather than overwriting
		for k, v := range pod.ObjectMeta.Labels {
			if p.labelWhitelist.MatchString(k) {
				tags = append(tags, k+":"+v)
			}
		}
		for k, v := range pod.ObjectMeta.Annotations {
			if !p.annotationWhitelist.MatchString(k) {
				continue
			}
			// Annotations are often of the format "purpose.company.com/annotationName" so we want to split off the
			// start when processing tag names
			tagName := k
			splitTagName := strings.SplitN(k, "/", 2)
			if len(splitTagName) > 1 {
				tagName = splitTagName[1]
			}
			tags = append(tags, tagName+":"+v)
		}
		instanceID := pod.Namespace + "/" + pod.Name
		instanceIPs[lookupIP] = &gostatsd.Instance{
			ID:   instanceID,
			Tags: tags,
		}
		p.logger.WithFields(logrus.Fields{
			"instance": instanceID,
			"ip":       lookupIP,
			"tags":     tags,
		}).Debug("Added tags")
	}
	return instanceIPs, returnErr
}

// MaxInstancesBatch returns maximum number of instances that could be requested via the Instance method.
func (p *Provider) MaxInstancesBatch() int {
	// This is arbitrary since we have a local cache of information
	return 64
}

// Name returns the name of the provider.
func (p *Provider) Name() string {
	return ProviderName
}

// SelfIP returns host's IPv4 address.
func (p *Provider) SelfIP() (gostatsd.IP, error) {
	// This IP is only used for start/stop events of gostatsd. To simplify the k8s provider we have
	// chosen to just avoid finding the IP.
	return gostatsd.UnknownIP, nil
}

// NewProviderFromOptions returns a new k8s provider.
func NewProviderFromOptions(options gostatsd.CloudProviderOptions) (gostatsd.CloudProvider, error) {
	a := util.GetSubViper(options.Viper, "k8s")
	a.SetDefault(ParamKubeconfigContext, DefaultKubeconfigContext)
	a.SetDefault(ParamKubeconfigPath, DefaultKubeconfigPath)

	// Set up the k8s client
	var restConfig *rest.Config
	var err error
	kubeconfigPath := a.GetString(ParamKubeconfigPath)
	kubeconfigContext := a.GetString(ParamKubeconfigContext)
	if kubeconfigPath != "" {
		configOverrides := &clientcmd.ConfigOverrides{}
		if kubeconfigContext != "" {
			configOverrides.CurrentContext = kubeconfigContext
		}

		restConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath},
			configOverrides).ClientConfig()
	} else {
		restConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}

	apiQPS := a.GetFloat64(ParamAPIQPS)
	apiQPSBurstFactor := a.GetFloat64(ParamAPIQPSBurstFactor)
	restConfig.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(float32(apiQPS), int(apiQPS*apiQPSBurstFactor))
	restConfig.UserAgent = UserAgent + "/" + options.Version

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	return NewProvider(options, clientset)
}

// NewProvider returns a new k8s provider with the given k8s client configuration.
func NewProvider(options gostatsd.CloudProviderOptions, clientset kubernetes.Interface) (gostatsd.CloudProvider, error) {
	a := util.GetSubViper(options.Viper, "k8s")
	a.SetDefault(ParamAPIQPS, DefaultAPIQPS)
	a.SetDefault(ParamAPIQPSBurstFactor, DefaultAPIQPSBurstFactor)
	a.SetDefault(ParamAnnotationTagWhitelist, DefaultAnnotationTagWhitelist)
	a.SetDefault(ParamLabelTagWhitelist, DefaultLabelTagWhitelist)
	a.SetDefault(ParamResyncPeriod, DefaultResyncPeriod)
	a.SetDefault(ParamWatchCluster, DefaultWatchCluster)

	// Create the tag whitelists
	annotationWhitelist, err := stringListToRegex(a.GetStringSlice(ParamAnnotationTagWhitelist))
	if err != nil {
		return nil, err
	}
	labelWhitelist, err := stringListToRegex(a.GetStringSlice(ParamLabelTagWhitelist))
	if err != nil {
		return nil, err
	}

	// Set up informers
	indexers := cache.Indexers{
		PodsByIPIndexName: podByIpIndexFunc,
	}

	watchCluster := a.GetBool(ParamWatchCluster)
	customWatchOptions := func(*meta_v1.ListOptions) {}
	if !watchCluster {
		if options.NodeName == "" {
			return nil, fmt.Errorf("'%s' set to false, and node name not supplied", ParamWatchCluster)
		}
		customWatchOptions = func(lo *meta_v1.ListOptions) {
			lo.FieldSelector = fmt.Sprintf("spec.nodeName=%s", options.NodeName)
		}
	}

	podsInf := core_v1inf.NewFilteredPodInformer(
		clientset,
		meta_v1.NamespaceAll,
		a.GetDuration(ParamResyncPeriod),
		indexers,
		customWatchOptions)

	// TODO: we should emit prometheus metrics to fit in with the k8s ecosystem
	// TODO: we should emit events to the k8s API to fit in with the k8s ecosystem

	return &Provider{
		logger:              options.Logger,
		podsInf:             podsInf,
		annotationWhitelist: annotationWhitelist,
		labelWhitelist:      labelWhitelist,
	}, nil
}

func (p *Provider) Run(ctx context.Context) {
	p.podsInf.Run(ctx.Done())
}

func stringListToRegex(strList []string) (*regexp.Regexp, error) {
	if len(strList) == 0 {
		// This regex should always fail to match anything
		return regexp.Compile("(k^)")
	}

	combinedRegexes := "("
	for i, regexStr := range strList {
		combinedRegexes = combinedRegexes + regexStr
		if i < len(strList)-1 {
			combinedRegexes = combinedRegexes + "|"
		}
	}
	return regexp.Compile(combinedRegexes + ")")
}

func podByIpIndexFunc(obj interface{}) ([]string, error) {
	pod := obj.(*core_v1.Pod)
	ip := pod.Status.PodIP
	// Pod must have an IP, be running, and also not have the same IP as the host
	// If a pod is a HostNetwork pod then there could be multiple with the same IP sending stats, which breaks this
	// abstraction
	if ip == "" ||
		podIsFinishedRunning(pod) ||
		podIsHostNetwork(pod) {
		// Do not index irrelevant Pods
		return nil, nil
	}
	return []string{ip}, nil
}

func podIsHostNetwork(pod *core_v1.Pod) bool {
	return pod.Spec.HostNetwork || pod.Status.PodIP == pod.Status.HostIP
}

func podIsFinishedRunning(pod *core_v1.Pod) bool {
	return pod.Status.Phase == core_v1.PodSucceeded || pod.Status.Phase == core_v1.PodFailed || pod.DeletionTimestamp != nil
}
