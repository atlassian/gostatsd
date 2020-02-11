package k8s

import (
	"context"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"time"

	"k8s.io/client-go/util/flowcontrol"

	"github.com/ash2k/stager/wait"

	"k8s.io/client-go/tools/clientcmd"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/util"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	core_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core_v1inf "k8s.io/client-go/informers/core/v1"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

var (
	// DefaultAnnotationTagWhitelist is the default ParamAnnotationTagWhitelist. Everything beginning with a standard
	// gostatsd string is included by default.
	DefaultAnnotationTagWhitelist = []string{
		fmt.Sprintf("^%s", AnnotationPrefix),
	}
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
	// NodeNameEnvVar is the name of the environment variable that the k8s cloud provider will find it's node name in.
	NodeNameEnvVar = "GOSTATSD_NODE_NAME"

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
	// ParamKubeconfigPath is the path to the kubeconfig file to use for auth, or none if using in-cluster auth.
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
	// DefaultKubeconfigContext is the default context to use inside the kubeconfig file. None means use the current
	// context without switching.
	DefaultKubeconfigContext = ""
	// DefaultKubeconfigPath is the default path to the kubeconfig file. None means use in-cluster auth.
	DefaultKubeconfigPath = ""
	// DefaultResyncPeriod is the default resync period for the pod cache.
	DefaultResyncPeriod = 5 * time.Minute
	// DefaultWatchCluster is the default watch mode for pods. By default we watch the entire cluster.
	DefaultWatchCluster = true
)

// Provider represents a k8s provider.
type Provider struct {
	logger logrus.FieldLogger

	PodsInf             cache.SharedIndexInformer
	annotationWhitelist []regexp.Regexp
	labelWhitelist      []regexp.Regexp
}

func (p *Provider) EstimatedTags() int {
	// There is no real way to estimate this for k8s provider as any pod can have arbitrary labels/annotations
	return 8
}

// Instance returns pod details from k8s API server watches.
// ip -> nil pointer if pod was not found.
// map is returned even in case of errors because it may contain partial data.
// An "instance", as far as k8s cloud provider is concerned, is an individual pod running in the cluster
func (p *Provider) Instance(ctx context.Context, IP ...gostatsd.IP) (map[gostatsd.IP]*gostatsd.Instance, error) {
	instanceIPs := map[gostatsd.IP]*gostatsd.Instance{}
	var returnErr error

	// Lookup via the pod cache
	for _, lookupIP := range IP {
		if lookupIP == gostatsd.UnknownIP {
			continue
		}

		p.logger.WithField("ip", lookupIP).Debug("Looking up pod ip")
		objs, err := p.PodsInf.GetIndexer().ByIndex(PodsByIPIndexName, string(lookupIP))
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
			p.logger.Warn("More than one pod in cache for IP '%s'. Using first stored", lookupIP)
		}
		pod := objs[0].(*core_v1.Pod)

		// Turn the pod metadata into tags
		tags := gostatsd.Tags{}
		// TODO: deduplicate labels and annotations in their tag format, rather than overwriting
		for k, v := range pod.ObjectMeta.Labels {
			if tagKeyMatchesWhitelist(k, p.labelWhitelist) {
				tags = append(tags, fmt.Sprintf("%s:%s", k, v))
			}
		}
		for k, v := range pod.ObjectMeta.Annotations {
			if !tagKeyMatchesWhitelist(k, p.annotationWhitelist) {
				continue
			}
			// Annotations are often of the format "purpose.company.com/annotationName" so we want to expectedKey off the
			// start when processing tag names
			tagName := k
			splitTagName := strings.SplitN(k, "/", 2)
			if len(splitTagName) > 1 {
				tagName = splitTagName[1]
			}
			tags = append(tags, fmt.Sprintf("%s:%s", tagName, v))
		}
		instanceID := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
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
	// Get our pod's IP address from the network stack
	podSelfIP, err := getSelfIPAddress()
	if err != nil {
		return gostatsd.UnknownIP, err
	}
	return podSelfIP, nil
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
		configOverrides := &clientcmd.ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: ""}}
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
	annotationWhitelist, err := stringListToRegexList(a.GetStringSlice(ParamAnnotationTagWhitelist))
	if err != nil {
		return nil, err
	}
	labelWhitelist, err := stringListToRegexList(a.GetStringSlice(ParamLabelTagWhitelist))
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
			return nil, fmt.Errorf(
				"'%s' set to false, and node name could not be found from environment variable '%s'",
				ParamWatchCluster, NodeNameEnvVar)
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
		PodsInf:             podsInf,
		annotationWhitelist: annotationWhitelist,
		labelWhitelist:      labelWhitelist,
		logger:              options.Logger,
	}, nil
}

func (p *Provider) Run(ctx context.Context) {
	var wg wait.Group
	defer wg.Wait()

	informerCtx, informerCancel := context.WithCancel(ctx)
	defer informerCancel()

	wg.StartWithChannel(informerCtx.Done(), p.PodsInf.Run) // Fork

	// Work until asked to stop
	<-ctx.Done()
}

func GetNodeName() string {
	// To ensure we get this information in a timely manner this MUST be passed down to the pod via an environment
	// variable using the downwards API
	// See: https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/
	return os.Getenv(NodeNameEnvVar)
}

func tagKeyMatchesWhitelist(tagKey string, whitelist []regexp.Regexp) bool {
	for _, r := range whitelist {
		if r.MatchString(tagKey) {
			return true
		}
	}
	return false
}

func stringListToRegexList(strList []string) ([]regexp.Regexp, error) {
	var regexList []regexp.Regexp
	for _, str := range strList {
		regex, err := regexp.Compile(str)
		if err != nil {
			return nil, err
		}
		regexList = append(regexList, *regex)
	}
	return regexList, nil
}

func getSelfIPAddress() (gostatsd.IP, error) {
	// In Kubernetes, the IP address of eth0 will be the IP address of the pod running this application
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, i := range interfaces {
		if i.Name != "eth0" {
			continue
		}
		addrs, err := i.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil {
				continue
			}
			// process IP address
			return gostatsd.IP(ip.String()), nil
		}
	}
	return "", fmt.Errorf("no eth0 interface address found")
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
