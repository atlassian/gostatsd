package k8s

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/util"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	core_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	// DefaultAnnotationTagRegex is the default ParamAnnotationTagRegex. Everything beginning with a standard
	// gostatsd string is included by default. The default sets the tag name as the part after the "/", for example
	// gostatsd.atlassian.com/tag1 would return "tag1" as the tag name after matching.
	DefaultAnnotationTagRegex = fmt.Sprintf("^%s%s$", regexp.QuoteMeta(AnnotationPrefix), DefaultTagCaptureRegex)
	// DefaultTagCaptureRegex is a regex subexpression to capture from now until the end of the regex as the tag name.
	DefaultTagCaptureRegex = fmt.Sprintf("(?P<%s>.*)", TagNameRegexSubexp)
)

const (
	// ProviderName is the name of the k8s cloud provider.
	ProviderName = "k8s"
	// PodsByIPIndexName is the name of the index function storing pods by IP.
	PodsByIPIndexName = "PodByIP"
	// AnnotationPrefix is the annotation prefix that is turned into tags by default.
	AnnotationPrefix = "gostatsd.atlassian.com/"
	// TagNameRegexSubexp is the name of the regex subexpression that is used to parse tag names from label/annotation
	// names.
	TagNameRegexSubexp = "tag"

	// ParamAPIQPS is the maximum amount of queries per second we allow to the Kubernetes API server. This is
	// so we don't overwhelm the server with requests under load.
	ParamAPIQPS = "kube-api-qps"
	// ParamAPIQPSBurst is the amount of queries per second we can burst above ParamAPIQPS.
	ParamAPIQPSBurst = "kube-api-burst"
	// ParamAnnotationTagRegex is a regex to check annotations against. Any pod annotations matching
	// this pattern will be included as tags on metrics emitted by that pod. The tag name for these annotations will
	// be the capture group named "tag". "" means ignore all annotations.
	ParamAnnotationTagRegex = "annotation-tag-regex"
	// ParamLabelTagRegex is a list of regexes to check labels against. Any pod labels matching this
	// pattern will be included as tags on metrics emitted by that pod. The tag name for these labels will
	// be the capture group named "tag". "" means ignore all labels.
	ParamLabelTagRegex = "label-tag-regex"
	// KubeconfigContextis the name of the context to use inside a provided ParamKubeconfigPath. If ParamKubeconfigPath
	// is unset this has no effect.
	ParamKubeconfigContext = "kubeconfig-context"
	// ParamKubeconfigPath is the path to the kubeconfig file to use for auth, or "" if using in-cluster auth.
	ParamKubeconfigPath = "kubeconfig-path"
	// ParamNodeName is the Kubernetes node name of the node this is running on. This is only used if ParamWatchCluster
	// is set to false.
	ParamNodeName = "node-name"
	// ParamResyncPeriod is the resync period for the pod cache as a Duration.
	ParamResyncPeriod = "resync-period"
	// ParamUserAgent is the user agent used when talking to the k8s API.
	ParamUserAgent = "user-agent"
	// ParamWatchCluster is true if we should watch pods in the entire cluster, false if we should watch pods on our
	// own node.
	ParamWatchCluster = "watch-cluster"

	// DefaultAPIQPS is the default maximum amount of queries per second we allow to the Kubernetes API server.
	DefaultAPIQPS = 5
	// DefaultAPIQPSBurst is the default amount of queries per second we can burst above the maximum QPS.
	DefaultAPIQPSBurst = 10
	// DefaultKubeconfigContext is the default context to use inside the kubeconfig file. "" means use the current
	// context without switching.
	DefaultKubeconfigContext = ""
	// DefaultKubeconfigPath is the default path to the kubeconfig file. "" means use in-cluster auth.
	DefaultKubeconfigPath = ""
	// DefaultLabelTagRegex is the default ParamLabelTagRegex. Every label is ignored by default.
	DefaultLabelTagRegex = ""
	// DefaultNodeName is the default node name to watch pods on when ParamWatchCluster is false. Defaults to unset
	// as this will fail fast and alert users to set this appropriately.
	DefaultNodeName = ""
	// DefaultResyncPeriod is the default resync period for the pod cache.
	DefaultResyncPeriod = 5 * time.Minute
	// DefaultUserAgent is the default user agent used when talking to the k8s API.
	DefaultUserAgent = "gostatsd"
	// DefaultWatchCluster is the default watch mode for pods. By default we watch the entire cluster.
	DefaultWatchCluster = true
)

// PodInformerOptions represent options for a pod informer.
type PodInformerOptions struct {
	ResyncPeriod time.Duration
	WatchCluster bool
	NodeName     string
}

// Provider represents a k8s provider.
type Provider struct {
	logger logrus.FieldLogger

	podsInf         cache.SharedIndexInformer
	factory         informers.SharedInformerFactory
	annotationRegex *regexp.Regexp // can be nil to disable annotation matching
	labelRegex      *regexp.Regexp // can be nil to disable label matching
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
			p.logger.Debug("Could not find IP in cache, continuing")
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
		if p.labelRegex != nil {
			for k, v := range pod.ObjectMeta.Labels {
				tagName := getTagNameFromRegex(p.labelRegex, k)
				if tagName != "" {
					tags = append(tags, tagName+":"+v)
				}
			}
		}
		if p.annotationRegex != nil {
			for k, v := range pod.ObjectMeta.Annotations {
				tagName := getTagNameFromRegex(p.annotationRegex, k)
				if tagName != "" {
					tags = append(tags, tagName+":"+v)
				}
			}
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

// NewProviderFromViper returns a new k8s provider.
func NewProviderFromViper(v *viper.Viper, logger logrus.FieldLogger, version string) (gostatsd.CloudProvider, error) {
	k := util.GetSubViper(v, "k8s")
	setViperDefaults(k, version)

	// Set up the k8s client
	clientset, err := createKubernetesClient(k.GetString(ParamUserAgent), k.GetString(ParamKubeconfigPath),
		k.GetString(ParamKubeconfigContext), k.GetFloat64(ParamAPIQPS), k.GetFloat64(ParamAPIQPSBurst))
	if err != nil {
		return nil, err
	}
	var (
		annotationRegex *regexp.Regexp
		labelRegex      *regexp.Regexp
	)
	annotationTagRegex := k.GetString(ParamAnnotationTagRegex)
	if annotationTagRegex != "" {
		annotationRegex, err = regexp.Compile(annotationTagRegex)
		if err != nil {
			return nil, fmt.Errorf("bad annotation regex: %s: %v", annotationTagRegex, err)
		}
	}
	labelTagRegex := k.GetString(ParamLabelTagRegex)
	if labelTagRegex != "" {
		labelRegex, err = regexp.Compile(labelTagRegex)
		if err != nil {
			return nil, fmt.Errorf("bad label regex: %s: %v", labelTagRegex, err)
		}
	}
	return NewProvider(
		logger,
		clientset,
		PodInformerOptions{
			ResyncPeriod: k.GetDuration(ParamResyncPeriod),
			WatchCluster: k.GetBool(ParamWatchCluster),
			NodeName:     k.GetString(ParamNodeName),
		},
		annotationRegex,
		labelRegex)
}

// NewProvider returns a new k8s provider.
// annotationRegex and/or labelRegex can be nil to disable annotation/label matching.
func NewProvider(logger logrus.FieldLogger, clientset kubernetes.Interface, podInfOpts PodInformerOptions,
	annotationRegex, labelRegex *regexp.Regexp) (gostatsd.CloudProvider, error) {

	// This list operation is for debugging purposes and failing fast. If this fails then the cache will likely fail.
	_, err := clientset.CoreV1().Pods(meta_v1.NamespaceAll).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// If we're not watching the entire cluster we need to limit our watch to pods with our node name
	customWatchOptions := func(*meta_v1.ListOptions) {}
	if !podInfOpts.WatchCluster {
		if podInfOpts.NodeName == "" {
			return nil, fmt.Errorf("watch-cluster set to false, and node name not supplied")
		}
		fieldSelector := "spec.nodeName=" + podInfOpts.NodeName
		logger.WithField("fieldSelector", fieldSelector).Debug("set fieldSelector for informers")
		customWatchOptions = func(lo *meta_v1.ListOptions) {
			lo.FieldSelector = fieldSelector
		}
	}

	// Create a shared informer factory that watches the correct nodes
	factory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		podInfOpts.ResyncPeriod,
		informers.WithTweakListOptions(customWatchOptions))

	// Set up the pod informer which fills an index with the pods we care about
	podsInf := factory.Core().V1().Pods().Informer()
	indexers := cache.Indexers{
		PodsByIPIndexName: podByIpIndexFunc,
	}
	err = podsInf.AddIndexers(indexers)
	if err != nil {
		return nil, err
	}

	// TODO: we should emit prometheus metrics to fit in with the k8s ecosystem
	// TODO: we should emit events to the k8s API to fit in with the k8s ecosystem

	return &Provider{
		logger:          logger,
		podsInf:         podsInf,
		factory:         factory,
		annotationRegex: annotationRegex,
		labelRegex:      labelRegex,
	}, nil
}

func (p *Provider) Run(ctx context.Context) {
	p.logger.Debug("Starting informer cache")
	p.factory.Start(ctx.Done())
}

func createKubernetesClient(userAgent, kubeconfigPath, kubeconfigContext string, apiQPS, apiQPSBurst float64) (kubernetes.Interface, error) {
	var restConfig *rest.Config
	var err error
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

	restConfig.QPS = float32(apiQPS)
	restConfig.Burst = int(apiQPSBurst)
	restConfig.UserAgent = userAgent

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	return clientset, nil
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

func setViperDefaults(v *viper.Viper, version string) {
	v.SetDefault(ParamAPIQPS, DefaultAPIQPS)
	v.SetDefault(ParamAPIQPSBurst, DefaultAPIQPSBurst)
	v.SetDefault(ParamAnnotationTagRegex, DefaultAnnotationTagRegex)
	v.SetDefault(ParamKubeconfigContext, DefaultKubeconfigContext)
	v.SetDefault(ParamKubeconfigPath, DefaultKubeconfigPath)
	v.SetDefault(ParamLabelTagRegex, DefaultLabelTagRegex)
	// This is intended to be taken in primarily as an environment variable when running inside k8s, as that is the
	// k8s standard way of providing variable information to pods via the downwards API.
	// See: https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/
	v.SetDefault(ParamNodeName, DefaultNodeName)
	v.SetDefault(ParamResyncPeriod, DefaultResyncPeriod)
	v.SetDefault(ParamUserAgent, DefaultUserAgent+"/"+version)
	v.SetDefault(ParamWatchCluster, DefaultWatchCluster)
}

// getTagNameFromRegex gets a tag name from the regex. This is either the entire input string, or a subset matching
// the "tag" capture group if it exists and matched. Returns "" if the regex matched nothing.
func getTagNameFromRegex(re *regexp.Regexp, s string) string {
	match := re.FindStringSubmatch(s)
	if match == nil {
		return ""
	}
	// The first subexp name is always "" and is the entire regex, so go in reverse to find the tag name subexp first
	for i, subexpName := range re.SubexpNames() {
		if subexpName == TagNameRegexSubexp && match[i] != "" {
			return match[i] // the matching text of the tag name subexp
		}
	}
	// If the regex matched as a whole then match[0] will be the matching text, otherwise ""
	if match[0] != "" {
		return s
	}
	return ""
}
