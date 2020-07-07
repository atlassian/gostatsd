package k8s

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	core_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/internal/util"
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

	ipSinkSource   chan gostatsd.Source
	infoSinkSource chan gostatsd.InstanceInfo

	rw    sync.RWMutex // Protects cache
	cache map[gostatsd.Source]*gostatsd.Instance
}

func (p *Provider) IpSink() chan<- gostatsd.Source {
	return p.ipSinkSource
}

func (p *Provider) InfoSource() <-chan gostatsd.InstanceInfo {
	return p.infoSinkSource
}

func (p *Provider) EstimatedTags() int {
	// There is no real way to estimate this for k8s provider as any pod can have arbitrary labels/annotations
	return 0
}

func (p *Provider) Peek(ip gostatsd.Source) (*gostatsd.Instance, bool /*is a cache hit*/) {
	// it's always a cache hit
	return p.instanceFromCache(ip), true
}

func (p *Provider) Run(ctx context.Context) {
	p.logger.Debug("Starting informer cache")
	p.factory.Start(ctx.Done())
	var (
		infoSink   chan<- gostatsd.InstanceInfo
		info       gostatsd.InstanceInfo
		infoToSend []gostatsd.InstanceInfo
	)
	for {
		select {
		case <-ctx.Done():
			return
		case ip := <-p.ipSinkSource:
			infoToSend = append(infoToSend, gostatsd.InstanceInfo{
				IP:       ip,
				Instance: p.instanceFromCache(ip),
			})
		case infoSink <- info:
			info = gostatsd.InstanceInfo{} // enable GC
			infoSink = nil                 // info has been sent; if there is nothing to send, the case is disabled
		}
		if infoSink == nil && len(infoToSend) > 0 {
			last := len(infoToSend) - 1
			info = infoToSend[last]
			infoToSend[last] = gostatsd.InstanceInfo{} // enable GC
			infoToSend = infoToSend[:last]
			infoSink = p.infoSinkSource
		}
	}
}

func (p *Provider) instanceFromCache(ip gostatsd.Source) *gostatsd.Instance {
	p.rw.RLock()
	instance := p.cache[ip]
	p.rw.RUnlock()
	if instance != nil {
		// Instance found
		return instance
	}
	instance = p.instanceFromInformer(ip)
	// We are only holding the write lock while updating the cache. This may lead to concurrent calculations
	// but this is totally fine. Performance-wise this should be a rare event (Pod's info update).
	// Holding the lock around the whole block would prevent concurrent calculations but also ALL lookups.
	// This is unacceptable.
	p.rw.Lock()
	p.cache[ip] = instance
	p.rw.Unlock()
	return instance
}

func (p *Provider) instanceFromInformer(ip gostatsd.Source) *gostatsd.Instance {
	logger := p.logger.WithField("ip", ip)
	// Instance not found in cache. Fetch it from informer's cache and post-process.
	objs, err := p.podsInf.GetIndexer().ByIndex(PodsByIPIndexName, string(ip))
	if err != nil {
		logger.WithError(err).Error("got error from informer")
		return nil
	}
	if len(objs) < 1 {
		logger.Debug("Could not find IP in cache")
		return nil
	}
	if len(objs) > 1 {
		logger.Warn("More than one Pod in cache. Using first stored")
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
	logger.WithFields(logrus.Fields{
		"instance": instanceID,
		"tags":     tags,
	}).Debug("Added tags")
	return &gostatsd.Instance{
		ID:   gostatsd.Source(instanceID),
		Tags: tags,
	}
}

// NewProviderFromViper returns a new k8s provider.
func NewProviderFromViper(v *viper.Viper, logger logrus.FieldLogger, version string) (gostatsd.CachedInstances, error) {
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
	annotationRegex, labelRegex *regexp.Regexp) (*Provider, error) {

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
		fieldSelector := fields.OneTermEqualSelector("spec.nodeName", podInfOpts.NodeName).String()
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
	p := &Provider{
		logger:          logger,
		podsInf:         podsInf,
		factory:         factory,
		annotationRegex: annotationRegex,
		labelRegex:      labelRegex,
		ipSinkSource:    make(chan gostatsd.Source),
		infoSinkSource:  make(chan gostatsd.InstanceInfo),
		cache:           make(map[gostatsd.Source]*gostatsd.Instance),
	}
	podsInf.AddEventHandler(cacheInvalidationHandler{p: p})

	// TODO: we should emit prometheus metrics to fit in with the k8s ecosystem
	// TODO: we should emit events to the k8s API to fit in with the k8s ecosystem

	return p, nil
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

func isIndexablePod(pod *core_v1.Pod) bool {
	// Pod must have an IP, be running, and also not have the same IP as the host
	// If a pod is a HostNetwork pod then there could be multiple with the same IP sending stats, which breaks this
	// abstraction
	if pod.Status.PodIP == "" ||
		podIsFinishedRunning(pod) ||
		podIsHostNetwork(pod) {
		// Do not index irrelevant Pods
		return false
	}
	return true
}

func podByIpIndexFunc(obj interface{}) ([]string, error) {
	pod := obj.(*core_v1.Pod)
	// Pod must have an IP, be running, and also not have the same IP as the host
	// If a pod is a HostNetwork pod then there could be multiple with the same IP sending stats, which breaks this
	// abstraction
	if !isIndexablePod(pod) {
		// Do not index irrelevant Pods
		return nil, nil
	}
	return []string{pod.Status.PodIP}, nil
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

type cacheInvalidationHandler struct {
	p *Provider
}

func (e cacheInvalidationHandler) OnAdd(obj interface{}) {
	// Nothing to do
}

func (e cacheInvalidationHandler) OnUpdate(oldObj, newObj interface{}) {
	e.maybeInvalidateCacheForPod(oldObj.(*core_v1.Pod))
}

func (e cacheInvalidationHandler) OnDelete(obj interface{}) {
	pod, ok := obj.(*core_v1.Pod)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		pod, ok = tombstone.Obj.(*core_v1.Pod)
		if !ok {
			return
		}
	}
	e.maybeInvalidateCacheForPod(pod)
}

func (e cacheInvalidationHandler) maybeInvalidateCacheForPod(pod *core_v1.Pod) {
	if !isIndexablePod(pod) {
		// This Pod was not in the IP->Pod index so it could have not been used for lookups so it
		// should be ignored. Don't want to invalidate the cache for it's IP because there may be another Pod
		// with the same IP that is indexable, for which the cache holds something useful.
		return
	}
	e.p.rw.Lock()
	delete(e.p.cache, gostatsd.Source(pod.Status.PodIP))
	e.p.rw.Unlock()
}
