package k8s

import (
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/ash2k/stager"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	mainFake "k8s.io/client-go/kubernetes/fake"
	kube_testing "k8s.io/client-go/testing"

	"github.com/atlassian/gostatsd"
)

const (
	namespace = "foo"
	podName1  = "pod1"
	podName2  = "pod2"
	nodeName  = "node1"

	ipAddr  = "127.0.0.1"
	ipAddr2 = "10.0.0.1"
)

type tagTestValue struct {
	key         string // the key of the annotation/label
	expectedKey string // the expected key of the tag
	value       string // the value of the annotation/label & tag
}

var (
	annotationTestValues = []tagTestValue{
		{AnnotationPrefix + "tag1", "tag1", "value1"},
		{"product.company.com/tag2", "tag2", "value2"},
		{"tag3", "tag3", "value3"},
	}
	labelTestValues = []tagTestValue{
		{"app", "app", "testApp"},
		{"label", "label", "value"},
	}
)

func pod() *core_v1.Pod {
	annotations := map[string]string{}
	for _, atv := range annotationTestValues {
		annotations[atv.key] = atv.value
	}
	labels := map[string]string{}
	for _, atv := range labelTestValues {
		labels[atv.key] = atv.value
	}

	return &core_v1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:        podName1,
			Namespace:   namespace,
			Annotations: annotations,
			Labels:      labels,
		},
		Status: core_v1.PodStatus{
			PodIP: ipAddr,
		},
	}
}

func pod2() *core_v1.Pod {
	// No labels/annotations for this pod
	annotations := map[string]string{}
	labels := map[string]string{}

	return &core_v1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:        podName2,
			Namespace:   namespace,
			Annotations: annotations,
			Labels:      labels,
		},
		Status: core_v1.PodStatus{
			PodIP: ipAddr2,
		},
	}
}

type testFixture struct {
	fakeClient *mainFake.Clientset
	provider   *Provider
	podsWatch  *watch.FakeWatcher
}

func setupTest(t *testing.T, test func(*testing.T, *testFixture), v *viper.Viper, nn string) {
	fakeClient := mainFake.NewSimpleClientset()
	podsWatch := watch.NewFake()
	fakeClient.PrependWatchReactor("pods", kube_testing.DefaultWatchReactor(podsWatch, nil))

	// We have to set these to the defaults manually here as we're sidestepping the Viper creation path
	// to inject things
	setViperDefaults(v, "test")

	cloudProvider, err := NewProvider(
		logrus.StandardLogger(),
		fakeClient,
		PodInformerOptions{
			ResyncPeriod: 0, // Disable resync to simplify testing
			WatchCluster: v.GetBool(ParamWatchCluster),
			NodeName:     nn,
		},
		regexp.MustCompile(v.GetString(ParamAnnotationTagRegex)),
		regexp.MustCompile(v.GetString(ParamLabelTagRegex)),
	)
	require.NoError(t, err)
	stgr := stager.New()
	defer stgr.Shutdown()
	stgr.NextStage().StartWithContext(cloudProvider.Run) // run the cloud provider

	test(t, &testFixture{
		fakeClient: fakeClient,
		provider:   cloudProvider,
		podsWatch:  podsWatch,
	})
}

type tagTest struct {
	name                 string
	viperParams          map[string]interface{}
	pods                 []*core_v1.Pod
	expectedNumTags      int
	expectedTagsContains []tagTestValue
}

var ipTagTests = []tagTest{
	{
		name:                 "WithDefaultTagWhitelists",
		pods:                 []*core_v1.Pod{pod()},
		expectedNumTags:      1,
		expectedTagsContains: []tagTestValue{annotationTestValues[0]},
	},
	// The same test as above but with two pods to choose from
	{
		name:                 "WhenMultiplePods",
		pods:                 []*core_v1.Pod{pod(), pod2()},
		expectedNumTags:      1,
		expectedTagsContains: []tagTestValue{annotationTestValues[0]},
	},
	{
		name: "WithCustomAnnotationTagWhitelist",
		viperParams: map[string]interface{}{
			ParamAnnotationTagRegex: fmt.Sprintf("^%s%s$", regexp.QuoteMeta("product.company.com/"), DefaultTagCaptureRegex),
		},
		pods:                 []*core_v1.Pod{pod()},
		expectedNumTags:      1,
		expectedTagsContains: []tagTestValue{annotationTestValues[1]},
	},
	{
		name: "WithCustomAnnotationTagWhitelistMultipleRegex",
		viperParams: map[string]interface{}{
			ParamAnnotationTagRegex: fmt.Sprintf(
				"^(%s|%s)%s$",
				regexp.QuoteMeta("product.company.com/"),
				regexp.QuoteMeta(AnnotationPrefix),
				DefaultTagCaptureRegex),
		},
		pods:                 []*core_v1.Pod{pod()},
		expectedNumTags:      2,
		expectedTagsContains: []tagTestValue{annotationTestValues[0], annotationTestValues[1]},
	},
	{
		name: "WithCustomLabelTagWhitelist",
		viperParams: map[string]interface{}{
			ParamLabelTagRegex: "^app",
		},
		pods:                 []*core_v1.Pod{pod()},
		expectedNumTags:      2, // we're still using the default annotation whitelist too
		expectedTagsContains: []tagTestValue{labelTestValues[0], annotationTestValues[0]},
	},
	{
		name: "WithCustomLabelTagWhitelistMultipleRegex",
		viperParams: map[string]interface{}{
			ParamLabelTagRegex: "^(app|label)",
		},
		pods:                 []*core_v1.Pod{pod()},
		expectedNumTags:      3, // we're still using the default annotation whitelist too
		expectedTagsContains: []tagTestValue{labelTestValues[0], annotationTestValues[0], labelTestValues[1]},
	},
}

func (f *testFixture) waitForCacheSize(t *testing.T, numExpectedPods int) {
	// Wait for the cache to fill up before moving on
	indexer := f.provider.podsInf.GetIndexer()
	for i := 1; i < 100 && len(indexer.List()) != numExpectedPods; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	require.Len(t, indexer.List(), numExpectedPods)
}

func TestIPToTags(t *testing.T) {
	t.Parallel()

	for _, testCase := range ipTagTests {
		flags := viper.New()
		for k, v := range testCase.viperParams {
			flags.Set(k, v)
		}

		// Run tests in their own namespace for clarity
		t.Run(testCase.name, func(tt *testing.T) {
			setupTest(tt, func(ttt *testing.T, fixtures *testFixture) {
				for _, p := range testCase.pods {
					fixtures.podsWatch.Add(p)
				}
				fixtures.waitForCacheSize(t, len(testCase.pods))

				// Run the test
				instance, cacheHit := fixtures.provider.Peek(ipAddr)
				require.True(ttt, cacheHit)
				require.NotNil(ttt, instance)
				assert.Equal(ttt, instance.ID, fmt.Sprintf("%s/%s", namespace, podName1))
				assert.Len(ttt, instance.Tags, testCase.expectedNumTags)
				for _, expectedTag := range testCase.expectedTagsContains {
					assert.Contains(ttt, instance.Tags, fmt.Sprintf("%s:%s", expectedTag.expectedKey, expectedTag.value))
				}
			}, flags, nodeName)
		})
	}
}

func TestNoHostNetworkPodsCached(t *testing.T) {
	t.Parallel()

	setupTest(t, func(t *testing.T, fixtures *testFixture) {
		hostNetworkPod := pod()
		hostNetworkPod.Status.HostIP = ipAddr
		hostNetworkPod.Spec.HostNetwork = true
		fixtures.podsWatch.Add(hostNetworkPod)
		fixtures.waitForCacheSize(t, 1)

		instance, cacheHit := fixtures.provider.Peek(ipAddr)
		require.True(t, cacheHit)
		require.Nil(t, instance)
	}, viper.New(), nodeName)
}

func TestWatchNodeFailsNoNodeName(t *testing.T) {
	t.Parallel()

	fakeClient := mainFake.NewSimpleClientset()
	podsWatch := watch.NewFake()
	fakeClient.PrependWatchReactor("pods", kube_testing.DefaultWatchReactor(podsWatch, nil))

	_, err := NewProvider(
		logrus.StandardLogger(),
		fakeClient,
		PodInformerOptions{
			ResyncPeriod: 0,     // Disable resync to simplify testing
			WatchCluster: false, // the important bit
			NodeName:     "",
		},
		regexp.MustCompile(DefaultAnnotationTagRegex),
		regexp.MustCompile(DefaultLabelTagRegex),
	)
	require.Error(t, err, "creating k8s provider to watch node with no node name should fail")
}

func TestGetTagNameFromRegex(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		str             string
		re              string
		expectedTagName string
	}{
		"NoMatchNoTagCaptureGroup":   {"donotmatch", "aaa", ""},
		"NoMatchWithTagCaptureGroup": {"donotmatch", "(?P<tag>aaa)", ""},
		"MatchNoTagCaptureGroup":     {"matchthis", "match", "matchthis"},
		"MatchWithTagCaptureGroup":   {"matchthis", "(?P<tag>match)", "match"},
		"NoMatchWithEmptyRegex":      {"matchthis", "", ""},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			tagName := getTagNameFromRegex(regexp.MustCompile(testCase.re), testCase.str)
			assert.Equal(t, testCase.expectedTagName, tagName)
		})
	}
}

// Tests that internal cache is cleared when Pod is removed from informer.
func TestCacheInvalidation1(t *testing.T) {
	t.Parallel()

	setupTest(t, func(t *testing.T, fixtures *testFixture) {
		p1 := pod()
		fixtures.podsWatch.Add(p1)
		fixtures.waitForCacheSize(t, 1)

		instance, cacheHit := fixtures.provider.Peek(ipAddr)
		require.True(t, cacheHit)
		require.NotNil(t, instance)
		func() {
			fixtures.provider.rw.RLock()
			defer fixtures.provider.rw.RUnlock()
			require.Len(t, fixtures.provider.cache, 1)
			require.Contains(t, fixtures.provider.cache, gostatsd.Source(ipAddr))
		}()
		fixtures.podsWatch.Delete(p1)
		fixtures.waitForCacheSize(t, 0)
		// Even though the informer's cache is empty, the listener may not have been called yet.
		// Give it some time.
		require.NoError(t, wait.Poll(100*time.Millisecond, 10*time.Second, func() (done bool, err error) {
			fixtures.provider.rw.RLock()
			defer fixtures.provider.rw.RUnlock()
			l := len(fixtures.provider.cache)
			t.Logf("Number of Pods in internal cache: %d", l)
			return l == 0, nil
		}))
	}, viper.New(), nodeName)
}

// Tests that internal cache is not cleared when a non-indexable Pod is removed from informer.
func TestCacheInvalidation2(t *testing.T) {
	t.Parallel()

	setupTest(t, func(t *testing.T, fixtures *testFixture) {
		p1 := pod()
		fixtures.podsWatch.Add(p1)
		p2 := pod2()
		p2.Status.PodIP = p1.Status.PodIP
		p2.Status.Phase = core_v1.PodSucceeded
		require.False(t, isIndexablePod(p2))
		fixtures.podsWatch.Add(p2)
		fixtures.waitForCacheSize(t, 2)

		instance, cacheHit := fixtures.provider.Peek(ipAddr)
		require.True(t, cacheHit)
		require.NotNil(t, instance)

		onePodInCache := func() (done bool, err error) {
			fixtures.provider.rw.RLock()
			defer fixtures.provider.rw.RUnlock()
			l := len(fixtures.provider.cache)
			t.Logf("Number of Pods in internal cache: %d", l)
			pod := fixtures.provider.cache[ipAddr]
			return l == 1 && pod != nil, nil
		}
		require.NoError(t, wait.Poll(100*time.Millisecond, 10*time.Second, onePodInCache))
		fixtures.podsWatch.Delete(p2)
		fixtures.waitForCacheSize(t, 1)
		// Even though the informer's cache is empty, the listener may not have been called yet.
		// Give it some time.
		require.NoError(t, wait.Poll(100*time.Millisecond, 10*time.Second, onePodInCache))
	}, viper.New(), nodeName)
}
