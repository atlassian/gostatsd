package k8s

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/atlassian/gostatsd"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	mainFake "k8s.io/client-go/kubernetes/fake"
	kube_testing "k8s.io/client-go/testing"
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
	fakeClient    *mainFake.Clientset
	cloudProvider gostatsd.CloudProvider
	podsWatch     *watch.FakeWatcher
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
			ResyncPeriod: v.GetDuration(ParamResyncPeriod),
			WatchCluster: v.GetBool(ParamWatchCluster),
			NodeName:     nn,
		},
		regexp.MustCompile(v.GetString(ParamAnnotationTagRegex)),
		regexp.MustCompile(v.GetString(ParamLabelTagRegex)),
	)
	require.NoError(t, err)

	r, ok := cloudProvider.(gostatsd.Runner)
	require.True(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Run(ctx) // run the cloud provider

	test(t, &testFixture{
		fakeClient:    fakeClient,
		cloudProvider: cloudProvider,
		podsWatch:     podsWatch,
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

func waitForFullCache(t *testing.T, fixtures *testFixture, numExpectedPods int) {
	// Wait for the cache to fill up before moving on
	cp, ok := fixtures.cloudProvider.(*Provider)
	require.True(t, ok, "fixture must produce a k8s.Provider")
	for i := 1; i < 100 && len(cp.podsInf.GetIndexer().List()) < numExpectedPods; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, numExpectedPods, len(cp.podsInf.GetIndexer().List()))
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
				waitForFullCache(t, fixtures, len(testCase.pods))

				// Run the test
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				instanceData, err := fixtures.cloudProvider.Instance(ctx, ipAddr)
				require.NoError(ttt, err)

				instance := instanceData[ipAddr]
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
	setupTest(t, func(t *testing.T, fixtures *testFixture) {
		hostNetworkPod := pod()
		hostNetworkPod.Status.HostIP = ipAddr
		hostNetworkPod.Spec.HostNetwork = true
		fixtures.podsWatch.Add(hostNetworkPod)
		waitForFullCache(t, fixtures, 1)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		instanceData, err := fixtures.cloudProvider.Instance(ctx, ipAddr)
		require.NoError(t, err)

		instance := instanceData[ipAddr]
		require.Nil(t, instance)
	}, viper.New(), nodeName)
}

func TestWatchNodeOnly(t *testing.T) {
	v := viper.New()
	v.Set(ParamWatchCluster, false)

	setupTest(t, func(t *testing.T, fixtures *testFixture) {
		expectedKey := "node"
		nodeAnnotationKey := AnnotationPrefix + expectedKey
		expectedValue := nodeName

		p := pod()
		p.Spec.NodeName = nodeName
		p.ObjectMeta.Annotations = map[string]string{nodeAnnotationKey: expectedValue}
		p.ObjectMeta.Labels = map[string]string{}
		fixtures.podsWatch.Add(p)

		p = pod2()
		p.Spec.NodeName = "node2"
		p.ObjectMeta.Annotations = map[string]string{nodeAnnotationKey: "node2"}
		p.ObjectMeta.Labels = map[string]string{}
		fixtures.podsWatch.Add(p)

		waitForFullCache(t, fixtures, 2)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		instanceData, err := fixtures.cloudProvider.Instance(ctx, ipAddr)
		require.NoError(t, err)

		instance := instanceData[ipAddr2]
		require.Nil(t, instance, "expected pod with IP '%s' not to be indexed since it is on a different node to us", ipAddr2)

		instance = instanceData[ipAddr]
		require.NotNil(t, instance, "expected pod with IP '%s' to be indexed since it is on our node", ipAddr)
		assert.Len(t, instance.Tags, 1)
		assert.Contains(t, instance.Tags, fmt.Sprintf("%s:%s", expectedKey, expectedValue))
	}, v, nodeName)
}

func TestWatchNodeFailsNoNodeName(t *testing.T) {
	fakeClient := mainFake.NewSimpleClientset()
	podsWatch := watch.NewFake()
	fakeClient.PrependWatchReactor("pods", kube_testing.DefaultWatchReactor(podsWatch, nil))

	_, err := NewProvider(
		logrus.StandardLogger(),
		fakeClient,
		PodInformerOptions{
			ResyncPeriod: DefaultResyncPeriod,
			WatchCluster: false, // the important bit
			NodeName:     "",
		},
		regexp.MustCompile(DefaultAnnotationTagRegex),
		regexp.MustCompile(DefaultLabelTagRegex),
	)
	require.Error(t, err, "creating k8s provider to watch node with no node name should fail")
}

func TestGetTagNameFromRegex(t *testing.T) {
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
