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
	userAgent = "test"

	ipAddr  = "127.0.0.1"
	ipAddr2 = "10.0.0.1"
)

var (
	FullyQualifiedAnnotationWhitelistParam = ProviderName + "." + ParamAnnotationTagWhitelist
	FullyQualifiedLabelWhitelistParam      = ProviderName + "." + ParamLabelTagWhitelist
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

type testFixtures struct {
	fakeClient    *mainFake.Clientset
	cloudProvider gostatsd.CloudProvider
	podsWatch     *watch.FakeWatcher
}

func setupTest(t *testing.T, test func(*testing.T, *testFixtures), v *viper.Viper, nn string) {
	fakeClient := mainFake.NewSimpleClientset()
	podsWatch := watch.NewFake()
	fakeClient.PrependWatchReactor("pods", kube_testing.DefaultWatchReactor(podsWatch, nil))

	options := gostatsd.CloudProviderOptions{
		Viper:    v,
		Logger:   logrus.StandardLogger(),
		Version:  userAgent,
		NodeName: nn,
	}
	cloudProvider, err := NewProvider(options, fakeClient)
	require.NoError(t, err)

	r, ok := cloudProvider.(gostatsd.Runner)
	assert.True(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		r.Run(ctx) // fork
	}()

	test(t, &testFixtures{
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
		name: "WithNoAnnotationTagWhitelist",
		viperParams: map[string]interface{}{
			FullyQualifiedAnnotationWhitelistParam: []string{},
		},
		pods:            []*core_v1.Pod{pod()},
		expectedNumTags: 0,
	},
	{
		name: "WithCustomAnnotationTagWhitelist",
		viperParams: map[string]interface{}{
			FullyQualifiedAnnotationWhitelistParam: []string{
				"^" + regexp.QuoteMeta("product.company.com/"),
			},
		},
		pods:                 []*core_v1.Pod{pod()},
		expectedNumTags:      1,
		expectedTagsContains: []tagTestValue{annotationTestValues[1]},
	},
	{
		name: "WithCustomAnnotationTagWhitelistMultipleRegex",
		viperParams: map[string]interface{}{
			FullyQualifiedAnnotationWhitelistParam: []string{
				"^" + regexp.QuoteMeta("product.company.com/"),
				"^" + regexp.QuoteMeta(AnnotationPrefix),
			},
		},
		pods:                 []*core_v1.Pod{pod()},
		expectedNumTags:      2,
		expectedTagsContains: []tagTestValue{annotationTestValues[0], annotationTestValues[1]},
	},
	{
		name: "WithCustomLabelTagWhitelist",
		viperParams: map[string]interface{}{
			FullyQualifiedLabelWhitelistParam: []string{"^app"},
		},
		pods:                 []*core_v1.Pod{pod()},
		expectedNumTags:      2, // we're still using the default annotation whitelist too
		expectedTagsContains: []tagTestValue{labelTestValues[0], annotationTestValues[0]},
	},
	{
		name: "WithCustomLabelTagWhitelistMultipleRegex",
		viperParams: map[string]interface{}{
			FullyQualifiedLabelWhitelistParam: []string{"^app", "^label"},
		},
		pods:                 []*core_v1.Pod{pod()},
		expectedNumTags:      3, // we're still using the default annotation whitelist too
		expectedTagsContains: []tagTestValue{labelTestValues[0], annotationTestValues[0], labelTestValues[1]},
	},
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
			setupTest(tt, func(ttt *testing.T, fixtures *testFixtures) {
				for _, p := range testCase.pods {
					fixtures.podsWatch.Add(p)
				}

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
	setupTest(t, func(t *testing.T, fixtures *testFixtures) {
		hostNetworkPod := pod()
		hostNetworkPod.Status.HostIP = ipAddr
		hostNetworkPod.Spec.HostNetwork = true
		fixtures.podsWatch.Add(hostNetworkPod)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		instanceData, err := fixtures.cloudProvider.Instance(ctx, ipAddr)
		require.NoError(t, err)

		instance := instanceData[ipAddr]
		assert.Nil(t, instance)
	}, viper.New(), nodeName)
}

func TestWatchNodeOnly(t *testing.T) {
	v := viper.New()
	v.Set(ProviderName+"."+ParamWatchCluster, false)

	setupTest(t, func(t *testing.T, fixtures *testFixtures) {
		expectedKey := "node"
		nodeAnnotationKey := AnnotationPrefix + expectedKey
		expectedValue := nodeName

		p := pod()
		p.Spec.NodeName = nodeName
		p.Status.PodIP = ipAddr
		p.ObjectMeta.Annotations = map[string]string{nodeAnnotationKey: expectedValue}
		p.ObjectMeta.Labels = map[string]string{}
		fixtures.podsWatch.Add(p)

		p = pod()
		p.Spec.NodeName = "node2"
		p.Status.PodIP = ipAddr2
		p.ObjectMeta.Annotations = map[string]string{nodeAnnotationKey: "node2"}
		p.ObjectMeta.Labels = map[string]string{}
		fixtures.podsWatch.Add(p)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		instanceData, err := fixtures.cloudProvider.Instance(ctx, ipAddr)
		require.NoError(t, err)

		instance := instanceData[ipAddr2]
		assert.Nil(t, instance, "expected pod with IP '%s' not to be indexed since it is on a different node to us", ipAddr2)

		instance = instanceData[ipAddr]
		assert.NotNil(t, instance, "expected pod with IP '%s' to be indexed since it is on our node", ipAddr)
		assert.Len(t, instance.Tags, 1)
		assert.Contains(t, instance.Tags, fmt.Sprintf("%s:%s", expectedKey, expectedValue))
	}, v, nodeName)
}

func TestWatchNodeFailsNoNodeName(t *testing.T) {
	v := viper.New()
	v.Set(ProviderName+"."+ParamWatchCluster, false)

	fakeClient := mainFake.NewSimpleClientset()
	podsWatch := watch.NewFake()
	fakeClient.PrependWatchReactor("pods", kube_testing.DefaultWatchReactor(podsWatch, nil))

	options := gostatsd.CloudProviderOptions{
		Viper:    v,
		Logger:   logrus.StandardLogger(),
		Version:  userAgent,
		NodeName: "",
	}
	_, err := NewProvider(options, fakeClient)
	require.Error(t, err, "creating k8s provider to watch node with no node name should fail")
}
