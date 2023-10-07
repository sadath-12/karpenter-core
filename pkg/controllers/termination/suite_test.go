/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package termination_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"k8s.io/client-go/tools/record"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/apis"
	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/apis/v1beta1"
	"github.com/aws/karpenter-core/pkg/cloudprovider/fake"
	"github.com/aws/karpenter-core/pkg/controllers/termination"
	"github.com/aws/karpenter-core/pkg/controllers/termination/terminator"
	"github.com/aws/karpenter-core/pkg/events"
	"github.com/aws/karpenter-core/pkg/metrics"
	"github.com/aws/karpenter-core/pkg/operator/controller"
	"github.com/aws/karpenter-core/pkg/operator/scheme"
	"github.com/aws/karpenter-core/pkg/test"
	nodeclaimutil "github.com/aws/karpenter-core/pkg/utils/nodeclaim"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	. "knative.dev/pkg/logging/testing"

	. "github.com/aws/karpenter-core/pkg/test/expectations"
)

var ctx context.Context
var terminationController controller.Controller
var evictionQueue *terminator.EvictionQueue
var env *test.Environment
var defaultOwnerRefs = []metav1.OwnerReference{{Kind: "ReplicaSet", APIVersion: "appsv1", Name: "rs", UID: "1234567890"}}
var fakeClock *clock.FakeClock
var cloudProvider *fake.CloudProvider

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Termination")
}

var _ = BeforeSuite(func() {
	fakeClock = clock.NewFakeClock(time.Now())
	env = test.NewEnvironment(scheme.Scheme, test.WithCRDs(apis.CRDs...), test.WithFieldIndexers(test.MachineFieldIndexer(ctx), test.NodeClaimFieldIndexer(ctx)))

	cloudProvider = fake.NewCloudProvider()
	evictionQueue = terminator.NewEvictionQueue(ctx, env.KubernetesInterface.CoreV1(), events.NewRecorder(&record.FakeRecorder{}))
	terminationController = termination.NewController(env.Client, cloudProvider, terminator.NewTerminator(fakeClock, env.Client, evictionQueue), events.NewRecorder(&record.FakeRecorder{}))
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Termination", func() {
	var node *v1.Node
	var nodeClaim *v1beta1.NodeClaim
	var machine *v1alpha5.Machine

	BeforeEach(func() {
		nodeClaim, node = test.NodeClaimAndNode(v1beta1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Finalizers: []string{v1beta1.TerminationFinalizer}}})
		machine = test.Machine(v1alpha5.Machine{ObjectMeta: metav1.ObjectMeta{Finalizers: []string{v1beta1.TerminationFinalizer}}, Status: v1alpha5.MachineStatus{ProviderID: node.Spec.ProviderID}})
		cloudProvider.CreatedNodeClaims[node.Spec.ProviderID] = nodeclaimutil.New(machine)
	})

	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)
		fakeClock.SetTime(time.Now())
		cloudProvider.Reset()

		// Reset the metrics collectors
		metrics.NodesTerminatedCounter.Reset()
		termination.TerminationSummary.Reset()
	})

	Context("Reconciliation", func() {
		It("should delete nodes", func() {
			ExpectApplied(ctx, env.Client, node)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should delete machines associated with nodes", func() {
			ExpectApplied(ctx, env.Client, node, machine)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectExists(ctx, env.Client, machine)
			ExpectFinalizersRemoved(ctx, env.Client, machine)
			ExpectNotFound(ctx, env.Client, node, machine)
		})
		It("should delete nodeclaims associated with nodes", func() {
			ExpectApplied(ctx, env.Client, node, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectExists(ctx, env.Client, nodeClaim)
			ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
			ExpectNotFound(ctx, env.Client, node, nodeClaim)
		})
		It("should not race if deleting nodes in parallel", func() {
			var nodes []*v1.Node
			for i := 0; i < 10; i++ {
				node = test.Node(test.NodeOptions{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{v1alpha5.TerminationFinalizer},
					},
				})
				ExpectApplied(ctx, env.Client, node)
				Expect(env.Client.Delete(ctx, node)).To(Succeed())
				node = ExpectNodeExists(ctx, env.Client, node.Name)
				nodes = append(nodes, node)
			}

			var wg sync.WaitGroup
			// this is enough to trip the race detector
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(node *v1.Node) {
					defer GinkgoRecover()
					defer wg.Done()
					ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
				}(nodes[i])
			}
			wg.Wait()
			ExpectNotFound(ctx, env.Client, lo.Map(nodes, func(n *v1.Node, _ int) client.Object { return n })...)
		})
		It("should exclude nodes from load balancers when terminating", func() {
			// This is a kludge to prevent the node from being deleted before we can
			// inspect its labels
			podNoEvict := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					Annotations:     map[string]string{v1alpha5.DoNotEvictPodAnnotationKey: "true"},
					OwnerReferences: defaultOwnerRefs,
				},
			})

			ExpectApplied(ctx, env.Client, node, podNoEvict)

			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.Labels[v1.LabelNodeExcludeBalancers]).Should(Equal("karpenter"))
		})
		It("should not evict pods that tolerate unschedulable taint", func() {
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podSkip := test.Pod(test.PodOptions{
				NodeName:    node.Name,
				Tolerations: []v1.Toleration{{Key: v1.TaintNodeUnschedulable, Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoSchedule}},
				ObjectMeta:  metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs},
			})
			ExpectApplied(ctx, env.Client, node, podEvict, podSkip)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Expect node to exist and be draining
			ExpectNodeDraining(env.Client, node.Name)

			// Expect podEvict to be evicting, and delete it
			ExpectEvicted(env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should delete nodes that have pods without an ownerRef", func() {
			pod := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: nil,
				},
			})

			ExpectApplied(ctx, env.Client, node, pod)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Expect pod with no owner ref to be enqueued for eviction
			ExpectEvicted(env.Client, pod)

			// Expect node to exist and be draining
			ExpectNodeDraining(env.Client, node.Name)

			// Delete no owner refs pod to simulate successful eviction
			ExpectDeleted(ctx, env.Client, pod)

			// Reconcile node to evict pod
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Reconcile to delete node
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should delete nodes with terminal pods", func() {
			podEvictPhaseSucceeded := test.Pod(test.PodOptions{
				NodeName: node.Name,
				Phase:    v1.PodSucceeded,
			})
			podEvictPhaseFailed := test.Pod(test.PodOptions{
				NodeName: node.Name,
				Phase:    v1.PodFailed,
			})

			ExpectApplied(ctx, env.Client, node, podEvictPhaseSucceeded, podEvictPhaseFailed)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Trigger Termination Controller, which should ignore these pods and delete the node
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should fail to evict pods that violate a PDB", func() {
			minAvailable := intstr.FromInt(1)
			labelSelector := map[string]string{test.RandomName(): test.RandomName()}
			pdb := test.PodDisruptionBudget(test.PDBOptions{
				Labels: labelSelector,
				// Don't let any pod evict
				MinAvailable: &minAvailable,
			})
			podNoEvict := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					Labels:          labelSelector,
					OwnerReferences: defaultOwnerRefs,
				},
				Phase: v1.PodRunning,
			})

			ExpectApplied(ctx, env.Client, node, podNoEvict, pdb)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Expect node to exist and be draining
			ExpectNodeDraining(env.Client, node.Name)

			// Expect podNoEvict to fail eviction due to PDB, and be retried
			Eventually(func() int {
				return evictionQueue.NumRequeues(client.ObjectKeyFromObject(podNoEvict))
			}).Should(BeNumerically(">=", 1))

			// Delete pod to simulate successful eviction
			ExpectDeleted(ctx, env.Client, podNoEvict)
			ExpectNotFound(ctx, env.Client, podNoEvict)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should evict non-critical pods first", func() {
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podNodeCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-node-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podClusterCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-cluster-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})

			ExpectApplied(ctx, env.Client, node, podEvict, podNodeCritical, podClusterCritical)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Expect node to exist and be draining
			ExpectNodeDraining(env.Client, node.Name)

			// Expect podEvict to be evicting, and delete it
			ExpectEvicted(env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			// Expect the critical pods to be evicted and deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectEvicted(env.Client, podNodeCritical)
			ExpectDeleted(ctx, env.Client, podNodeCritical)
			ExpectEvicted(env.Client, podClusterCritical)
			ExpectDeleted(ctx, env.Client, podClusterCritical)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should not evict static pods", func() {
			ExpectApplied(ctx, env.Client, node)
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			ExpectApplied(ctx, env.Client, node, podEvict)

			podNoEvict := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "v1",
						Kind:       "Node",
						Name:       node.Name,
						UID:        node.UID,
					}},
				},
			})
			ExpectApplied(ctx, env.Client, podNoEvict)

			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Expect mirror pod to not be queued for eviction
			ExpectNotEnqueuedForEviction(evictionQueue, podNoEvict)

			// Expect podEvict to be enqueued for eviction then be successful
			ExpectEvicted(env.Client, podEvict)

			// Expect node to exist and be draining
			ExpectNodeDraining(env.Client, node.Name)

			// Reconcile node to evict pod
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Delete pod to simulate successful eviction
			ExpectDeleted(ctx, env.Client, podEvict)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)

		})
		It("should not delete nodes until all pods are deleted", func() {
			pods := []*v1.Pod{
				test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}}),
				test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}}),
			}
			ExpectApplied(ctx, env.Client, node, pods[0], pods[1])

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Expect the pods to be evicted
			ExpectEvicted(env.Client, pods[0], pods[1])

			// Expect node to exist and be draining, but not deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNodeDraining(env.Client, node.Name)

			ExpectDeleted(ctx, env.Client, pods[1])

			// Expect node to exist and be draining, but not deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNodeDraining(env.Client, node.Name)

			ExpectDeleted(ctx, env.Client, pods[0])

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should delete nodes with no underlying instance even if not fully drained", func() {
			pods := []*v1.Pod{
				test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}}),
				test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}}),
			}
			ExpectApplied(ctx, env.Client, node, pods[0], pods[1])

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			// Expect the pods to be evicted
			ExpectEvicted(env.Client, pods[0], pods[1])

			// Expect node to exist and be draining, but not deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNodeDraining(env.Client, node.Name)

			// After this, the node still has one pod that is evicting.
			ExpectDeleted(ctx, env.Client, pods[1])

			// Remove the node from created machines so that the cloud provider returns DNE
			cloudProvider.CreatedNodeClaims = map[string]*v1beta1.NodeClaim{}

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should wait for pods to terminate", func() {
			pod := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			fakeClock.SetTime(time.Now()) // make our fake clock match the pod creation time
			ExpectApplied(ctx, env.Client, node, pod)

			// Before grace period, node should not delete
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectEvicted(env.Client, pod)

			// After grace period, node should delete. The deletion timestamps are from etcd which we can't control, so
			// to eliminate test-flakiness we reset the time to current time + 90 seconds instead of just advancing
			// the clock by 90 seconds.
			fakeClock.SetTime(time.Now().Add(90 * time.Second))
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		})
	})
	Context("Metrics", func() {
		It("should fire the terminationSummary metric when deleting nodes", func() {
			ExpectApplied(ctx, env.Client, node)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			m, ok := FindMetricWithLabelValues("karpenter_nodes_termination_time_seconds", map[string]string{"provisioner": ""})
			Expect(ok).To(BeTrue())
			Expect(m.GetSummary().GetSampleCount()).To(BeNumerically("==", 1))
		})
		It("should fire the nodesTerminated counter metric when deleting nodes", func() {
			ExpectApplied(ctx, env.Client, node)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectReconcileSucceeded(ctx, terminationController, client.ObjectKeyFromObject(node))

			m, ok := FindMetricWithLabelValues("karpenter_nodes_terminated", map[string]string{"provisioner": ""})
			Expect(ok).To(BeTrue())
			Expect(lo.FromPtr(m.GetCounter().Value)).To(BeNumerically("==", 1))
		})
	})
})

func ExpectNotEnqueuedForEviction(e *terminator.EvictionQueue, pods ...*v1.Pod) {
	GinkgoHelper()
	for _, pod := range pods {
		Expect(e.Contains(client.ObjectKeyFromObject(pod))).To(BeFalse())
	}
}

func ExpectEvicted(c client.Client, pods ...*v1.Pod) {
	GinkgoHelper()
	for _, pod := range pods {
		Eventually(func() bool {
			return ExpectPodExists(ctx, c, pod.Name, pod.Namespace).GetDeletionTimestamp().IsZero()
		}, ReconcilerPropagationTime, RequestInterval).Should(BeFalse(), func() string {
			return fmt.Sprintf("expected %s/%s to be evicting, but it isn't", pod.Namespace, pod.Name)
		})
	}
}

func ExpectNodeDraining(c client.Client, nodeName string) *v1.Node {
	GinkgoHelper()
	node := ExpectNodeExists(ctx, c, nodeName)
	Expect(node.Spec.Unschedulable).To(BeTrue())
	Expect(lo.Contains(node.Finalizers, v1alpha5.TerminationFinalizer)).To(BeTrue())
	Expect(node.DeletionTimestamp.IsZero()).To(BeFalse())
	return node
}
