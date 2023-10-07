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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/apis/v1beta1"
	"github.com/aws/karpenter-core/pkg/cloudprovider"
	"github.com/aws/karpenter-core/pkg/cloudprovider/fake"
	. "github.com/aws/karpenter-core/pkg/test/expectations"

	"github.com/aws/karpenter-core/pkg/test"
)

var _ = Describe("NodeClaim", func() {
	var nodePool *v1beta1.NodePool
	var nodeClaim *v1beta1.NodeClaim

	BeforeEach(func() {
		nodePool = test.NodePool()
		nodeClaim = test.NodeClaim(v1beta1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha5.ProvisionerNameLabelKey: nodePool.Name,
				},
				Finalizers: []string{
					v1alpha5.TerminationFinalizer,
				},
			},
			Spec: v1beta1.NodeClaimSpec{
				Resources: v1beta1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:          resource.MustParse("2"),
						v1.ResourceMemory:       resource.MustParse("50Mi"),
						v1.ResourcePods:         resource.MustParse("5"),
						fake.ResourceGPUVendorA: resource.MustParse("1"),
					},
				},
			},
		})
	})
	It("should delete the node and the CloudProvider NodeClaim when NodeClaim deletion is triggered", func() {
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		_, err := cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(err).ToNot(HaveOccurred())

		node := test.NodeClaimLinkedNode(nodeClaim) 
		ExpectApplied(ctx, env.Client, node)

		// Expect the node and the nodeClaim to both be gone
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // triggers the node deletion
		ExpectFinalizersRemoved(ctx, env.Client, node)
		ExpectNotFound(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // now all nodes are gone so nodeClaim deletion continues
		ExpectNotFound(ctx, env.Client, nodeClaim, node)

		// Expect the nodeClaim to be gone from the cloudprovider
		_, err = cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(cloudprovider.IsNodeClaimNotFoundError(err)).To(BeTrue())
	})
	It("should delete multiple Nodes if multiple Nodes map to the NodeClaim", func() {
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		_, err := cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(err).ToNot(HaveOccurred())

		node1 := test.NodeClaimLinkedNode(nodeClaim)
		node2 := test.NodeClaimLinkedNode(nodeClaim)
		node3 := test.NodeClaimLinkedNode(nodeClaim)
		ExpectApplied(ctx, env.Client, node1, node2, node3)

		// Expect the node and the nodeClaim to both be gone
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // triggers the node deletion
		ExpectFinalizersRemoved(ctx, env.Client, node1, node2, node3)
		ExpectNotFound(ctx, env.Client, node1, node2, node3)

		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // now all nodes are gone so nodeClaim deletion continues
		ExpectNotFound(ctx, env.Client, nodeClaim, node1, node2, node3)

		// Expect the nodeClaim to be gone from the cloudprovider
		_, err = cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(cloudprovider.IsNodeClaimNotFoundError(err)).To(BeTrue())
	})
	It("should delete the Instance if the NodeClaim is linked but doesn't have its providerID resolved yet", func() {
		node := test.NodeClaimLinkedNode(nodeClaim)

		nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1alpha5.MachineLinkedAnnotationKey: nodeClaim.Status.ProviderID})
		nodeClaim.Status.ProviderID = ""
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)

		// Expect the nodeClaim to be gone
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // triggers the nodeClaim deletion
		ExpectNotFound(ctx, env.Client, nodeClaim)

		// Expect the nodeClaim to be gone from the cloudprovider
		_, err := cloudProvider.Get(ctx, nodeClaim.Annotations[v1alpha5.MachineLinkedAnnotationKey])
		Expect(cloudprovider.IsNodeClaimNotFoundError(err)).To(BeTrue())
	})
	It("should not delete the NodeClaim until all the Nodes are removed", func() {
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		_, err := cloudProvider.Get(ctx, nodeClaim.Status.ProviderID)
		Expect(err).ToNot(HaveOccurred())

		node := test.NodeClaimLinkedNode(nodeClaim)
		ExpectApplied(ctx, env.Client, node)

		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // triggers the node deletion
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // the node still hasn't been deleted, so the nodeClaim should remain

		ExpectExists(ctx, env.Client, nodeClaim)
		ExpectExists(ctx, env.Client, node)

		ExpectFinalizersRemoved(ctx, env.Client, node)
		ExpectNotFound(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim)) // now the nodeClaim should be gone

		ExpectNotFound(ctx, env.Client, nodeClaim)
	})
	It("should not call Delete() on the CloudProvider if the NodeClaim hasn't been launched yet", func() {
		nodeClaim.Status.ProviderID = ""
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)

		// Expect the nodeClaim to be gone
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim))

		Expect(cloudProvider.DeleteCalls).To(HaveLen(0))
		ExpectNotFound(ctx, env.Client, nodeClaim)
	})
	It("should not delete nodes without provider ids if the NodeClaim hasn't been launched yet", func() {
		// Generate 10 nodes, none of which have a provider id
		var nodes []*v1.Node
		for i := 0; i < 10; i++ {
			nodes = append(nodes, test.Node())
		}
		ExpectApplied(ctx, env.Client, lo.Map(nodes, func(n *v1.Node, _ int) client.Object { return n })...)

		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)

		// Expect the nodeClaim to be gone
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimTerminationController, client.ObjectKeyFromObject(nodeClaim))

		ExpectNotFound(ctx, env.Client, nodeClaim)
		for _, node := range nodes {
			ExpectExists(ctx, env.Client, node)
		}
	})
})
