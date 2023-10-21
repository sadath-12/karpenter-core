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

package state 

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/apis/v1beta1"
	"github.com/aws/karpenter-core/pkg/operator/options"
	"github.com/aws/karpenter-core/pkg/scheduling"
	nodeutils "github.com/aws/karpenter-core/pkg/utils/node"
	nodepoolutil "github.com/aws/karpenter-core/pkg/utils/nodepool"
	podutils "github.com/aws/karpenter-core/pkg/utils/pod"
	"github.com/aws/karpenter-core/pkg/utils/resources"
)

//go:generate controller-gen object:headerFile="../../../hack/boilerplate.go.txt" paths="."

// StateNodes is a typed version of a list of *Node
// nolint: revive
type StateNodes []*StateNode

// Active filters StateNodes that are not in a MarkedForDeletion state
func (n StateNodes) Active() StateNodes {
	return lo.Filter(n, func(node *StateNode, _ int) bool {
		return !node.MarkedForDeletion()
	})
}

// Deleting filters StateNodes that are in a MarkedForDeletion state
func (n StateNodes) Deleting() StateNodes {
	return lo.Filter(n, func(node *StateNode, _ int) bool {
		return node.MarkedForDeletion()
	})
}

// Pods gets the pods assigned to all StateNodes based on the kubernetes api-server bindings
func (n StateNodes) Pods(ctx context.Context, c client.Client) ([]*v1.Pod, error) {
	var pods []*v1.Pod
	for _, node := range n {
		p, err := node.Pods(ctx, c)
		if err != nil {
			return nil, err
		}
		pods = append(pods, p...)
	}
	return pods, nil
}

// StateNode is a cached version of a node in the cluster that maintains state which is expensive to compute every time it's
// needed.  This currently contains node utilization across all the allocatable resources, but will soon be used to
// compute topology information.
// +k8s:deepcopy-gen=true
// nolint: revive
type StateNode struct {
	Node      *v1.Node
	NodeClaim *v1beta1.NodeClaim

	inflightInitialized bool            // TODO @joinnis: This can be removed when machine is added
	inflightAllocatable v1.ResourceList // TODO @joinnis: This can be removed when machine is added
	inflightCapacity    v1.ResourceList // TODO @joinnis: This can be removed when machine is added

	startupTaintsInitialized bool       // TODO: @joinnis: This can be removed when machine is added
	startupTaints            []v1.Taint // TODO: @joinnis: This can be removed when machine is added

	// daemonSetRequests is the total amount of resources that have been requested by daemon sets. This allows users
	// of the Node to identify the remaining resources that we expect future daemonsets to consume.
	daemonSetRequests map[types.NamespacedName]v1.ResourceList
	daemonSetLimits   map[types.NamespacedName]v1.ResourceList

	podRequests map[types.NamespacedName]v1.ResourceList
	podLimits   map[types.NamespacedName]v1.ResourceList

	hostPortUsage *scheduling.HostPortUsage
	volumeUsage   *scheduling.VolumeUsage

	// TODO remove this when v1alpha5 APIs are deprecated. With v1beta1 APIs Karpenter relies on the existence
	// of the karpenter.sh/disruption taint to know when a node is marked for deletion.
	markedForDeletion bool
	nominatedUntil    metav1.Time
}

func NewNode() *StateNode {
	return &StateNode{
		inflightAllocatable: v1.ResourceList{},
		inflightCapacity:    v1.ResourceList{},
		startupTaints:       []v1.Taint{},
		daemonSetRequests:   map[types.NamespacedName]v1.ResourceList{},
		daemonSetLimits:     map[types.NamespacedName]v1.ResourceList{},
		podRequests:         map[types.NamespacedName]v1.ResourceList{},
		podLimits:           map[types.NamespacedName]v1.ResourceList{},
		hostPortUsage:       scheduling.NewHostPortUsage(),
		volumeUsage:         scheduling.NewVolumeUsage(),
	}
}

func (in *StateNode) OwnerKey() nodepoolutil.Key {
	if in.Labels()[v1beta1.NodePoolLabelKey] != "" {
		return nodepoolutil.Key{Name: in.Labels()[v1beta1.NodePoolLabelKey], IsProvisioner: false}
	}
	if in.Labels()[v1alpha5.ProvisionerNameLabelKey] != "" {
		return nodepoolutil.Key{Name: in.Labels()[v1alpha5.ProvisionerNameLabelKey], IsProvisioner: true}
	}
	return nodepoolutil.Key{}
}

func (in *StateNode) Name() string {
	if in.Node == nil {
		return in.NodeClaim.Name
	}
	if in.NodeClaim == nil {
		return in.Node.Name
	}
	// TODO @joinnis: The !in.Initialized() check can be removed when we can assume that all nodes have the v1alpha5.NodeRegisteredLabel on them
	// We can assume that all nodes will have this label and no back-propagation will be required once we hit v1
	if !in.Registered() && !in.Initialized() {
		return in.NodeClaim.Name
	}
	return in.Node.Name
}

// ProviderID is the key that is used to map this StateNode
// If the Node and NodeClaim have a providerID, this should map to a real providerID
// If the Node does not have a providerID, this will map to the node name
func (in *StateNode) ProviderID() string {
	if in.Node == nil {
		return in.NodeClaim.Status.ProviderID
	}
	return in.Node.Spec.ProviderID
}

// Pods gets the pods assigned to the Node based on the kubernetes api-server bindings
func (in *StateNode) Pods(ctx context.Context, c client.Client) ([]*v1.Pod, error) {
	if in.Node == nil {
		return nil, nil
	}
	return nodeutils.GetNodePods(ctx, c, in.Node)
}

func (in *StateNode) HostName() string {
	if in.Labels()[v1.LabelHostname] == "" {
		return in.Name()
	}
	return in.Labels()[v1.LabelHostname]
}

func (in *StateNode) Annotations() map[string]string {
	// If the machine exists and the state node isn't initialized
	// use the machine representation of the annotations
	if in.Node == nil {
		return in.NodeClaim.Annotations
	}
	if in.NodeClaim == nil {
		return in.Node.Annotations
	}
	// TODO @joinnis: The !in.Initialized() check can be removed when we can assume that all nodes have the v1alpha5.NodeRegisteredLabel on them
	// We can assume that all nodes will have this label and no back-propagation will be required once we hit v1
	if !in.Registered() && !in.Initialized() {
		return in.NodeClaim.Annotations
	}
	return in.Node.Annotations
}

// GetLabels exists to conform to the GetLabels() function on the metav1.Object interface
func (in *StateNode) GetLabels() map[string]string {
	return in.Labels()
}

func (in *StateNode) Labels() map[string]string {
	// If the machine exists and the state node isn't registered
	// use the machine representation of the labels
	if in.Node == nil {
		return in.NodeClaim.Labels
	}
	if in.NodeClaim == nil {
		return in.Node.Labels
	}
	// TODO @joinnis: The !in.Initialized() check can be removed when we can assume that all nodes have the v1alpha5.NodeRegisteredLabel on them
	// We can assume that all nodes will have this label and no back-propagation will be required once we hit v1
	if !in.Registered() && !in.Initialized() {
		return in.NodeClaim.Labels
	}
	return in.Node.Labels
}

func (in *StateNode) Taints() []v1.Taint {
	// Only consider startup taints until the node is initialized. Without this, if the startup taint is generic and
	// re-appears on the node for a different reason (e.g. the node is cordoned) we will assume that pods can
	// schedule against the node in the future incorrectly.
	ephemeralTaints := scheduling.KnownEphemeralTaints
	if !in.Initialized() && in.Managed() {
		if in.NodeClaim != nil {
			ephemeralTaints = append(ephemeralTaints, in.NodeClaim.Spec.StartupTaints...)
		} else {
			ephemeralTaints = append(ephemeralTaints, in.startupTaints...)
		}
	}

	var taints []v1.Taint
	// TODO @joinnis: The !in.Initialized() check can be removed when we can assume that all nodes have the v1alpha5.NodeRegisteredLabel on them
	// We can assume that all nodes will have this label and no back-propagation will be required once we hit v1
	if (!in.Registered() && !in.Initialized() && in.NodeClaim != nil) || in.Node == nil {
		taints = in.NodeClaim.Spec.Taints
	} else {
		taints = in.Node.Spec.Taints
	}
	return lo.Reject(taints, func(taint v1.Taint, _ int) bool {
		_, found := lo.Find(ephemeralTaints, func(t v1.Taint) bool {
			return t.MatchTaint(&taint)
		})
		return found
	})
}

func (in *StateNode) Registered() bool {
	// Node is managed by Karpenter, so we can check for the Registered label
	if in.Managed() {
		return in.Node != nil && in.Node.Labels[v1beta1.NodeRegisteredLabelKey] == "true"
	}
	// Nodes not managed by Karpenter are always considered Registered
	return true
}

func (in *StateNode) Initialized() bool {
	// Node is managed by Karpenter, so we can check for the Initialized label
	if in.Managed() {
		return in.Node != nil && in.Node.Labels[v1beta1.NodeInitializedLabelKey] == "true"
	}
	// Nodes not managed by Karpenter are always considered Initialized
	return true
}

func (in *StateNode) Capacity() v1.ResourceList {
	if !in.Initialized() && in.NodeClaim != nil {
		// Override any zero quantity values in the node status
		if in.Node != nil {
			ret := lo.Assign(in.Node.Status.Capacity)
			for resourceName, quantity := range in.NodeClaim.Status.Capacity {
				if resources.IsZero(ret[resourceName]) {
					ret[resourceName] = quantity
				}
			}
			return ret
		}
		return in.NodeClaim.Status.Capacity
	}
	// TODO @joinnis: Remove this when machine migration is complete
	if !in.Initialized() && in.Managed() {
		// Override any zero quantity values in the node status
		ret := lo.Assign(in.Node.Status.Capacity)
		for resourceName, quantity := range in.inflightCapacity {
			if resources.IsZero(ret[resourceName]) {
				ret[resourceName] = quantity
			}
		}
		return ret
	}
	return in.Node.Status.Capacity
}

func (in *StateNode) Allocatable() v1.ResourceList {
	if !in.Initialized() && in.NodeClaim != nil {
		// Override any zero quantity values in the node status
		if in.Node != nil {
			ret := lo.Assign(in.Node.Status.Allocatable)
			for resourceName, quantity := range in.NodeClaim.Status.Allocatable {
				if resources.IsZero(ret[resourceName]) {
					ret[resourceName] = quantity
				}
			}
			return ret
		}
		return in.NodeClaim.Status.Allocatable
	}
	// TODO @joinnis: Remove this when machine migration is complete
	if !in.Initialized() && in.Managed() {
		// Override any zero quantity values in the node status
		ret := lo.Assign(in.Node.Status.Allocatable)
		for resourceName, quantity := range in.inflightAllocatable {
			if resources.IsZero(ret[resourceName]) {
				ret[resourceName] = quantity
			}
		}
		return ret
	}
	return in.Node.Status.Allocatable
}

// Available is allocatable minus anything allocated to pods.
func (in *StateNode) Available() v1.ResourceList {
	return resources.Subtract(in.Allocatable(), in.PodRequests())
}

func (in *StateNode) DaemonSetRequests() v1.ResourceList {
	return resources.Merge(lo.Values(in.daemonSetRequests)...)
}

func (in *StateNode) DaemonSetLimits() v1.ResourceList {
	return resources.Merge(lo.Values(in.daemonSetLimits)...)
}

func (in *StateNode) HostPortUsage() *scheduling.HostPortUsage {
	return in.hostPortUsage
}

func (in *StateNode) VolumeUsage() *scheduling.VolumeUsage {
	return in.volumeUsage
}

func (in *StateNode) PodRequests() v1.ResourceList {
	var totalRequests v1.ResourceList
	for _, requests := range in.podRequests {
		totalRequests = resources.MergeInto(totalRequests, requests)
	}
	return totalRequests
}

func (in *StateNode) PodLimits() v1.ResourceList {
	return resources.Merge(lo.Values(in.podLimits)...)
}

func (in *StateNode) MarkedForDeletion() bool {
	// The Node is marked for deletion if:
	//  1. The Node has MarkedForDeletion set
	//  2. The Node has a NodeClaim counterpart and is actively deleting
	//  3. The Node has no NodeClaim counterpart and is actively deleting
	// TODO remove check for machine after v1alpha5 APIs are dropped.
	return in.markedForDeletion ||
		(in.NodeClaim != nil && !in.NodeClaim.DeletionTimestamp.IsZero()) ||
		(in.Node != nil && in.NodeClaim == nil && !in.Node.DeletionTimestamp.IsZero())
}

func (in *StateNode) Nominate(ctx context.Context) {
	in.nominatedUntil = metav1.Time{Time: time.Now().Add(nominationWindow(ctx))}
}

func (in *StateNode) Nominated() bool {
	return in.nominatedUntil.After(time.Now())
}

func (in *StateNode) Managed() bool {
	return in.NodeClaim != nil ||
		(in.Node != nil && in.Node.Labels[v1alpha5.ProvisionerNameLabelKey] != "") ||
		(in.Node != nil && in.Node.Labels[v1beta1.NodePoolLabelKey] != "")
}

func (in *StateNode) updateForPod(ctx context.Context, kubeClient client.Client, pod *v1.Pod) error {
	podKey := client.ObjectKeyFromObject(pod)
	hostPorts := scheduling.GetHostPorts(pod)
	volumes, err := scheduling.GetVolumes(ctx, kubeClient, pod)
	if err != nil {
		return fmt.Errorf("tracking volume usage, %w", err)
	}
	in.podRequests[podKey] = resources.RequestsForPods(pod)
	in.podLimits[podKey] = resources.LimitsForPods(pod)
	// if it's a daemonset, we track what it has requested separately
	if podutils.IsOwnedByDaemonSet(pod) {
		in.daemonSetRequests[podKey] = resources.RequestsForPods(pod)
		in.daemonSetLimits[podKey] = resources.LimitsForPods(pod)
	}
	in.hostPortUsage.Add(pod, hostPorts)
	in.volumeUsage.Add(pod, volumes)
	return nil
}

func (in *StateNode) cleanupForPod(podKey types.NamespacedName) {
	in.hostPortUsage.DeletePod(podKey)
	in.volumeUsage.DeletePod(podKey)
	delete(in.podRequests, podKey)
	delete(in.podLimits, podKey)
	delete(in.daemonSetRequests, podKey)
	delete(in.daemonSetLimits, podKey)
}

func nominationWindow(ctx context.Context) time.Duration {
	nominationPeriod := 2 * options.FromContext(ctx).BatchMaxDuration
	if nominationPeriod < 10*time.Second {
		nominationPeriod = 10 * time.Second
	}
	return nominationPeriod
}
