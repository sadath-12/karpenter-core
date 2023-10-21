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

package scheduling

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/cloudprovider"
	"github.com/aws/karpenter-core/pkg/scheduling"
	"github.com/aws/karpenter-core/pkg/utils/resources"
)

// NodeClaim is a set of constraints, compatible pods, and possible instance types that could fulfill these constraints. This
// will be turned into one or more actual node instances within the cluster after bin packing.
type NodeClaim struct {
	NodeClaimTemplate

	Pods            []*v1.Pod
	topology        *Topology
	hostPortUsage   *scheduling.HostPortUsage
	daemonResources v1.ResourceList
}

var nodeID int64

func NewNodeClaim(nodeClaimTemplate *NodeClaimTemplate, topology *Topology, daemonResources v1.ResourceList, instanceTypes []*cloudprovider.InstanceType) *NodeClaim {
	// Copy the template, and add hostname
	hostname := fmt.Sprintf("hostname-placeholder-%04d", atomic.AddInt64(&nodeID, 1))
	topology.Register(v1.LabelHostname, hostname)
	template := *nodeClaimTemplate
	template.Requirements = scheduling.NewRequirements()
	template.Requirements.Add(nodeClaimTemplate.Requirements.Values()...)
	template.Requirements.Add(scheduling.NewRequirement(v1.LabelHostname, v1.NodeSelectorOpIn, hostname))
	template.InstanceTypeOptions = instanceTypes
	template.Spec.Resources.Requests = daemonResources

	return &NodeClaim{
		NodeClaimTemplate: template,
		hostPortUsage:     scheduling.NewHostPortUsage(),
		topology:          topology,
		daemonResources:   daemonResources,
	}
}

func (n *NodeClaim) Add(pod *v1.Pod) error {
	// Check Taints
	if err := scheduling.Taints(n.Spec.Taints).Tolerates(pod); err != nil {
		return err
	}

	// exposed host ports on the node
	hostPorts := scheduling.GetHostPorts(pod)
	if err := n.hostPortUsage.Conflicts(pod, hostPorts); err != nil {
		return fmt.Errorf("checking host port usage, %w", err)
	}

	nodeClaimRequirements := scheduling.NewRequirements(n.Requirements.Values()...)
	fmt.Println("nodeclaim reqs", nodeClaimRequirements)
	// nodeclaim reqs karpenter.sh/capacity-type In [on-demand spot], karpenter.sh/provisioner-name In [condorplump-1-wixhuhyj8t], testing/cluster In [unspecified]
	podRequirements := scheduling.NewPodRequirements(pod)
	fmt.Println("pod reqs", podRequirements)

	// Check NodeClaim Affinity Requirements
	if err := nodeClaimRequirements.Compatible(podRequirements, lo.Ternary(n.OwnerKey.IsProvisioner, scheduling.AllowUndefinedWellKnownLabelsV1Alpha5, scheduling.AllowUndefinedWellKnownLabelsV1Beta1)); err != nil {
		return fmt.Errorf("incompatible requirements, %w", err)
	}
	nodeClaimRequirements.Add(podRequirements.Values()...)

	strictPodRequirements := podRequirements
	if scheduling.HasPreferredNodeAffinity(pod) {
		// strictPodRequirements is important as it ensures we don't inadvertently restrict the possible pod domains by a
		// preferred node affinity.  Only required node affinities can actually reduce pod domains.
		strictPodRequirements = scheduling.NewStrictPodRequirements(pod)
	}
	// Check Topology Requirements
	topologyRequirements, err := n.topology.AddRequirements(strictPodRequirements, nodeClaimRequirements, pod, lo.Ternary(n.OwnerKey.IsProvisioner, scheduling.AllowUndefinedWellKnownLabelsV1Alpha5, scheduling.AllowUndefinedWellKnownLabelsV1Beta1))
	fmt.Println("topology reqs", topologyRequirements)
	// topology reqs karpenter.sh/capacity-type In [on-demand spot], karpenter.sh/provisioner-name In [spikeshore-1-3uouamznar], testing/cluster In [unspecified], topology.kubernetes.io/zone In [test-zone-2]
	if err != nil {
		return err
	}
	if err = nodeClaimRequirements.Compatible(topologyRequirements, lo.Ternary(n.OwnerKey.IsProvisioner, scheduling.AllowUndefinedWellKnownLabelsV1Alpha5, scheduling.AllowUndefinedWellKnownLabelsV1Beta1)); err != nil {
		return err
	}
	nodeClaimRequirements.Add(topologyRequirements.Values()...)

	// Check instance type combinations
	requests := resources.Merge(n.Spec.Resources.Requests, resources.RequestsForPods(pod))
	filtered := filterInstanceTypesByRequirements(n.InstanceTypeOptions, nodeClaimRequirements, requests)
	if len(filtered.remaining) == 0 {
		// log the total resources being requested (daemonset + the pod)
		cumulativeResources := resources.Merge(n.daemonResources, resources.RequestsForPods(pod))
		return fmt.Errorf("no instance type satisfied resources %s and requirements %s (%s)", resources.String(cumulativeResources), nodeClaimRequirements, filtered.FailureReason())
	}

	// Update node
	n.Pods = append(n.Pods, pod)
	n.InstanceTypeOptions = filtered.remaining
	n.Spec.Resources.Requests = requests
	n.Requirements = nodeClaimRequirements
	n.topology.Record(pod, nodeClaimRequirements, lo.Ternary(n.OwnerKey.IsProvisioner, scheduling.AllowUndefinedWellKnownLabelsV1Alpha5, scheduling.AllowUndefinedWellKnownLabelsV1Beta1))
	n.hostPortUsage.Add(pod, hostPorts)
	return nil
}

// FinalizeScheduling is called once all scheduling has completed and allows the node to perform any cleanup
// necessary before its requirements are used for instance launching
func (n *NodeClaim) FinalizeScheduling() {
	// We need nodes to have hostnames for topology purposes, but we don't want to pass that node name on to consumers
	// of the node as it will be displayed in error messages
	delete(n.Requirements, v1.LabelHostname)
}

func InstanceTypeList(instanceTypeOptions []*cloudprovider.InstanceType) string {
	var itSb strings.Builder
	for i, it := range instanceTypeOptions {
		// print the first 5 instance types only (indices 0-4)
		if i > 4 {
			fmt.Fprintf(&itSb, " and %d other(s)", len(instanceTypeOptions)-i)
			break
		} else if i > 0 {
			fmt.Fprint(&itSb, ", ")
		}
		fmt.Fprint(&itSb, it.Name)
	}
	return itSb.String()
}

type filterResults struct {
	remaining []*cloudprovider.InstanceType
	// Each of these three flags indicates if that particular criteria was met by at least one instance type
	requirementsMet bool
	fits            bool
	hasOffering     bool

	// requirementsAndFits indicates if a single instance type met the scheduling requirements and had enough resources
	requirementsAndFits bool
	// requirementsAndOffering indicates if a single instance type met the scheduling requirements and was a required offering
	requirementsAndOffering bool
	// fitsAndOffering indicates if a single instance type had enough resources and was a required offering
	fitsAndOffering bool

	requests v1.ResourceList
}

// FailureReason returns a presentable string explaining why all instance types were filtered out
//
//nolint:gocyclo
func (r filterResults) FailureReason() string {
	if len(r.remaining) > 0 {
		return ""
	}

	// no instance type met any of the three criteria, meaning each criteria was enough to completely prevent
	// this pod from scheduling
	if !r.requirementsMet && !r.fits && !r.hasOffering {
		return "no instance type met the scheduling requirements or had enough resources or had a required offering"
	}

	// check the other pairwise criteria
	if !r.requirementsMet && !r.fits {
		return "no instance type met the scheduling requirements or had enough resources"
	}

	if !r.requirementsMet && !r.hasOffering {
		return "no instance type met the scheduling requirements or had a required offering"
	}

	if !r.fits && !r.hasOffering {
		return "no instance type had enough resources or had a required offering"
	}

	// and then each individual criteria. These are sort of the same as above in that each one indicates that no
	// instance type matched that criteria at all, so it was enough to exclude all instance types.  I think it's
	// helpful to have these separate, since we can report the multiple excluding criteria above.
	if !r.requirementsMet {
		return "no instance type met all requirements"
	}

	if !r.fits {
		msg := "no instance type has enough resources"
		// special case for a user typo I saw reported once
		if r.requests.Cpu().Cmp(resource.MustParse("1M")) >= 0 {
			msg += " (CPU request >= 1 Million, m vs M typo?)"
		}
		return msg
	}

	if !r.hasOffering {
		return "no instance type has the required offering"
	}

	// see if any pair of criteria was enough to exclude all instances
	if r.requirementsAndFits {
		return "no instance type which met the scheduling requirements and had enough resources, had a required offering"
	}
	if r.fitsAndOffering {
		return "no instance type which had enough resources and the required offering met the scheduling requirements"
	}
	if r.requirementsAndOffering {
		return "no instance type which met the scheduling requirements and the required offering had the required resources"
	}

	// finally all instances were filtered out, but we had at least one instance that met each criteria, and met each
	// pairwise set of criteria, so the only thing that remains is no instance which met all three criteria simultaneously
	return "no instance type met the requirements/resources/offering tuple"
}

//nolint:gocyclo
func filterInstanceTypesByRequirements(instanceTypes []*cloudprovider.InstanceType, requirements scheduling.Requirements, requests v1.ResourceList) filterResults {
	results := filterResults{
		requests:        requests,
		requirementsMet: false,
		fits:            false,
		hasOffering:     false,

		requirementsAndFits:     false,
		requirementsAndOffering: false,
		fitsAndOffering:         false,
	}
	for _, it := range instanceTypes {
		// the tradeoff to not short circuiting on the filtering is that we can report much better error messages
		// about why scheduling failed
		fmt.Println("instance typ", it.Name)
		// 		instance typ default-instance-type
		// instance typ small-instance-type
		// instance typ gpu-vendor-instance-type
		// instance typ gpu-vendor-b-instance-type
		// instance typ arm-instance-type
		// instance typ single-pod-instance-type
		fmt.Println("the reqs to satisfy", requirements)
		// Eg:
		// the reqs to satisfy karpenter.sh/capacity-type In [on-demand spot], karpenter.sh/provisioner-name In [foxblue-1-xydlh2pgan], testing/cluster In [unspecified], topology.kubernetes.io/zone In [test-zone-3]
		// the reqs the it has  &{single-pod-instance-type integer In [4], karpenter.sh/capacity-type In [on-demand spot], kubernetes.io/arch In [amd64], kubernetes.io/os In [darwin linux windows], node.kubernetes.io/instance-type In [single-pod-instance-type], size In [small], special DoesNotExist, topology.kubernetes.io/zone In [test-zone-1 test-zone-2 test-zone-3] [{spot test-zone-1 0.8294967296 true} {spot test-zone-2 0.8294967296 true} {on-demand test-zone-1 0.8294967296 true} {on-demand test-zone-2 0.8294967296 true} {on-demand test-zone-3 0.8294967296 true}] map[cpu:{{4 0} {<nil>} 4 DecimalSI} memory:{{4294967296 0} {<nil>} 4Gi BinarySI} pods:{{1 0} {<nil>} 1 DecimalSI}] 0xc00049c750 {1 {0 0}} map[cpu:{{3900 -3} {<nil>}  DecimalSI} memory:{{4284481536 0} {<nil>}  BinarySI} pods:{{1 0} {<nil>} 1 DecimalSI}]}
		fmt.Println("the reqs the instance type has ", it)
		// checks if reqs match
		itCompat := compatible(it, requirements)

		// capacity for instance  arm-instance-type  is map[cpu:{{16 0} {<nil>} 16 DecimalSI} memory:{{137438953472 0} {<nil>}  BinarySI} pods:{{5 0} {<nil>} 5 DecimalSI}]
		// overhead  arm-instance-type  is map[cpu:{{100 -3} {<nil>}  DecimalSI} memory:{{10485760 0} {<nil>}  BinarySI}]
		fmt.Println("it allocatable is", it.Allocatable())
		// it allocatable is map[cpu:{{15900 -3} {<nil>}  DecimalSI} memory:{{137428467712 0} {<nil>}  BinarySI} pods:{{5 0} {<nil>} 5 DecimalSI}] -- for  arm-instance-type

		// checks if it resource quantities are always greater than requests quanitites

		itFits := fits(it, requests)

		// here we check if that zone and capacity match
		itHasOffering := hasOffering(it, requirements)

		fmt.Println("instance ", it.Name, " offerings available is", it.Offerings)

		// instance  arm-instance-type  offerings available is [{spot test-zone-1 15.3438953472 true} {spot test-zone-2 15.3438953472 true} {on-demand test-zone-1 15.3438953472 true} {on-demand test-zone-2 15.3438953472 true} {on-demand test-zone-3 15.3438953472 true}]
		// where [spot] [test-zone-1] [15.3438953472] [true] ---> [type] [zone of that it] [price of that it] [if that it is available]

		// track if any single instance type met a single criteria
		results.requirementsMet = results.requirementsMet || itCompat
		results.fits = results.fits || itFits
		results.hasOffering = results.hasOffering || itHasOffering

		// track if any single instance type met the three pairs of criteria
		results.requirementsAndFits = results.requirementsAndFits || (itCompat && itFits && !itHasOffering)
		results.requirementsAndOffering = results.requirementsAndOffering || (itCompat && itHasOffering && !itFits)
		results.fitsAndOffering = results.fitsAndOffering || (itFits && itHasOffering && !itCompat)

		// and if it met all criteria, we keep the instance type and continue filtering.  We now won't be reporting
		// any errors.
		if itCompat && itFits && itHasOffering {
			results.remaining = append(results.remaining, it)
		}
	}
	return results
}

func compatible(instanceType *cloudprovider.InstanceType, requirements scheduling.Requirements) bool {
	return instanceType.Requirements.Intersects(requirements) == nil
}

func fits(instanceType *cloudprovider.InstanceType, requests v1.ResourceList) bool {
	return resources.Fits(requests, instanceType.Allocatable())
}

func hasOffering(instanceType *cloudprovider.InstanceType, requirements scheduling.Requirements) bool {
	for _, offering := range instanceType.Offerings.Available() {
		if (!requirements.Has(v1.LabelTopologyZone) || requirements.Get(v1.LabelTopologyZone).Has(offering.Zone)) &&
			(!requirements.Has(v1alpha5.LabelCapacityType) || requirements.Get(v1alpha5.LabelCapacityType).Has(offering.CapacityType)) {
			return true
		}
	}

	return false
}
