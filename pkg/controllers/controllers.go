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

package controllers

import (
	"context"

	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/cloudprovider"
	"github.com/aws/karpenter-core/pkg/controllers/disruption"
	"github.com/aws/karpenter-core/pkg/controllers/leasegarbagecollection"
	metricsnode "github.com/aws/karpenter-core/pkg/controllers/metrics/node"
	metricspod "github.com/aws/karpenter-core/pkg/controllers/metrics/pod"
	metricsprovisioner "github.com/aws/karpenter-core/pkg/controllers/metrics/provisioner"
	"github.com/aws/karpenter-core/pkg/controllers/node/termination"
	"github.com/aws/karpenter-core/pkg/controllers/node/termination/terminator"
	nodeclaimconsistency "github.com/aws/karpenter-core/pkg/controllers/nodeclaim/consistency"
	nodeclaimdisruption "github.com/aws/karpenter-core/pkg/controllers/nodeclaim/disruption"
	nodeclaimgarbagecollection "github.com/aws/karpenter-core/pkg/controllers/nodeclaim/garbagecollection"
	nodeclaimlifecycle "github.com/aws/karpenter-core/pkg/controllers/nodeclaim/lifecycle"
	nodeclaimtermination "github.com/aws/karpenter-core/pkg/controllers/nodeclaim/termination"
	nodepoolcounter "github.com/aws/karpenter-core/pkg/controllers/nodepool/counter"
	nodepoolhash "github.com/aws/karpenter-core/pkg/controllers/nodepool/hash"
	"github.com/aws/karpenter-core/pkg/controllers/provisioning"
	"github.com/aws/karpenter-core/pkg/controllers/state"
	"github.com/aws/karpenter-core/pkg/controllers/state/informer"
	"github.com/aws/karpenter-core/pkg/operator/controller"
)

func NewControllers(
	clock clock.Clock,
	kubeClient client.Client,
	kubernetesInterface kubernetes.Interface,
	cluster *state.Cluster,
	cloudProvider cloudprovider.CloudProvider,
	ctx context.Context,
) []controller.Controller {

	p := provisioning.NewProvisioner(ctx, kubeClient, kubernetesInterface.CoreV1(), cloudProvider, cluster)
	evictionQueue := terminator.NewQueue(ctx, kubernetesInterface.CoreV1())

	return []controller.Controller{
		p, evictionQueue,
		disruption.NewController(ctx, clock, kubeClient, p, cloudProvider, cluster),
		provisioning.NewController(ctx, kubeClient, p),
		nodepoolhash.NewProvisionerController(kubeClient),
		informer.NewDaemonSetController(kubeClient, cluster),
		informer.NewNodeController(kubeClient, cluster),
		informer.NewPodController(kubeClient, cluster),
		informer.NewProvisionerController(kubeClient, cluster),
		informer.NewMachineController(kubeClient, cluster),
		termination.NewController(ctx, kubeClient, cloudProvider, terminator.NewTerminator(clock, kubeClient, evictionQueue)),
		metricspod.NewController(kubeClient),
		metricsprovisioner.NewController(kubeClient),
		metricsnode.NewController(cluster),
		nodepoolcounter.NewProvisionerController(kubeClient, cluster),
		nodeclaimconsistency.NewMachineController(ctx, clock, kubeClient, cloudProvider),
		nodeclaimlifecycle.NewMachineController(ctx, clock, kubeClient, cloudProvider),
		nodeclaimgarbagecollection.NewController(clock, kubeClient, cloudProvider),
		nodeclaimtermination.NewMachineController(kubeClient, cloudProvider),
		nodeclaimdisruption.NewMachineController(clock, kubeClient, cluster, cloudProvider),
		leasegarbagecollection.NewController(kubeClient),
	}
}
