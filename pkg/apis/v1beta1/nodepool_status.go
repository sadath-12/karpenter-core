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

package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
)

// NodePoolStatus defines the observed state of NodePool
type NodePoolStatus struct {
	// Resources is the list of resources that have been provisioned.
	// +optional
	Resources v1.ResourceList `json:"resources,omitempty"`
	// Conditions represent the latest available observations of a NodePool's current state.
	// +optional
	Conditions apis.Conditions `json:"conditions,omitempty"`
}

var (
	NodeClassReady apis.ConditionType = "NodeClassReady"
)

func (in *NodePool) StatusConditions() apis.ConditionManager {
	return apis.NewLivingConditionSet(
		NodeClassReady,
	).Manage(in)
}

func (in *NodePool) GetConditions() apis.Conditions {
	return in.Status.Conditions
}

func (in *NodePool) SetConditions(conditions apis.Conditions) {
	in.Status.Conditions = conditions
}

