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

import v1 "k8s.io/api/core/v1"

// Karpenter specific taints
const (
	DisruptionTaintKey             = Group + "/disruption"
	DisruptingNoScheduleTaintValue = "disrupting"
	TerminationTaintKey            = Group + "/termination"
	TerminationNoExecuteTaintValue = "Termination"
)


var TaintFuncs = map[v1.Taint]func(taint v1.Taint) bool{
	DisruptionNoScheduleTaint: func(taint v1.Taint) bool {
		return IsDisruptingTaint(taint) // Call IsDisruptingTaint with the required argument
	},
	TerminationNoExecuteTaint: func(taint v1.Taint) bool {
		return IsTerminatingTaint(taint) // Call IsTerminatingTaint with the required argument
	},
}

var (
	// DisruptionNoScheduleTaint is used by the deprovisioning controller to ensure no pods
	// are scheduled to a node that Karpenter is actively disrupting.
	DisruptionNoScheduleTaint = v1.Taint{
		Key:    DisruptionTaintKey,
		Effect: v1.TaintEffectNoSchedule,
		Value:  DisruptingNoScheduleTaintValue,
	}
	TerminationNoExecuteTaint = v1.Taint{
		Key:    TerminationTaintKey,
		Effect: v1.TaintEffectNoExecute,
		Value:  TerminationNoExecuteTaintValue,
	}
)

func IsDisruptingTaint(taint v1.Taint) bool {
	return taint.MatchTaint(&DisruptionNoScheduleTaint) && taint.Value == DisruptingNoScheduleTaintValue
}

func IsTerminatingTaint(taint v1.Taint) bool {
	return taint.MatchTaint(&TerminationNoExecuteTaint) && taint.Value == TerminationNoExecuteTaintValue
}
