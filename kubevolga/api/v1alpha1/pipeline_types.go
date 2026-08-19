package v1alpha1

import (
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type VolgaPipelineSpec struct {
	PipelineSpec    json.RawMessage   `json:"pipelineSpec"`
	Workers         VolgaWorkerSpec   `json:"workers,omitempty"`
	Image           string            `json:"image,omitempty"`
	HoldOnFinish    bool              `json:"holdOnFinish,omitempty"`
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
	Master          VolgaPodSpec      `json:"master,omitempty"`
	Worker          VolgaPodSpec      `json:"worker,omitempty"`
}

type VolgaWorkerSpec struct {
	Replicas int32 `json:"replicas,omitempty"`
}

type VolgaPodSpec struct {
	ImagePullPolicy corev1.PullPolicy           `json:"imagePullPolicy,omitempty"`
	Resources       corev1.ResourceRequirements `json:"resources,omitempty"`
	NodeSelector    map[string]string           `json:"nodeSelector,omitempty"`
	Tolerations     []corev1.Toleration         `json:"tolerations,omitempty"`
	Affinity        *corev1.Affinity            `json:"affinity,omitempty"`
}

type VolgaPipelineStatus struct {
	PipelineID        string             `json:"pipelineId,omitempty"`
	Phase             string             `json:"phase,omitempty"`
	MasterServiceAddr string             `json:"masterServiceAddr,omitempty"`
	Conditions        []metav1.Condition `json:"conditions,omitempty"`
	LifecycleEvents   []json.RawMessage  `json:"lifecycleEvents,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type VolgaPipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VolgaPipelineSpec   `json:"spec,omitempty"`
	Status VolgaPipelineStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type VolgaPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VolgaPipeline `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VolgaPipeline{}, &VolgaPipelineList{})
}

func (in *VolgaPipeline) DeepCopyInto(out *VolgaPipeline) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	if in.Spec.PipelineSpec != nil {
		out.Spec.PipelineSpec = make([]byte, len(in.Spec.PipelineSpec))
		copy(out.Spec.PipelineSpec, in.Spec.PipelineSpec)
	}
	out.Spec.Master = deepCopyVolgaPodSpec(in.Spec.Master)
	out.Spec.Worker = deepCopyVolgaPodSpec(in.Spec.Worker)
	if in.Status.Conditions != nil {
		out.Status.Conditions = make([]metav1.Condition, len(in.Status.Conditions))
		copy(out.Status.Conditions, in.Status.Conditions)
	}
	if in.Status.LifecycleEvents != nil {
		out.Status.LifecycleEvents = make([]json.RawMessage, len(in.Status.LifecycleEvents))
		for i := range in.Status.LifecycleEvents {
			out.Status.LifecycleEvents[i] = append(json.RawMessage(nil), in.Status.LifecycleEvents[i]...)
		}
	}
}

func deepCopyVolgaPodSpec(in VolgaPodSpec) VolgaPodSpec {
	out := VolgaPodSpec{
		ImagePullPolicy: in.ImagePullPolicy,
		Resources:       deepCopyResourceRequirements(in.Resources),
	}
	if in.NodeSelector != nil {
		out.NodeSelector = make(map[string]string, len(in.NodeSelector))
		for k, v := range in.NodeSelector {
			out.NodeSelector[k] = v
		}
	}
	if in.Tolerations != nil {
		out.Tolerations = make([]corev1.Toleration, len(in.Tolerations))
		for i := range in.Tolerations {
			in.Tolerations[i].DeepCopyInto(&out.Tolerations[i])
		}
	}
	if in.Affinity != nil {
		out.Affinity = in.Affinity.DeepCopy()
	}
	return out
}

func (in VolgaPodSpec) DeepCopy() VolgaPodSpec {
	return deepCopyVolgaPodSpec(in)
}

func deepCopyResourceRequirements(in corev1.ResourceRequirements) corev1.ResourceRequirements {
	out := corev1.ResourceRequirements{}
	if in.Limits != nil {
		out.Limits = corev1.ResourceList{}
		for k, v := range in.Limits {
			out.Limits[k] = v.DeepCopy()
		}
	}
	if in.Requests != nil {
		out.Requests = corev1.ResourceList{}
		for k, v := range in.Requests {
			out.Requests[k] = v.DeepCopy()
		}
	}
	return out
}

func (in *VolgaPipeline) DeepCopy() *VolgaPipeline {
	if in == nil {
		return nil
	}
	out := new(VolgaPipeline)
	in.DeepCopyInto(out)
	return out
}

func (in *VolgaPipeline) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

func (in *VolgaPipelineList) DeepCopyInto(out *VolgaPipelineList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]VolgaPipeline, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *VolgaPipelineList) DeepCopy() *VolgaPipelineList {
	if in == nil {
		return nil
	}
	out := new(VolgaPipelineList)
	in.DeepCopyInto(out)
	return out
}

func (in *VolgaPipelineList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}
