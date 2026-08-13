package v1alpha1

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestVolgaPodSpecDeepCopyDoesNotAlias(t *testing.T) {
	in := VolgaPipeline{
		Spec: VolgaPipelineSpec{
			Master: VolgaPodSpec{
				NodeSelector: map[string]string{"volga.io/role": "infra"},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("100m"),
					},
				},
			},
			Worker: VolgaPodSpec{
				NodeSelector: map[string]string{"volga.io/role": "worker"},
				Tolerations: []corev1.Toleration{{
					Key:      "volga.io/role",
					Operator: corev1.TolerationOpEqual,
					Value:    "worker",
					Effect:   corev1.TaintEffectNoSchedule,
				}},
				Affinity: &corev1.Affinity{
					PodAntiAffinity: &corev1.PodAntiAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
							TopologyKey: "kubernetes.io/hostname",
						}},
					},
				},
			},
		},
	}

	out := in.DeepCopy()
	out.Spec.Master.NodeSelector["volga.io/role"] = "mutated"
	out.Spec.Worker.Tolerations[0].Value = "mutated"
	out.Spec.Worker.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey = "mutated"
	out.Spec.Master.Resources.Requests[corev1.ResourceCPU] = resource.MustParse("1")

	if in.Spec.Master.NodeSelector["volga.io/role"] != "infra" {
		t.Fatalf("master nodeSelector aliased: %v", in.Spec.Master.NodeSelector)
	}
	if in.Spec.Worker.Tolerations[0].Value != "worker" {
		t.Fatalf("worker tolerations aliased: %+v", in.Spec.Worker.Tolerations[0])
	}
	if got := in.Spec.Worker.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey; got != "kubernetes.io/hostname" {
		t.Fatalf("worker affinity aliased: %s", got)
	}
	cpu := in.Spec.Master.Resources.Requests[corev1.ResourceCPU]
	if !cpu.Equal(resource.MustParse("100m")) {
		t.Fatalf("master resources aliased: %s", cpu.String())
	}
}
