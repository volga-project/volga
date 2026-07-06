package controller

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v5"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"kubevolga/api/v1alpha1"
)

// +kubebuilder:rbac:groups=volga.io,resources=volgapipelines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=volga.io,resources=volgapipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=volga.io,resources=volgapipelines/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
type PipelineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	masterPort             = int32(50051)
	workerControlPort      = int32(50052)
	workerTransportPort    = int32(60052)
	defaultVolgaImage      = "volga:latest"
	workerIDLabelKey       = "statefulset.kubernetes.io/pod-name"
	kubeAPIServerInCluster = "https://kubernetes.default.svc"
)

//go:embed kube_pipeline_spec.schema.json
var kubePipelineSpecSchema string

var (
	compileKubePipelineSchemaOnce sync.Once
	kubePipelineSchema            *jsonschema.Schema
	kubePipelineSchemaErr         error
)

func (r *PipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pipeline", req.NamespacedName.String())

	var vp v1alpha1.VolgaPipeline
	if err := r.Get(ctx, req.NamespacedName, &vp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	replicas := vp.Spec.Workers.Replicas
	if replicas <= 0 {
		replicas = 1
	}

	volgaImage := vp.Spec.Image
	if volgaImage == "" {
		volgaImage = defaultVolgaImage
	}
	masterPullPolicy := resolvePullPolicy(vp.Spec.ImagePullPolicy, vp.Spec.Master.ImagePullPolicy)
	workerPullPolicy := resolvePullPolicy(vp.Spec.ImagePullPolicy, vp.Spec.Worker.ImagePullPolicy)
	masterResources, err := resolveRequiredResources(vp.Spec.Master.Resources, "master")
	if err != nil {
		return ctrl.Result{}, err
	}
	workerResources, err := resolveRequiredResources(vp.Spec.Worker.Resources, "worker")
	if err != nil {
		return ctrl.Result{}, err
	}

	baseLabels := map[string]string{
		"app.kubernetes.io/part-of": "kubevolga",
		"volga.io/name":             vp.Name,
	}
	masterLabels := cloneAndAdd(baseLabels, map[string]string{"volga.io/component": "master"})
	workerLabels := cloneAndAdd(baseLabels, map[string]string{"volga.io/component": "worker"})
	workerLabelSelector := fmt.Sprintf("volga.io/name=%s,volga.io/component=worker", vp.Name)

	masterServiceName := fmt.Sprintf("%s-master", vp.Name)
	workerServiceName := fmt.Sprintf("%s-workers", vp.Name)
	runtimeServiceAccountName := fmt.Sprintf("%s-runtime", vp.Name)
	runtimeRoleName := fmt.Sprintf("%s-runtime-role", vp.Name)
	runtimeRoleBindingName := fmt.Sprintf("%s-runtime-rolebinding", vp.Name)
	masterServiceAddr := fmt.Sprintf("%s.%s.svc.cluster.local:%d", masterServiceName, vp.Namespace, masterPort)
	pipelineID := string(vp.UID)
	if pipelineID == "" {
		// Fallback for safety; UID should be present for persisted CRs.
		pipelineID = fmt.Sprintf("%s-%s", vp.Namespace, vp.Name)
	}
	if err := validatePipelineSpec(vp.Spec.PipelineSpec); err != nil {
		if statusErr := r.updatePipelineStatus(ctx, req.NamespacedName, pipelineID, "", "InvalidSpec"); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		logger.Error(err, "invalid pipelineSpec; skipping resource reconciliation")
		return ctrl.Result{}, nil
	}

	if err := r.reconcileMasterService(ctx, &vp, masterServiceName, masterLabels); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileWorkerHeadlessService(ctx, &vp, workerServiceName, workerLabels); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileRuntimeServiceAccount(ctx, &vp, runtimeServiceAccountName); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileRuntimeRole(ctx, &vp, runtimeRoleName); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileRuntimeRoleBinding(ctx, &vp, runtimeRoleBindingName, runtimeRoleName, runtimeServiceAccountName); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileMasterPod(
		ctx,
		&vp,
		runtimeServiceAccountName,
		volgaImage,
		masterPullPolicy,
		masterResources,
		masterLabels,
		workerLabelSelector,
		masterServiceAddr,
		pipelineID,
	); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileWorkerStatefulSet(
		ctx,
		&vp,
		workerServiceName,
		volgaImage,
		workerPullPolicy,
		workerResources,
		workerLabels,
		replicas,
		masterServiceAddr,
	); err != nil {
		return ctrl.Result{}, err
	}

	phase := "Starting"
	var masterPod corev1.Pod
	if err := r.Get(ctx, client.ObjectKey{Namespace: vp.Namespace, Name: fmt.Sprintf("%s-master", vp.Name)}, &masterPod); err == nil {
		if masterPod.Status.Phase == corev1.PodFailed {
			phase = "Failed"
		}
		var sts appsv1.StatefulSet
		if err := r.Get(ctx, client.ObjectKey{Namespace: vp.Namespace, Name: fmt.Sprintf("%s-worker", vp.Name)}, &sts); err == nil {
			if masterPod.Status.Phase == corev1.PodRunning && sts.Status.ReadyReplicas == replicas {
				phase = "Running"
			}
		}
	}

	if vp.Status.MasterServiceAddr != masterServiceAddr || vp.Status.Phase != phase || vp.Status.PipelineID != pipelineID {
		if err := r.updatePipelineStatus(ctx, req.NamespacedName, pipelineID, masterServiceAddr, phase); err != nil {
			return ctrl.Result{}, err
		}
	}

	logger.Info("reconcile complete", "phase", phase, "masterAddr", masterServiceAddr, "replicas", replicas)
	if phase != "Running" {
		return ctrl.Result{RequeueAfter: 3 * 1000 * 1000 * 1000}, nil
	}
	return ctrl.Result{RequeueAfter: 30 * 1000 * 1000 * 1000}, nil
}

func (r *PipelineReconciler) reconcileMasterService(
	ctx context.Context,
	vp *v1alpha1.VolgaPipeline,
	name string,
	labels map[string]string,
) error {
	var svc corev1.Service
	svc.Namespace = vp.Namespace
	svc.Name = name
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, &svc, func() error {
			if err := controllerutil.SetControllerReference(vp, &svc, r.Scheme); err != nil {
				return err
			}
			svc.Labels = labels
			svc.Spec.Selector = labels
			svc.Spec.Ports = []corev1.ServicePort{{
				Name: "grpc",
				Port: masterPort,
			}}
			return nil
		})
		return err
	})
	return err
}

func (r *PipelineReconciler) reconcileWorkerHeadlessService(
	ctx context.Context,
	vp *v1alpha1.VolgaPipeline,
	name string,
	labels map[string]string,
) error {
	var svc corev1.Service
	svc.Namespace = vp.Namespace
	svc.Name = name
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, &svc, func() error {
			if err := controllerutil.SetControllerReference(vp, &svc, r.Scheme); err != nil {
				return err
			}
			svc.Labels = labels
			svc.Spec.ClusterIP = "None"
			svc.Spec.Selector = labels
			svc.Spec.Ports = []corev1.ServicePort{
				{Name: "control", Port: workerControlPort},
				{Name: "transport", Port: workerTransportPort},
			}
			return nil
		})
		return err
	})
	return err
}

func (r *PipelineReconciler) reconcileMasterPod(
	ctx context.Context,
	vp *v1alpha1.VolgaPipeline,
	serviceAccountName string,
	image string,
	imagePullPolicy corev1.PullPolicy,
	resources corev1.ResourceRequirements,
	labels map[string]string,
	workerLabelSelector string,
	masterServiceAddr string,
	pipelineID string,
) error {
	masterName := fmt.Sprintf("%s-master", vp.Name)

	holdOnFinish := "false"
	if vp.Spec.HoldOnFinish {
		holdOnFinish = "true"
	}

	var pod corev1.Pod
	if err := r.Get(ctx, client.ObjectKey{Namespace: vp.Namespace, Name: masterName}, &pod); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		create := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: vp.Namespace,
				Name:      masterName,
				Labels:    labels,
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: serviceAccountName,
				RestartPolicy:      corev1.RestartPolicyNever,
				Containers: []corev1.Container{{
					Name:            "master",
					Image:           image,
					ImagePullPolicy: imagePullPolicy,
					Resources:       resources,
					Command:         []string{"volga-master"},
					Ports:           []corev1.ContainerPort{{ContainerPort: masterPort, Name: "grpc"}},
					Env: []corev1.EnvVar{
						{Name: "VOLGA_ORCHESTRATOR_KIND", Value: "kube"},
						{Name: "VOLGA_MASTER_BIND_ADDR", Value: "0.0.0.0:" + strconv.FormatInt(int64(masterPort), 10)},
						{Name: "VOLGA_MASTER_HOLD_ON_FINISH", Value: holdOnFinish},
						{Name: "VOLGA_PIPELINE_CRD_NAME", Value: vp.Name},
						{Name: "VOLGA_WORKER_LABEL_SELECTOR", Value: workerLabelSelector},
						{Name: "VOLGA_WORKER_ID_LABEL", Value: workerIDLabelKey},
						{Name: "VOLGA_WORKER_PORT", Value: strconv.FormatInt(int64(workerControlPort), 10)},
						{Name: "VOLGA_WORKER_TRANSPORT_PORT", Value: strconv.FormatInt(int64(workerTransportPort), 10)},
						{Name: "VOLGA_PIPELINE_ID", Value: pipelineID},
						{Name: "KUBE_API_SERVER", Value: kubeAPIServerInCluster},
						{
							Name: "KUBE_NAMESPACE",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
							},
						},
						{
							Name: "POD_NAMESPACE",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
							},
						},
						{Name: "MASTER_SERVICE_ADDR", Value: masterServiceAddr},
					},
				}},
			},
		}
		if err := controllerutil.SetControllerReference(vp, &create, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, &create)
	}

	return nil
}

func (r *PipelineReconciler) reconcileWorkerStatefulSet(
	ctx context.Context,
	vp *v1alpha1.VolgaPipeline,
	serviceName string,
	image string,
	imagePullPolicy corev1.PullPolicy,
	resources corev1.ResourceRequirements,
	labels map[string]string,
	replicas int32,
	masterServiceAddr string,
) error {
	var sts appsv1.StatefulSet
	sts.Namespace = vp.Namespace
	sts.Name = fmt.Sprintf("%s-worker", vp.Name)

	holdOnFinish := "false"
	if vp.Spec.HoldOnFinish {
		holdOnFinish = "true"
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, &sts, func() error {
			if err := controllerutil.SetControllerReference(vp, &sts, r.Scheme); err != nil {
				return err
			}
			sts.Labels = labels
			sts.Spec.ServiceName = serviceName
			sts.Spec.Replicas = ptr.To(replicas)
			sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
			sts.Spec.Template.ObjectMeta.Labels = labels
			sts.Spec.Template.Spec.Containers = []corev1.Container{{
				Name:            "worker",
				Image:           image,
				ImagePullPolicy: imagePullPolicy,
				Resources:       resources,
				Command:         []string{"volga-worker"},
				Ports: []corev1.ContainerPort{
					{ContainerPort: workerControlPort, Name: "control"},
					{ContainerPort: workerTransportPort, Name: "transport"},
				},
				Env: []corev1.EnvVar{
					{Name: "VOLGA_ORCHESTRATOR_KIND", Value: "kube"},
					{Name: "VOLGA_WORKER_BIND_ADDR", Value: "0.0.0.0:" + strconv.FormatInt(int64(workerControlPort), 10)},
					{Name: "VOLGA_WORKER_HOLD_ON_FINISH", Value: holdOnFinish},
					{Name: "MASTER_SERVICE_ADDR", Value: masterServiceAddr},
					{
						Name: "VOLGA_WORKER_ID",
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
						},
					},
				},
			}}
			return nil
		})
		return err
	})
	return err
}

func (r *PipelineReconciler) reconcileRuntimeServiceAccount(
	ctx context.Context,
	vp *v1alpha1.VolgaPipeline,
	name string,
) error {
	var sa corev1.ServiceAccount
	sa.Namespace = vp.Namespace
	sa.Name = name
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, &sa, func() error {
			if err := controllerutil.SetControllerReference(vp, &sa, r.Scheme); err != nil {
				return err
			}
			sa.Labels = cloneAndAdd(map[string]string{
				"app.kubernetes.io/part-of": "kubevolga",
				"volga.io/name":             vp.Name,
			}, map[string]string{"volga.io/component": "runtime"})
			return nil
		})
		return err
	})
}

func (r *PipelineReconciler) reconcileRuntimeRole(
	ctx context.Context,
	vp *v1alpha1.VolgaPipeline,
	name string,
) error {
	var role rbacv1.Role
	role.Namespace = vp.Namespace
	role.Name = name
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, &role, func() error {
			if err := controllerutil.SetControllerReference(vp, &role, r.Scheme); err != nil {
				return err
			}
			role.Labels = cloneAndAdd(map[string]string{
				"app.kubernetes.io/part-of": "kubevolga",
				"volga.io/name":             vp.Name,
			}, map[string]string{"volga.io/component": "runtime"})
			role.Rules = []rbacv1.PolicyRule{
				{
					APIGroups: []string{"volga.io"},
					Resources: []string{"volgapipelines"},
					Verbs:     []string{"get", "list", "watch"},
				},
				{
					APIGroups: []string{""},
					Resources: []string{"pods"},
					Verbs:     []string{"get", "list", "watch"},
				},
			}
			return nil
		})
		return err
	})
}

func (r *PipelineReconciler) reconcileRuntimeRoleBinding(
	ctx context.Context,
	vp *v1alpha1.VolgaPipeline,
	name string,
	roleName string,
	serviceAccountName string,
) error {
	var roleBinding rbacv1.RoleBinding
	roleBinding.Namespace = vp.Namespace
	roleBinding.Name = name
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, &roleBinding, func() error {
			if err := controllerutil.SetControllerReference(vp, &roleBinding, r.Scheme); err != nil {
				return err
			}
			roleBinding.Labels = cloneAndAdd(map[string]string{
				"app.kubernetes.io/part-of": "kubevolga",
				"volga.io/name":             vp.Name,
			}, map[string]string{"volga.io/component": "runtime"})
			roleBinding.RoleRef = rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "Role",
				Name:     roleName,
			}
			roleBinding.Subjects = []rbacv1.Subject{{
				Kind:      "ServiceAccount",
				Name:      serviceAccountName,
				Namespace: vp.Namespace,
			}}
			return nil
		})
		return err
	})
}

func cloneAndAdd(base map[string]string, extra map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func validatePipelineSpec(raw json.RawMessage) error {
	schema, err := getKubePipelineSchema()
	if err != nil {
		return err
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("pipelineSpec must be valid JSON: %w", err)
	}
	if err := schema.Validate(value); err != nil {
		return fmt.Errorf("pipelineSpec does not match KubePipelineSpec schema: %w", err)
	}
	return nil
}

func getKubePipelineSchema() (*jsonschema.Schema, error) {
	compileKubePipelineSchemaOnce.Do(func() {
		compiler := jsonschema.NewCompiler()
		const schemaURL = "kube_pipeline_spec.schema.json"
		kubePipelineSchemaErr = compiler.AddResource(schemaURL, strings.NewReader(kubePipelineSpecSchema))
		if kubePipelineSchemaErr != nil {
			return
		}
		kubePipelineSchema, kubePipelineSchemaErr = compiler.Compile(schemaURL)
	})
	return kubePipelineSchema, kubePipelineSchemaErr
}

func (r *PipelineReconciler) updatePipelineStatus(
	ctx context.Context,
	key client.ObjectKey,
	pipelineID string,
	masterServiceAddr string,
	phase string,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest v1alpha1.VolgaPipeline
		if err := r.Get(ctx, key, &latest); err != nil {
			return err
		}
		latest.Status.PipelineID = pipelineID
		latest.Status.MasterServiceAddr = masterServiceAddr
		latest.Status.Phase = phase
		return r.Status().Update(ctx, &latest)
	})
}

func resolvePullPolicy(global corev1.PullPolicy, component corev1.PullPolicy) corev1.PullPolicy {
	if component != "" {
		return component
	}
	if global != "" {
		return global
	}
	return corev1.PullIfNotPresent
}

func resolveRequiredResources(componentResources corev1.ResourceRequirements, component string) (corev1.ResourceRequirements, error) {
	if hasResources(componentResources) {
		return cloneResourceRequirements(componentResources), nil
	}
	return corev1.ResourceRequirements{}, fmt.Errorf(
		"resources are required for %s: set spec.%s.resources",
		component,
		component,
	)
}

func hasResources(resources corev1.ResourceRequirements) bool {
	return len(resources.Limits) > 0 || len(resources.Requests) > 0
}

func cloneResourceRequirements(in corev1.ResourceRequirements) corev1.ResourceRequirements {
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

func (r *PipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.VolgaPipeline{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Complete(r)
}
