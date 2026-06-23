/*
Copyright 2026.

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

package controller

import (
	"context"
	"fmt"
	"os"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	appsv1alpha1 "github.com/entr0pian/taskapp-operator/api/v1alpha1"
)

const backendFinalizer = "apps.taskapp.io/finalizer"

var sqsQueueGVK = schema.GroupVersionKind{
	Group:   "sqs.aws.upbound.io",
	Version: "v1beta1",
	Kind:    "Queue",
}

var externalSecretGVK = schema.GroupVersionKind{
	Group:   "external-secrets.io",
	Version: "v1beta1",
	Kind:    "ExternalSecret",
}

var rdsInstanceGVK = schema.GroupVersionKind{
	Group:   "database.taskapp.io",
	Version: "v1alpha1",
	Kind:    "RDSInstance",
}

// BackendReconciler reconciles a Backend object
type BackendReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=apps.taskapp.io,resources=backends,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.taskapp.io,resources=backends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.taskapp.io,resources=backends/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sqs.aws.upbound.io,resources=queues,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sqs.aws.upbound.io,resources=queues/status,verbs=get
// +kubebuilder:rbac:groups=external-secrets.io,resources=externalsecrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=database.taskapp.io,resources=rdsinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=database.taskapp.io,resources=rdsinstances/status,verbs=get

func (r *BackendReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	backend := &appsv1alpha1.Backend{}
	if err := r.Get(ctx, req.NamespacedName, backend); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !backend.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.handleDeletion(ctx, backend)
	}

	if !controllerutil.ContainsFinalizer(backend, backendFinalizer) {
		log.Info("adding finalizer")
		controllerutil.AddFinalizer(backend, backendFinalizer)
		if err := r.Update(ctx, backend); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	statusBase := backend.DeepCopy()
	if requeue, err := r.reconcileRDS(ctx, backend); err != nil {
		log.Error(err, "failed to reconcile RDSInstance")
		r.Recorder.Event(backend, corev1.EventTypeWarning, "ReconcileError", err.Error())
		return ctrl.Result{}, err
	} else if requeue {
		if err := r.patchStatusIfChanged(ctx, statusBase, backend); err != nil {
			log.Error(err, "failed to update status")
			r.Recorder.Event(backend, corev1.EventTypeWarning, "ReconcileError", err.Error())
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if err := r.reconcileExternalSecret(ctx, backend); err != nil {
		log.Error(err, "failed to reconcile ExternalSecret")
		r.Recorder.Event(backend, corev1.EventTypeWarning, "ReconcileError", err.Error())
		return ctrl.Result{}, err
	}

	queueURL, requeue, err := r.reconcileSQS(ctx, backend)
	if err != nil {
		log.Error(err, "failed to reconcile SQS Queue")
		r.Recorder.Event(backend, corev1.EventTypeWarning, "ReconcileError", err.Error())
		return ctrl.Result{}, err
	}

	if requeue {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if err := r.reconcileDeployment(ctx, backend, queueURL); err != nil {
		log.Error(err, "failed to reconcile Deployment")
		r.Recorder.Event(backend, corev1.EventTypeWarning, "ReconcileError", err.Error())
		return ctrl.Result{}, err
	}

	if err := r.reconcileService(ctx, backend); err != nil {
		log.Error(err, "failed to reconcile Service")
		r.Recorder.Event(backend, corev1.EventTypeWarning, "ReconcileError", err.Error())
		return ctrl.Result{}, err
	}

	if err := r.updateStatus(ctx, statusBase, backend, queueURL); err != nil {
		log.Error(err, "failed to update status")
		r.Recorder.Event(backend, corev1.EventTypeWarning, "ReconcileError", err.Error())
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *BackendReconciler) reconcileSQS(ctx context.Context, backend *appsv1alpha1.Backend) (string, bool, error) {
	log := logf.FromContext(ctx)

	if backend.Spec.Queue == nil {
		log.Info("queue spec removed, deleting owned queues")
		return "", false, r.deleteQueue(ctx, backend)
	}

	// If deadLetter is requested, ensure the DLQ exists and is ready first.
	dlqARN := ""
	if backend.Spec.Queue.DeadLetter {
		arn, requeue, err := r.reconcileQueue(ctx, backend, sqsDLQName(backend), "", false, true) // returnARN=true: ARN needed for main queue redrivePolicy
		if err != nil {
			return "", false, err
		}
		if requeue || arn == "" {
			log.Info("waiting for DLQ to become ready", "queue", sqsDLQName(backend))
			return "", true, nil
		}
		log.Info("DLQ ready", "queue", sqsDLQName(backend))
		dlqARN = arn
	}

	url, requeue, err := r.reconcileQueue(ctx, backend, sqsQueueName(backend), dlqARN, backend.Spec.Queue.Type == appsv1alpha1.QueueTypeFifo, false) // returnARN=false: URL needed for SQS_QUEUE_URL env var
	if err != nil {
		return "", false, err
	}
	if requeue || url == "" {
		log.Info("waiting for Queue to become ready", "queue", sqsQueueName(backend))
		return "", true, nil
	}
	return url, false, nil
}

func (r *BackendReconciler) reconcileRDS(ctx context.Context, backend *appsv1alpha1.Backend) (bool, error) {
	log := logf.FromContext(ctx)

	if backend.Spec.Database == nil {
		log.Info("database spec removed, deleting owned RDS instances")
		if err := r.deleteRDS(ctx, backend); err != nil {
			return false, err
		}
		backend.Status.Conditions = removeCondition(backend.Status.Conditions, "RDSReady")
		return false, nil
	}

	desired := &unstructured.Unstructured{}
	desired.SetGroupVersionKind(rdsInstanceGVK)
	desired.SetName(rdsInstanceName(backend))
	desired.SetNamespace(backend.Namespace)
	desired.SetLabels(map[string]string{
		"apps.taskapp.io/owned-by-backend":   backend.Name,
		"apps.taskapp.io/owned-by-namespace": backend.Namespace,
	})

	parameters := rdsParameters(backend.Spec.Database)
	if err := unstructured.SetNestedField(desired.Object, parameters, "spec", "parameters"); err != nil {
		return false, err
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(rdsInstanceGVK)
	err := r.Get(ctx, types.NamespacedName{Name: desired.GetName(), Namespace: desired.GetNamespace()}, existing)
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return false, err
		}
		log.Info("created RDSInstance", "rdsinstance", desired.GetName())
		r.Recorder.Event(backend, corev1.EventTypeNormal, "RDSInstanceCreated", fmt.Sprintf("Created RDSInstance %s", desired.GetName()))
		r.setRDSReadyCondition(backend, metav1.ConditionFalse, "RDSProvisioning", "created RDSInstance claim and waiting for readiness")
		return true, nil
	}
	if apimeta.IsNoMatchError(err) {
		log.Info("RDSInstance CRD not yet installed, skipping")
		r.setRDSReadyCondition(backend, metav1.ConditionFalse, "RDSInstanceCRDNotInstalled", "waiting for RDSInstance CRD to be installed")
		return true, nil
	}
	if err != nil {
		return false, err
	}

	existingParameters, _, _ := unstructured.NestedMap(existing.Object, "spec", "parameters")
	if !equality.Semantic.DeepEqual(existingParameters, parameters) {
		patch := client.MergeFrom(existing.DeepCopy())
		if err := unstructured.SetNestedField(existing.Object, parameters, "spec", "parameters"); err != nil {
			return false, err
		}
		existing.SetLabels(desired.GetLabels())
		if err := r.Patch(ctx, existing, patch); err != nil {
			return false, err
		}
		log.Info("patched RDSInstance", "rdsinstance", existing.GetName())
	}

	if !isReady(existing) {
		log.Info("RDSInstance not yet ready, requeueing", "rdsinstance", existing.GetName())
		r.setRDSReadyCondition(backend, metav1.ConditionFalse, "RDSProvisioning", "waiting for RDSInstance claim to become ready")
		return true, nil
	}

	log.Info("RDSInstance ready", "rdsinstance", existing.GetName())
	r.setRDSReadyCondition(backend, metav1.ConditionTrue, "RDSReady", "RDSInstance claim is ready")
	return false, nil
}

func (r *BackendReconciler) deleteRDS(ctx context.Context, backend *appsv1alpha1.Backend) error {
	log := logf.FromContext(ctx)

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   rdsInstanceGVK.Group,
		Version: rdsInstanceGVK.Version,
		Kind:    "RDSInstanceList",
	})
	if err := r.List(ctx, list, client.InNamespace(backend.Namespace), client.MatchingLabels{
		"apps.taskapp.io/owned-by-backend":   backend.Name,
		"apps.taskapp.io/owned-by-namespace": backend.Namespace,
	}); err != nil {
		if apimeta.IsNoMatchError(err) {
			log.Info("RDSInstance CRD not yet installed, skipping RDS cleanup")
			return nil
		}
		return err
	}
	for i := range list.Items {
		if err := client.IgnoreNotFound(r.Delete(ctx, &list.Items[i])); err != nil {
			return err
		}
		log.Info("deleted owned RDSInstance", "rdsinstance", list.Items[i].GetName())
	}
	return nil
}

// reconcileQueue ensures a single Crossplane Queue CR exists with the given name.
// Set returnARN=true when the caller needs the queue's ARN (DLQ calls, for use in
// redrivePolicy); set returnARN=false to get the queue URL (main queue, for SQS_QUEUE_URL).
func (r *BackendReconciler) reconcileQueue(ctx context.Context, backend *appsv1alpha1.Backend, name, dlqARN string, fifo, returnARN bool) (string, bool, error) {
	log := logf.FromContext(ctx)

	desired := &unstructured.Unstructured{}
	desired.SetGroupVersionKind(sqsQueueGVK)
	desired.SetName(name)
	desired.SetLabels(map[string]string{
		"apps.taskapp.io/owned-by-backend":   backend.Name,
		"apps.taskapp.io/owned-by-namespace": backend.Namespace,
	})

	forProvider := map[string]any{
		"name":   name,
		"region": sqsRegion(),
		"tags": map[string]any{
			"managed-by": "taskapp-operator",
		},
	}
	if fifo {
		forProvider["fifoQueue"] = true
	}
	if dlqARN != "" {
		forProvider["redrivePolicy"] = []any{
			map[string]any{
				"deadLetterTargetArn": dlqARN,
				"maxReceiveCount":     5,
			},
		}
	}

	if err := unstructured.SetNestedField(desired.Object, forProvider, "spec", "forProvider"); err != nil {
		return "", false, err
	}
	if err := unstructured.SetNestedField(desired.Object, "default", "spec", "providerConfigRef", "name"); err != nil {
		return "", false, err
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(sqsQueueGVK)
	err := r.Get(ctx, types.NamespacedName{Name: name}, existing)
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return "", false, err
		}
		log.Info("created queue", "queue", name)
		r.Recorder.Event(backend, corev1.EventTypeNormal, "QueueCreated", fmt.Sprintf("Created Queue %s", name))
		return "", true, nil
	}
	if err != nil {
		return "", false, err
	}

	// Check Ready condition
	conditions, _, _ := unstructured.NestedSlice(existing.Object, "status", "conditions")
	ready := false
	for _, c := range conditions {
		cond, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if cond["type"] == "Ready" && cond["status"] == "True" {
			ready = true
			break
		}
	}
	if !ready {
		log.Info("queue not yet ready, requeueing", "queue", name)
		return "", true, nil
	}

	log.Info("queue ready", "queue", name)
	if returnARN {
		arn, _, _ := unstructured.NestedString(existing.Object, "status", "atProvider", "arn")
		return arn, false, nil
	}
	url, _, _ := unstructured.NestedString(existing.Object, "status", "atProvider", "url")
	return url, false, nil
}

func (r *BackendReconciler) deleteQueue(ctx context.Context, backend *appsv1alpha1.Backend) error {
	log := logf.FromContext(ctx)

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   sqsQueueGVK.Group,
		Version: sqsQueueGVK.Version,
		Kind:    "QueueList",
	})
	if err := r.List(ctx, list, client.MatchingLabels{
		"apps.taskapp.io/owned-by-backend":   backend.Name,
		"apps.taskapp.io/owned-by-namespace": backend.Namespace,
	}); err != nil {
		return err
	}
	for i := range list.Items {
		if err := client.IgnoreNotFound(r.Delete(ctx, &list.Items[i])); err != nil {
			return err
		}
		log.Info("deleted owned queue", "queue", list.Items[i].GetName())
	}
	return nil
}

func (r *BackendReconciler) reconcileExternalSecret(ctx context.Context, backend *appsv1alpha1.Backend) error {
	log := logf.FromContext(ctx)
	name := backend.Name + "-aws-credentials"

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(externalSecretGVK)

	if backend.Spec.Queue == nil {
		err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: backend.Namespace}, existing)
		if errors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := r.Delete(ctx, existing); err != nil {
			return client.IgnoreNotFound(err)
		}
		log.Info("deleted ExternalSecret", "name", name)
		return nil
	}

	desired := &unstructured.Unstructured{}
	desired.SetGroupVersionKind(externalSecretGVK)
	desired.SetName(name)
	desired.SetNamespace(backend.Namespace)
	if err := ctrl.SetControllerReference(backend, desired, r.Scheme); err != nil {
		return err
	}

	spec := map[string]any{
		"refreshInterval": "1h",
		"secretStoreRef": map[string]any{
			"name": clusterSecretStoreName(),
			"kind": "ClusterSecretStore",
		},
		"target": map[string]any{
			"name":           name,
			"creationPolicy": "Owner",
		},
		"data": []any{
			map[string]any{
				"secretKey": "AWS_ACCESS_KEY_ID",
				"remoteRef": map[string]any{
					"key":      credentialsSecretPath(),
					"property": "aws_access_key_id",
				},
			},
			map[string]any{
				"secretKey": "AWS_SECRET_ACCESS_KEY",
				"remoteRef": map[string]any{
					"key":      credentialsSecretPath(),
					"property": "aws_secret_access_key",
				},
			},
		},
	}
	if err := unstructured.SetNestedField(desired.Object, spec, "spec"); err != nil {
		return err
	}

	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: backend.Namespace}, existing)
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return err
		}
		log.Info("created ExternalSecret", "name", name)
		r.Recorder.Event(backend, corev1.EventTypeNormal, "ExternalSecretCreated", fmt.Sprintf("Created ExternalSecret %s", name))
		return nil
	}
	if err != nil {
		return err
	}

	patch := client.MergeFrom(existing.DeepCopy())
	if err := unstructured.SetNestedField(existing.Object, spec, "spec"); err != nil {
		return err
	}
	if err := r.Patch(ctx, existing, patch); err != nil {
		return err
	}
	log.Info("patched ExternalSecret", "name", name)
	return nil
}

func (r *BackendReconciler) handleDeletion(ctx context.Context, backend *appsv1alpha1.Backend) error {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(backend, backendFinalizer) {
		return nil
	}

	log.Info("handling deletion")

	if err := r.deleteDeployment(ctx, backend); err != nil {
		return err
	}
	if err := r.deleteService(ctx, backend); err != nil {
		return err
	}
	if err := r.deleteQueue(ctx, backend); err != nil {
		return err
	}
	if err := r.deleteRDS(ctx, backend); err != nil {
		return err
	}

	log.Info("finalizer removed, deletion complete")
	controllerutil.RemoveFinalizer(backend, backendFinalizer)
	return r.Update(ctx, backend)
}

func (r *BackendReconciler) deleteDeployment(ctx context.Context, backend *appsv1alpha1.Backend) error {
	deploy := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: deploymentName(backend), Namespace: backend.Namespace}, deploy)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return client.IgnoreNotFound(r.Delete(ctx, deploy))
}

func (r *BackendReconciler) deleteService(ctx context.Context, backend *appsv1alpha1.Backend) error {
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: serviceName(backend), Namespace: backend.Namespace}, svc)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return client.IgnoreNotFound(r.Delete(ctx, svc))
}

func (r *BackendReconciler) reconcileDeployment(ctx context.Context, backend *appsv1alpha1.Backend, queueURL string) error {
	log := logf.FromContext(ctx)

	desired := r.buildDeployment(backend, queueURL)
	if err := ctrl.SetControllerReference(backend, desired, r.Scheme); err != nil {
		return err
	}

	existing := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return err
		}
		log.Info("created deployment", "deployment", desired.Name)
		r.Recorder.Event(backend, corev1.EventTypeNormal, "DeploymentCreated", "Created deployment")
		return nil
	}
	if err != nil {
		return err
	}

	desiredContainer := desired.Spec.Template.Spec.Containers[0]
	existingContainer := existing.Spec.Template.Spec.Containers[0]
	if equality.Semantic.DeepEqual(existing.Spec.Replicas, desired.Spec.Replicas) &&
		existingContainer.Image == desiredContainer.Image &&
		equality.Semantic.DeepEqual(existingContainer.Env, desiredContainer.Env) {
		return nil
	}
	patch := client.MergeFrom(existing.DeepCopy())
	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.Template.Spec.Containers[0].Image = desiredContainer.Image
	existing.Spec.Template.Spec.Containers[0].Env = desiredContainer.Env
	if err := r.Patch(ctx, existing, patch); err != nil {
		return err
	}
	log.Info("updated deployment", "deployment", existing.Name)
	r.Recorder.Event(backend, corev1.EventTypeNormal, "DeploymentUpdated", "Updated deployment")
	return nil
}

func (r *BackendReconciler) reconcileService(ctx context.Context, backend *appsv1alpha1.Backend) error {
	log := logf.FromContext(ctx)

	desired := r.buildService(backend)
	if err := ctrl.SetControllerReference(backend, desired, r.Scheme); err != nil {
		return err
	}

	existing := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if errors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return err
		}
		log.Info("created service", "service", desired.Name)
		return nil
	}
	if err != nil {
		return err
	}

	if equality.Semantic.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) &&
		equality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		return nil
	}
	existing.Spec.Ports = desired.Spec.Ports
	existing.Spec.Selector = desired.Spec.Selector
	if err := r.Update(ctx, existing); err != nil {
		return err
	}
	log.Info("updated service", "service", existing.Name)
	return nil
}

func (r *BackendReconciler) updateStatus(ctx context.Context, statusBase, backend *appsv1alpha1.Backend, queueURL string) error {
	deploy := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Name: deploymentName(backend), Namespace: backend.Namespace}, deploy); err != nil {
		return client.IgnoreNotFound(err)
	}

	backend.Status.ReadyReplicas = deploy.Status.ReadyReplicas
	backend.Status.QueueURL = queueURL

	desired := int32(1)
	if backend.Spec.Replicas != nil {
		desired = *backend.Spec.Replicas
	}

	available := metav1.ConditionFalse
	reason := "DeploymentUnavailable"
	message := fmt.Sprintf("%d/%d replicas ready", deploy.Status.ReadyReplicas, desired)
	if deploy.Status.ReadyReplicas >= desired {
		available = metav1.ConditionTrue
		reason = "DeploymentAvailable"
	}

	r.setCondition(backend, metav1.Condition{
		Type:               "Available",
		Status:             available,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: backend.Generation,
		LastTransitionTime: metav1.Now(),
	})

	// SQSReady condition — only set when a queue is requested
	if backend.Spec.Queue != nil {
		sqsStatus := metav1.ConditionFalse
		sqsReason := "QueueProvisioning"
		sqsMessage := "waiting for SQS queue to become ready"
		if queueURL != "" {
			sqsStatus = metav1.ConditionTrue
			sqsReason = "QueueReady"
			sqsMessage = fmt.Sprintf("queue URL: %s", queueURL)
		}
		r.setCondition(backend, metav1.Condition{
			Type:               "SQSReady",
			Status:             sqsStatus,
			Reason:             sqsReason,
			Message:            sqsMessage,
			ObservedGeneration: backend.Generation,
			LastTransitionTime: metav1.Now(),
		})
	} else {
		backend.Status.Conditions = removeCondition(backend.Status.Conditions, "SQSReady")
	}

	if backend.Spec.Database == nil {
		backend.Status.Conditions = removeCondition(backend.Status.Conditions, "RDSReady")
	}

	return r.patchStatusIfChanged(ctx, statusBase, backend)
}

func (r *BackendReconciler) setRDSReadyCondition(backend *appsv1alpha1.Backend, status metav1.ConditionStatus, reason, message string) {
	r.setCondition(backend, metav1.Condition{
		Type:               "RDSReady",
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: backend.Generation,
		LastTransitionTime: metav1.Now(),
	})
}

func (r *BackendReconciler) patchStatusIfChanged(ctx context.Context, statusBase, backend *appsv1alpha1.Backend) error {
	if equality.Semantic.DeepEqual(statusBase.Status, backend.Status) {
		return nil
	}
	patch := client.MergeFrom(statusBase)
	return client.IgnoreNotFound(r.Status().Patch(ctx, backend, patch))
}

func (r *BackendReconciler) setCondition(backend *appsv1alpha1.Backend, cond metav1.Condition) {
	existing := findCondition(backend.Status.Conditions, cond.Type)
	if existing != nil {
		if existing.Status != cond.Status {
			existing.LastTransitionTime = metav1.Now()
			if cond.Type == "Available" || cond.Type == "RDSReady" {
				if cond.Status == metav1.ConditionFalse {
					r.Recorder.Event(backend, corev1.EventTypeWarning, cond.Reason, cond.Message)
				} else {
					r.Recorder.Event(backend, corev1.EventTypeNormal, cond.Reason, cond.Message)
				}
			}
		}
		existing.Status = cond.Status
		existing.Reason = cond.Reason
		existing.Message = cond.Message
		existing.ObservedGeneration = cond.ObservedGeneration
	} else {
		backend.Status.Conditions = append(backend.Status.Conditions, cond)
	}
}

func (r *BackendReconciler) buildDeployment(backend *appsv1alpha1.Backend, queueURL string) *appsv1.Deployment {
	labels := map[string]string{
		"app.kubernetes.io/name":       "backend",
		"app.kubernetes.io/instance":   backend.Name,
		"app.kubernetes.io/managed-by": "taskapp-operator",
	}

	replicas := int32(1)
	if backend.Spec.Replicas != nil {
		replicas = *backend.Spec.Replicas
	}

	probeHandler := corev1.ProbeHandler{
		HTTPGet: &corev1.HTTPGetAction{
			Path: "/ready",
			Port: intstr.FromInt32(8080),
		},
	}

	envVars := []corev1.EnvVar{
		{Name: "PORT", Value: "8080"},
		{Name: "DB_HOST", Value: "taskapp-database"},
		{Name: "DB_PORT", Value: "5432"},
		{Name: "DB_USER", Value: "taskuser"},
		{Name: "DB_NAME", Value: "taskdb"},
		{
			Name: "DB_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: backend.Spec.DBSecret},
					Key:                  "POSTGRES_PASSWORD",
				},
			},
		},
	}
	if queueURL != "" {
		envVars = append(envVars, corev1.EnvVar{Name: backend.Spec.Queue.URLEnvVar, Value: queueURL})
	}
	if backend.Spec.Queue != nil {
		credSecretName := backend.Name + "-aws-credentials"
		envVars = append(envVars,
			corev1.EnvVar{
				Name: "AWS_ACCESS_KEY_ID",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: credSecretName},
						Key:                  "AWS_ACCESS_KEY_ID",
					},
				},
			},
			corev1.EnvVar{
				Name: "AWS_SECRET_ACCESS_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: credSecretName},
						Key:                  "AWS_SECRET_ACCESS_KEY",
					},
				},
			},
		)
	}
	envVars = append(envVars, backend.Spec.ExtraEnv...)

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName(backend),
			Namespace: backend.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "backend",
							Image: fmt.Sprintf("%s:%s", backend.Spec.Image, backend.Spec.Tag),
							Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
							Env:   envVars,
							ReadinessProbe: &corev1.Probe{
								ProbeHandler:        probeHandler,
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
								FailureThreshold:    3,
							},
						},
					},
				},
			},
		},
	}
}

func (r *BackendReconciler) buildService(backend *appsv1alpha1.Backend) *corev1.Service {
	labels := map[string]string{
		"app.kubernetes.io/name":       "backend",
		"app.kubernetes.io/instance":   backend.Name,
		"app.kubernetes.io/managed-by": "taskapp-operator",
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName(backend),
			Namespace: backend.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Port:       80,
					TargetPort: intstr.FromInt32(8080),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Type: corev1.ServiceTypeClusterIP,
		},
	}
}

func deploymentName(b *appsv1alpha1.Backend) string  { return b.Name + "-backend" }
func serviceName(b *appsv1alpha1.Backend) string     { return b.Name + "-backend" }
func rdsInstanceName(b *appsv1alpha1.Backend) string { return b.Name }

func rdsParameters(db *appsv1alpha1.DatabaseSpec) map[string]any {
	parameters := map[string]any{
		"dbName": db.DBName,
	}
	switch db.Size {
	case appsv1alpha1.DatabaseSizeMedium:
		parameters["instanceClass"] = "db.t3.small"
		parameters["storageGB"] = int64(50)
	case appsv1alpha1.DatabaseSizeLarge:
		parameters["instanceClass"] = "db.t3.medium"
		parameters["storageGB"] = int64(100)
	}
	return parameters
}

func sqsQueueName(b *appsv1alpha1.Backend) string {
	name := b.Namespace + "-" + b.Name + "-queue"
	if b.Spec.Queue != nil && b.Spec.Queue.Type == appsv1alpha1.QueueTypeFifo {
		name += ".fifo"
	}
	return name
}

func sqsDLQName(b *appsv1alpha1.Backend) string {
	name := b.Namespace + "-" + b.Name + "-queue-dlq"
	if b.Spec.Queue != nil && b.Spec.Queue.Type == appsv1alpha1.QueueTypeFifo {
		name += ".fifo"
	}
	return name
}

func sqsRegion() string {
	if r := os.Getenv("DEFAULT_QUEUE_REGION"); r != "" {
		return r
	}
	return "eu-west-1"
}

func clusterSecretStoreName() string {
	if v := os.Getenv("CLUSTER_SECRET_STORE"); v != "" {
		return v
	}
	return "aws-secrets-manager"
}

func credentialsSecretPath() string {
	if v := os.Getenv("CREDENTIALS_SECRET_PATH"); v != "" {
		return v
	}
	return "taskapp/prod/backend-credentials"
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

func removeCondition(conditions []metav1.Condition, condType string) []metav1.Condition {
	result := make([]metav1.Condition, 0, len(conditions))
	for _, c := range conditions {
		if c.Type != condType {
			result = append(result, c)
		}
	}
	return result
}

func isReady(obj *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, c := range conditions {
		cond, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if cond["type"] == "Ready" && cond["status"] == "True" {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *BackendReconciler) SetupWithManager(mgr ctrl.Manager) error {
	queueType := &unstructured.Unstructured{}
	queueType.SetGroupVersionKind(sqsQueueGVK)

	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.Backend{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Watches(
			queueType,
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				labels := obj.GetLabels()
				ns, nsOk := labels["apps.taskapp.io/owned-by-namespace"]
				name, nameOk := labels["apps.taskapp.io/owned-by-backend"]
				if !nsOk || !nameOk {
					return nil
				}
				return []reconcile.Request{
					{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}},
				}
			}),
		).
		Named("backend").
		Complete(r)
}
