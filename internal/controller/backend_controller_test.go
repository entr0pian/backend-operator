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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	appsv1alpha1 "github.com/entr0pian/taskapp-operator/api/v1alpha1"
)

const (
	timeout  = 10 * time.Second
	interval = 250 * time.Millisecond
)

func newBackend(name, namespace string) *appsv1alpha1.Backend {
	replicas := int32(1)
	return &appsv1alpha1.Backend{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1alpha1.BackendSpec{
			Image:    "boicotaz/taskapp-backend",
			Tag:      "abc123",
			Replicas: &replicas,
			DBSecret: "test-db-secret",
		},
	}
}

var _ = Describe("Backend controller", func() {
	var ns string

	BeforeEach(func() {
		// Each test gets its own namespace to avoid cross-test interference
		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "test-"},
		}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name
	})

	Context("when a Backend CR is created", func() {
		It("creates a Deployment and Service", func() {
			backend := newBackend("test-backend", ns)
			Expect(k8sClient.Create(ctx, backend)).To(Succeed())

			deployKey := types.NamespacedName{Name: "test-backend-backend", Namespace: ns}
			svcKey := types.NamespacedName{Name: "test-backend-backend", Namespace: ns}

			By("checking the Deployment is created with the correct image")
			deploy := &appsv1.Deployment{}
			Eventually(func() error {
				return k8sClient.Get(ctx, deployKey, deploy)
			}, timeout, interval).Should(Succeed())

			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("boicotaz/taskapp-backend:abc123"))
			Expect(*deploy.Spec.Replicas).To(Equal(int32(1)))

			By("checking the Service is created")
			svc := &corev1.Service{}
			Eventually(func() error {
				return k8sClient.Get(ctx, svcKey, svc)
			}, timeout, interval).Should(Succeed())

			Expect(svc.Spec.Ports[0].Port).To(Equal(int32(80)))
		})
	})

	Context("when a Backend CR requests a database", func() {
		It("creates an RDSInstance claim and waits for it to become ready", func() {
			backend := newBackend("rds-backend", ns)
			backend.Spec.Database = &appsv1alpha1.DatabaseSpec{
				DBName: "taskapp",
				Size:   appsv1alpha1.DatabaseSizeMedium,
			}
			Expect(k8sClient.Create(ctx, backend)).To(Succeed())

			rdsKey := types.NamespacedName{Name: "rds-backend", Namespace: ns}
			deployKey := types.NamespacedName{Name: "rds-backend-backend", Namespace: ns}

			By("checking the RDSInstance claim is created with mapped parameters")
			rds := &unstructured.Unstructured{}
			rds.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "database.taskapp.io",
				Version: "v1alpha1",
				Kind:    "RDSInstance",
			})
			Eventually(func() error {
				return k8sClient.Get(ctx, rdsKey, rds)
			}, timeout, interval).Should(Succeed())

			dbName, _, _ := unstructured.NestedString(rds.Object, "spec", "parameters", "dbName")
			instanceClass, _, _ := unstructured.NestedString(rds.Object, "spec", "parameters", "instanceClass")
			storageGB, _, _ := unstructured.NestedInt64(rds.Object, "spec", "parameters", "storageGB")
			Expect(dbName).To(Equal("taskapp"))
			Expect(instanceClass).To(Equal("db.t3.small"))
			Expect(storageGB).To(Equal(int64(50)))

			By("checking deployment waits while RDS is not ready")
			deploy := &appsv1.Deployment{}
			Consistently(func() error {
				return k8sClient.Get(ctx, deployKey, deploy)
			}, 500*time.Millisecond, interval).ShouldNot(Succeed())

			By("marking the RDSInstance claim ready")
			Expect(unstructured.SetNestedSlice(rds.Object, []any{
				map[string]any{
					"type":   "Ready",
					"status": "True",
				},
			}, "status", "conditions")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, rds)).To(Succeed())

			By("triggering a reconcile after RDS readiness changes")
			updatedBackend := &appsv1alpha1.Backend{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rds-backend", Namespace: ns}, updatedBackend)).To(Succeed())
			if updatedBackend.Annotations == nil {
				updatedBackend.Annotations = map[string]string{}
			}
			updatedBackend.Annotations["test.taskapp.io/rds-ready"] = "true"
			Expect(k8sClient.Update(ctx, updatedBackend)).To(Succeed())

			By("checking the Deployment is created after RDS is ready")
			Eventually(func() error {
				return k8sClient.Get(ctx, deployKey, deploy)
			}, timeout, interval).Should(Succeed())

			By("checking the Backend RDSReady condition is true")
			Eventually(func() metav1.ConditionStatus {
				updated := &appsv1alpha1.Backend{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "rds-backend", Namespace: ns}, updated); err != nil {
					return metav1.ConditionUnknown
				}
				for _, cond := range updated.Status.Conditions {
					if cond.Type == "RDSReady" {
						return cond.Status
					}
				}
				return metav1.ConditionUnknown
			}, timeout, interval).Should(Equal(metav1.ConditionTrue))
		})
	})

	Context("when the image tag is updated", func() {
		It("updates the Deployment image", func() {
			backend := newBackend("update-backend", ns)
			Expect(k8sClient.Create(ctx, backend)).To(Succeed())

			deployKey := types.NamespacedName{Name: "update-backend-backend", Namespace: ns}

			By("waiting for initial Deployment")
			deploy := &appsv1.Deployment{}
			Eventually(func() error {
				return k8sClient.Get(ctx, deployKey, deploy)
			}, timeout, interval).Should(Succeed())

			By("patching the tag to a new value")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "update-backend", Namespace: ns}, backend)).To(Succeed())
			backend.Spec.Tag = "newsha456"
			Expect(k8sClient.Update(ctx, backend)).To(Succeed())

			By("checking the Deployment image is updated")
			Eventually(func() string {
				if err := k8sClient.Get(ctx, deployKey, deploy); err != nil {
					return ""
				}
				return deploy.Spec.Template.Spec.Containers[0].Image
			}, timeout, interval).Should(Equal("boicotaz/taskapp-backend:newsha456"))
		})
	})

	Context("when a Backend CR is deleted", func() {
		It("cascades deletion to the Deployment and Service", func() {
			backend := newBackend("delete-backend", ns)
			Expect(k8sClient.Create(ctx, backend)).To(Succeed())

			deployKey := types.NamespacedName{Name: "delete-backend-backend", Namespace: ns}

			By("waiting for Deployment to exist")
			deploy := &appsv1.Deployment{}
			Eventually(func() error {
				return k8sClient.Get(ctx, deployKey, deploy)
			}, timeout, interval).Should(Succeed())

			By("deleting the Backend CR")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "delete-backend", Namespace: ns}, backend)).To(Succeed())
			Expect(k8sClient.Delete(ctx, backend)).To(Succeed())

			By("checking the Deployment is garbage collected")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, deployKey, deploy)
				return err != nil
			}, timeout, interval).Should(BeTrue())
		})
	})
})
