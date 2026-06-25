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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// QueueType defines the SQS queue type
// +kubebuilder:validation:Enum=standard;fifo
type QueueType string

const (
	QueueTypeStandard QueueType = "standard"
	QueueTypeFifo     QueueType = "fifo"
)

// QueueSpec defines the optional SQS queue desired by the Backend
type QueueSpec struct {
	// type is the SQS queue type: standard or fifo
	// +kubebuilder:default=standard
	// +optional
	Type QueueType `json:"type,omitempty"`

	// deadLetter enables a dead-letter queue for this queue
	// +kubebuilder:default=false
	// +optional
	DeadLetter bool `json:"deadLetter,omitempty"`

	// urlEnvVar is the name of the environment variable injected into the container with the queue URL.
	// +kubebuilder:default=SQS_QUEUE_URL
	// +optional
	URLEnvVar string `json:"urlEnvVar,omitempty"`
}

// DatabaseSize defines the provisioned RDS instance size preset.
// +kubebuilder:validation:Enum=small;medium;large
type DatabaseSize string

const (
	DatabaseSizeSmall  DatabaseSize = "small"
	DatabaseSizeMedium DatabaseSize = "medium"
	DatabaseSizeLarge  DatabaseSize = "large"
)

// SchemaSpec defines an optional inline schema for a provisioned database.
type SchemaSpec struct {
	// sql is the inline SQL schema applied by the Atlas integration.
	// +optional
	SQL string `json:"sql,omitempty"`
}

// DatabaseSpec defines the RDS database desired by the Backend
type DatabaseSpec struct {
	// dbName is the name of the database to create
	// +kubebuilder:validation:Required
	DBName string `json:"dbName"`

	// size is the RDS instance size preset.
	// +kubebuilder:default=small
	// +optional
	Size DatabaseSize `json:"size,omitempty"`

	// schema is the optional inline schema to apply after the database is ready.
	// +optional
	Schema *SchemaSpec `json:"schema,omitempty"`
}

// BackendSpec defines the desired state of Backend
type BackendSpec struct {
	// image is the container image repository (e.g. boicotaz/taskapp-backend)
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// tag is the container image tag to deploy
	// +kubebuilder:validation:Required
	Tag string `json:"tag"`

	// replicas is the desired number of pod replicas
	// +kubebuilder:default=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// dbSecret is the name of the Secret containing the DB_PASSWORD key.
	// Used as the local postgres fallback when database is not set.
	// +optional
	DBSecret string `json:"dbSecret,omitempty"`

	// database optionally provisions an RDS PostgreSQL instance via Crossplane.
	// When set, DB env vars in the deployment are sourced from the resulting
	// connection secret instead of dbSecret.
	// +optional
	Database *DatabaseSpec `json:"database,omitempty"`

	// queue optionally provisions an SQS queue for this Backend.
	// Omit the field entirely if no queue is needed.
	// +optional
	Queue *QueueSpec `json:"queue,omitempty"`

	// extraEnv is a list of additional environment variables appended to the
	// backend container after the built-in vars.
	// +optional
	ExtraEnv []corev1.EnvVar `json:"extraEnv,omitempty"`
}

// BackendStatus defines the observed state of Backend.
type BackendStatus struct {
	// readyReplicas is the number of pods currently ready
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// queueURL is the URL of the provisioned SQS queue, populated once ready.
	// +optional
	QueueURL string `json:"queueURL,omitempty"`

	// conditions represent the current state of the Backend resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image`
// +kubebuilder:printcolumn:name="Tag",type=string,JSONPath=`.spec.tag`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="RDSReady",type=string,JSONPath=`.status.conditions[?(@.type=="RDSReady")].status`,priority=1
// +kubebuilder:printcolumn:name="QueueURL",type=string,JSONPath=`.status.queueURL`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Backend is the Schema for the backends API
type Backend struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Backend
	// +required
	Spec BackendSpec `json:"spec"`

	// status defines the observed state of Backend
	// +optional
	Status BackendStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BackendList contains a list of Backend
type BackendList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Backend `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Backend{}, &BackendList{})
}
