package unregisteredtaint

/*
Copyright 2024 The CloudPilot AI Authors.

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
import (
	"context"
	"time"

	"github.com/awslabs/operatorpkg/singleton"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

type Controller struct {
	kubeClient client.Client
}

func NewController(kubeClient client.Client) *Controller {
	return &Controller{
		kubeClient: kubeClient,
	}
}

func (c *Controller) Reconcile(ctx context.Context) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	// get all nodes
	nodeList := &corev1.NodeList{}
	if err := c.kubeClient.List(ctx, nodeList, client.HasLabels{v1.NodeRegisteredLabelKey}); err != nil {
		return reconcile.Result{}, err
	}
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if !hasUnregisteredTaint(node) || !isNodeReady(node) {
			continue
		}
		// remove the unregistered taint
		newTaints := make([]corev1.Taint, 0)
		for _, taint := range node.Spec.Taints {
			if taint.Key != v1.UnregisteredTaintKey {
				newTaints = append(newTaints, taint)
			}
		}
		nodeCopy := node.DeepCopy()
		nodeCopy.Spec.Taints = newTaints
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			return c.kubeClient.Patch(ctx, nodeCopy, client.MergeFrom(node))
		}); err != nil {
			logger.Error(err, "failed to remove unregistered taint", "node", node.Name)
			continue
		}
		logger.Info("removed unregistered taint from node", "node", node.Name)
	}

	// check again every minute
	return reconcile.Result{RequeueAfter: time.Minute}, nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("node.unregisteredtaint").
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

func hasUnregisteredTaint(node *corev1.Node) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == v1.UnregisteredTaintKey {
			return true
		}
	}
	return false
}

func isNodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue &&
				time.Since(condition.LastTransitionTime.Time) > time.Minute
		}
	}
	return false
}
