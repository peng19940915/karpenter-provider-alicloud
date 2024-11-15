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

package controllers

import (
	"context"

	"github.com/awslabs/operatorpkg/controller"
	"k8s.io/client-go/rest"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"

	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/cache"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/controllers/interruption"
	nodeclaimgarbagecollection "github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/controllers/nodeclaim/garbagecollection"
	nodeclaimtagging "github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/controllers/nodeclaim/tagging"
	nodeclaimunregisteredtaint "github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/controllers/nodeclaim/unregisteredtaint"
	nodeclasshash "github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/controllers/nodeclass/hash"
	nodeclaasstatus "github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/controllers/nodeclass/status"
	nodeclasstermination "github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/controllers/nodeclass/termination"
	providersinstancetype "github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/controllers/providers/instancetype"
	controllerspricing "github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/controllers/providers/pricing"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/controllers/telemetry"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/operator/options"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/providers/imagefamily"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/providers/instance"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/providers/instancetype"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/providers/pricing"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/providers/securitygroup"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/providers/vswitch"
)

func NewControllers(ctx context.Context, mgr manager.Manager, clk clock.Clock, restConfig *rest.Config,
	kubeClient client.Client, recorder events.Recorder,
	unavailableOfferings *cache.UnavailableOfferings,
	cloudProvider cloudprovider.CloudProvider,
	instanceProvider instance.Provider, instanceTypeProvider instancetype.Provider,
	pricingProvider pricing.Provider,
	vSwitchProvider vswitch.Provider, securitygroupProvider securitygroup.Provider,
	imageProvider imagefamily.Provider) []controller.Controller {

	controllers := []controller.Controller{
		nodeclasshash.NewController(kubeClient),
		nodeclaasstatus.NewController(kubeClient, vSwitchProvider, securitygroupProvider, imageProvider),
		nodeclasstermination.NewController(kubeClient, recorder),
		controllerspricing.NewController(pricingProvider),
		nodeclaimgarbagecollection.NewController(kubeClient, cloudProvider),
		nodeclaimunregisteredtaint.NewController(kubeClient),
		nodeclaimtagging.NewController(kubeClient, instanceProvider),
		providersinstancetype.NewController(instanceTypeProvider),
	}

	if options.FromContext(ctx).Interruption {
		controllers = append(controllers, interruption.NewController(kubeClient, recorder, unavailableOfferings))
	}

	if options.FromContext(ctx).TelemetryShare {
		controllers = append(controllers, telemetry.NewController(kubeClient, metricsclientset.NewForConfigOrDie(restConfig)))
	}
	return controllers
}
