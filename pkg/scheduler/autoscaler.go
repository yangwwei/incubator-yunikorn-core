/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package scheduler

import (
	"fmt"
	"math"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"go.uber.org/zap"
)

type instanceTemplate struct {
	name             string
	instanceResource *resources.Resource
}

type autoScalingProposal struct {
	desire map[string]int32
}

func (m *Scheduler) SingleStepComputeScale() error {
	delta := m.computeScale()
	log.Logger().Info("auto-scaling-adviser",
		zap.String("desiredTotalResource", delta.String()))

	availableTemplates := []*instanceTemplate{
		{
			name: "m5.large",
			instanceResource: resources.NewResourceFromMap(
				map[string]resources.Quantity{
					resources.VCORE:  2000,
					resources.MEMORY: 8000}),
		},
	}

	proposal, err := m.generateAutoScalingProposal(delta, availableTemplates)
	if err != nil {
		log.Logger().Info("failed to general auto scaling proposal",
			zap.Error(err))
		return err
	}

	for _, template := range availableTemplates {
		log.Logger().Info("computing desired # of nodes",
			zap.String("desiredResource", delta.String()),
			zap.String("templateName", template.name),
			zap.String("templateResource", template.instanceResource.String()),
			zap.Int8("desiredNodes", int8(proposal.desire[template.name])),
			zap.Int32("scale#", proposal.desire[template.name]))
	}

	if processErr := process(httpSender{"http://localhost:1311/"}, proposal); processErr != nil {
		log.Logger().Error("failed to update auto-scaling cache",
			zap.Error(processErr))
		return processErr
	}

	log.Logger().Info("auto-scaling-adviser",
		zap.String("state", "compute-finished"),
		zap.String("cache(desiredNodes)", fmt.Sprintf("%v", internalCache.nodes)))

	return nil
}

func (m *Scheduler) generateAutoScalingProposal(deltaResource *resources.Resource,
	availableInstanceTypes []*instanceTemplate) (result *autoScalingProposal, err error) {
	// TODO figure out how to resolve bin-packing problem
	// for now, only assume there is only 1 type
	if availableInstanceTypes == nil {
		return &autoScalingProposal{}, nil
	}

	if len(availableInstanceTypes) > 1 {
		return nil, fmt.Errorf("multi-instance-type autoscaling decision not supported yet")
	}

	return computeDesireNumOfInstances(deltaResource, availableInstanceTypes[0])
}

func (m *Scheduler) computeScale() *resources.Resource {
	clusterTotal := resources.NewResource()

	for partition, partitionContext := range m.clusterSchedulingContext.getPartitionMapClone() {
		totalPartitionResource := m.clusterInfo.GetTotalPartitionResource(partition)
		if totalPartitionResource == nil {
			continue
		}

		totalDesire := resources.NewResource()
		computeQueueDesire(totalPartitionResource,
			partitionContext.Root, nil, nil, totalDesire)

		clusterTotal.AddTo(totalDesire)
	}

	return clusterTotal
}

func computeQueueDesire(totalPartitionResource *resources.Resource,
	queue *SchedulingQueue,
	parentHeadroom *resources.Resource,
	parentQueueMaxLimit *resources.Resource,
	totalDesire *resources.Resource) {

	queueMaxLimit := getQueueMaxLimit(totalPartitionResource, queue, parentQueueMaxLimit)
	newHeadroom := getHeadroomOfQueue(parentHeadroom, queueMaxLimit, queue,
		&preemptionParameters{crossQueuePreemption: false})

	if queue.isLeafQueue() {
		desire := resources.Sub(queue.pendingResource, newHeadroom)
		if resources.StrictlyGreaterThan(desire, resources.Zero) {
			totalDesire.AddTo(desire)
		}
	} else {
		for _, child := range queue.childrenQueues {
			computeQueueDesire(totalPartitionResource, child, newHeadroom, queueMaxLimit, totalDesire)
		}
	}
}

func computeDesireNumOfInstances(desire *resources.Resource,
	template *instanceTemplate) (*autoScalingProposal, error) {
	// sanity checks
	// make sure all resource types can be found in this template
	for resourceName := range desire.Resources {
		if _, ok := template.instanceResource.Resources[resourceName]; !ok {
			return nil, fmt.Errorf("resource type %s is not found in instance type %s",
				resourceName, template.name)
		}
	}

	// how many more this kind of instances in minimal is needed to fit this resource?
	num := int32(0)
	asProposal := &autoScalingProposal{
		desire: make(map[string]int32),
	}
	for resourceName, value := range desire.Resources {
		nodeResource := template.instanceResource.Resources[resourceName]
		desireNum := int32(math.Ceil(float64(value) / float64(nodeResource)))
		if desireNum > num {
			num = desireNum
		}
	}

	asProposal.desire[template.name] = num
	return asProposal, nil
}
