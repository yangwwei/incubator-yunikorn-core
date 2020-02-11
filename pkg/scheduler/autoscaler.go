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
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"go.uber.org/zap"
)

var autoScalingCache *cachedResult

type cachedResult struct {
	result     *autoScalingProposal
	lastUpdate time.Time
	synced     bool
}

type instanceTemplate struct {
	name             string
	instanceResource *resources.Resource
}

type autoScalingProposal struct {
	scalingResultType
	numPerType map[string]int32
}

func (a *cachedResult) absorb(pr *autoScalingProposal) {
	mergedNum := make(map[string]int32)
	for temp, num := range a.result.numPerType {
		if newNum, ok := pr.numPerType[temp]; ok {
			if newNum > num {
				// existing num not enough, extending
				mergedNum[temp] = newNum - num
				a.synced = false
			} else {
				mergedNum[temp] = newNum

			}
		}
	}
}

func (m *Scheduler) SingleStepComputeScale() (*autoScalingProposal, error) {
	delta := m.computeScale()
	log.Logger().Info("auto-scaling-adviser",
		zap.String("desiredTotalResource", delta.String()))

	availableTemplates := []*instanceTemplate{
		{
			name: "m5.large",
			instanceResource: resources.NewResourceFromMap(
				map[string]resources.Quantity{
					resources.VCORE:  2000,
					resources.MEMORY: 8000000}),
		},
	}

	result, err := m.computeDesiredDelta(delta, availableTemplates)
	if err == nil {
		// logging
		for _, template := range availableTemplates {
			log.Logger().Info("computing desired # of nodes",
				zap.String("desired resource", delta.String()),
				zap.String("templateName", template.name),
				zap.String("templateResource", template.instanceResource.String()),
				zap.Int8("scaleUpOrDown", int8(result.scalingResultType)),
				zap.Int32("scale#", result.numPerType[template.name]))
		}

		// update cache
		if err := updateCache(result); err != nil {
			log.Logger().Error("unable to update cache", zap.Error(err))
		} else {
			// grab whatever in cache and send to external service
			if err := sendRequest(); err != nil {
				log.Logger().Error("unable to update cache", zap.Error(err))
			}
		}
	}

	return result, err
}

func updateCache(update *autoScalingProposal) error {
	// is this first update
	if autoScalingCache == nil {
		autoScalingCache = &cachedResult{
			result:     update,
			lastUpdate: time.Now(),
			synced:     false,
		}
	}

	// if there was a result, merge it
	autoScalingCache.absorb(update)

	return nil
}

func sendRequest() error {

	return nil
}

func (m *Scheduler) computeDesiredDelta(deltaResource *resources.Resource,
	availableInstanceTypes []*instanceTemplate) (result *autoScalingProposal, err error) {
	// TODO figure out how to resolve bin-packing problem
	// for now, only assume there is only 1 type
	if availableInstanceTypes == nil {
		// skipped
		return &autoScalingProposal{scalingResultType: noop}, nil
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
