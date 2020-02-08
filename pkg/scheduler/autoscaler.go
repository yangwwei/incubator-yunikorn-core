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
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"go.uber.org/zap"
)

func (m *Scheduler) SingleComputeScale() *resources.Resource {
	return m.computeScale()
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

	log.Logger().Info("auto-scaling-adviser",
		zap.String("desiredTotalResource", clusterTotal.String()))

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
