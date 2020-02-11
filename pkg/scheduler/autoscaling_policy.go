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
)

type scalingResultType int8

const (
	up scalingResultType = iota
	down
	noop
)

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
	result := make(map[string]int32)
	for resourceName, value := range desire.Resources {
		nodeResource := template.instanceResource.Resources[resourceName]
		desireNum := int32(math.Ceil(float64(value) / float64(nodeResource)))
		if desireNum > num {
			num = desireNum
		}
	}

	if num > 0 {
		result[template.name] = num
	}

	return &autoScalingProposal{
		scalingResultType: up,
		numPerType:        result,
	}, nil
}
