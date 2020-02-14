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
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

var internalCache *autoScalingCache

// caches how many nodes in desire
type autoScalingCache struct {
	nodes map[string]int32
	lastUpdate time.Time
}

func process(sender proposalSender, proposal *autoScalingProposal) error {
	if internalCache == nil {
		internalCache = &autoScalingCache{
			nodes:      make(map[string]int32),
			lastUpdate: time.Now(),
		}
	}

	newProposal := make(map[string]int32)
	for template, proposeNum := range proposal.desire {
		if currentNum, ok := internalCache.nodes[template]; ok {
			if currentNum < proposeNum {
				// 1) request: we need more nodes, we need to increase the desire
				// here we calculate the delta and prepare to send the scaling request
				// 2) cache: we will need to wait until we get response to update cache
				newProposal[template] = proposeNum - currentNum
			} else if currentNum > proposeNum {
				// 1) request: not ask for more, we don't need to send any
				// new proposal at this point
				// 2) cache: desired num of nodes is less than current ask,
				// some outstanding requests have been satisfied, or removed,
				// that might because some nodes have been created,
				// we can shrink the desire in cache now.
				internalCache.nodes[template] = proposeNum
			}
		} else {
			// new desire
			newProposal[template] = proposeNum
		}
	}

	// we got delta to fire
	if len(newProposal) > 0 {
		if ackNodes, err := sender.send(&autoScalingProposal{desire:newProposal}); err == nil {
			for template, ackNum := range ackNodes {
				currentNum := internalCache.nodes[template]
				internalCache.nodes[template] = currentNum + ackNum
			}
		} else {
			return err
		}
	} else {
		log.Logger().Info("No new scaling up request needed")
	}

	return nil
}
