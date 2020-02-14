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
	"encoding/json"
	"fmt"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"go.uber.org/zap"
)

type proposalSender interface {
	send(proposal *autoScalingProposal) (map[string]int32, error)
}

type dummySender struct {}

func (d dummySender) send(proposal *autoScalingProposal) (map[string]int32, error) {
	log.Logger().Info("sending proposal",
		zap.String("proposal", fmt.Sprintf("%v", proposal.desire)))
	// assume success
	return proposal.desire, nil
}

type NodeRequests []NodeRequest

type NodeRequest struct {
	TemplateName string
	NodeNumber   int32
	NodesPending []string
}

type httpSender struct {}

func (h httpSender) send(proposal *autoScalingProposal) (map[string]int32, error) {
	log.Logger().Info("sending proposal",
		zap.String("proposal", fmt.Sprintf("%v", proposal.desire)))

	// [{"TemplateName":"test","NodeNumber":2,"NodesPending":null}]'

	var requests NodeRequests
	if proposal != nil {
		for k,v := range proposal.desire {
			requests = append(requests,
				NodeRequest{
					TemplateName: k,
					NodeNumber: v,
					NodesPending: nil,
				})
		}
	}

	message, err := json.Marshal(requests)
	if err != nil {
		return nil, err
	}

	log.Logger().Info("http message",
		zap.Any("json", fmt.Sprintf("%v", string(message))))

	return proposal.desire, nil
}