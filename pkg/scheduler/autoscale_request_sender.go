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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

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

type httpSender struct {
	endpoint string
}

func (h httpSender) send(proposal *autoScalingProposal) (map[string]int32, error) {
	log.Logger().Info("sending proposal",
		zap.String("proposal", fmt.Sprintf("%v", proposal.desire)))

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

	// send request to mk
	log.Logger().Info("http message",
		zap.Any("json", fmt.Sprintf("%v", string(message))))
	resp, sendErr := http.Post(h.endpoint, "application/json", bytes.NewBuffer(message))
	if sendErr != nil {
		return nil, sendErr
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Logger().Warn("close http connection failed",
				zap.Error(err))
		}
	}()


	var response NodeRequests
	jsonBlob, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}

	err = json.Unmarshal(jsonBlob, &response)
	log.Logger().Info("http response",
		zap.Any("json", fmt.Sprintf("%v", string(jsonBlob))))
	result := make(map[string]int32)
	for _, item := range response {
		result[item.TemplateName] = item.NodeNumber
	}

	return result, nil
}