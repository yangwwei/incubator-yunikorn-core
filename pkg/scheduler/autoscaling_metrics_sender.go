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
	"net/http"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"go.uber.org/zap"
)

type proposalSender interface {
	send(metrics *si.AutoScalingMetrics) (map[string]int32, error)
}

type dummySender struct {}

func (d dummySender) send(metrics *si.AutoScalingMetrics) error {
	log.Logger().Debug("sending auto-scale metrics",
		zap.String("metrics", fmt.Sprintf("%v", metrics)))
	// assume success
	return nil
}

type httpSender struct {
	endpoint string
}

func (h httpSender) send(metrics *si.AutoScalingMetrics) error {
	log.Logger().Info("sending auto-scale metrics",
		zap.String("metrics", fmt.Sprintf("%v", metrics)))

	message, err := json.Marshal(metrics)
	if err != nil {
		return err
	}

	// send request to mk
	log.Logger().Info("http message",
		zap.Any("json", fmt.Sprintf("%v", string(message))))
	resp, sendErr := http.Post(h.endpoint, "application/json", bytes.NewBuffer(message))
	if sendErr != nil {
		return sendErr
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Logger().Warn("close http connection failed",
				zap.Error(err))
		}
	}()

	return nil
}