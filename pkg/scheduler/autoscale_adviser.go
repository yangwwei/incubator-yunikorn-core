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
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"go.uber.org/zap"
)

var autoScaleLastRunTime time.Time
var metricsSender *httpSender
var once sync.Once

func (s *Scheduler) triggerAutoScaleIfNecessary() {
	once.Do(func() {
		metricsSender = &httpSender{
			endpoint:"http://localhost:1311",
		}
	})

	if autoScaleLastRunTime.IsZero() {
		autoScaleLastRunTime = time.Now()
	}

	if time.Since(autoScaleLastRunTime) > 3 * time.Second {
		log.Logger().Info("running auto-scale adviser")
		outstandingRequests := make([]*si.OutstandingResourceRequest, 0)
		for _, p := range s.clusterSchedulingContext.partitions {
			for _, app := range p.applications {
				for _, req := range app.outstandingRequests {
					log.Logger().Debug("outstanding request",
						zap.String("RequestID", req.AskProto.AllocationKey),
						zap.String("Resource", req.AskProto.ResourceAsk.String()),
						zap.Any("Tags", req.AskProto.Tags))
					outstandingRequests = append(outstandingRequests, &si.OutstandingResourceRequest{
						RequestID:            req.AskProto.AllocationKey,
						Resource:             req.AskProto.ResourceAsk,
						Tags:                 req.AskProto.Tags,
					})
				}
			}
		}

		if len(outstandingRequests) > 0 {
			log.Logger().Info("outstanding requests",
				zap.Int("size", len(outstandingRequests)))
			if err := metricsSender.send(&si.AutoScalingMetrics{
				OutstandingRequests:  outstandingRequests,
			}); err != nil {
				log.Logger().Error("failed to send metrics to auto-scaler",
					zap.Error(err))
			}
		}
	}
}