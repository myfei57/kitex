/*
 * Copyright 2026 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package retry

import (
	"context"

	"github.com/cloudwego/kitex/pkg/retry/attempttrace"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

// traceCollector returns the per-call attempt trace collector, or nil when
// attempt tracing is not enabled. The nil-safe collector/handle API keeps
// the disabled path allocation free.
func traceCollector(ctx context.Context) *attempttrace.Collector {
	return attempttrace.CollectorFromContext(ctx)
}

// traceFinishInfo builds the plain-value terminal description of an attempt
// from the REAL rpcCall result only. It never re-invokes a selector or a
// result-retry callback, never resends an RPC, never retains pooled
// RPCInfo/EndpointInfo/Instance pointers and never touches request or
// response business values.
func traceFinishInfo(ri rpcinfo.RPCInfo, err error, abandoned, resultAdopted bool) attempttrace.FinishInfo {
	return attempttrace.KitexFinishInfo(ri, err, abandoned, resultAdopted)
}

// traceFailureReason captures the real reason that drives a failure-driven
// retry, without re-invoking any ShouldResultRetry callback.
func traceFailureReason(err error) string {
	if err != nil {
		return "failure retry after error: " + err.Error()
	}
	return "failure retry after retryable business result"
}
