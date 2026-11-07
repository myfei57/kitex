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

package attempttrace

import (
	"context"
	"errors"
	"time"

	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/rpcinfo/remoteinfo"
	"github.com/cloudwego/kitex/pkg/stats"
)

// KitexFinishInfo builds the plain-value terminal description of an attempt
// from a REAL rpcCall result. It never re-invokes a selector or a
// result-retry callback, never resends an RPC, never retains pooled
// RPCInfo/EndpointInfo/Instance pointers and never touches request or
// response business values.
func KitexFinishInfo(ri rpcinfo.RPCInfo, err error, abandoned, resultAdopted bool) FinishInfo {
	info := FinishInfo{
		Status:        ClassifyError(err),
		Abandoned:     abandoned,
		ResultAdopted: resultAdopted,
		Instance:      SnapshotInstance(ri),
	}
	if err != nil {
		info.ErrMsg = err.Error()
	}
	if ri != nil && ri.Stats() != nil {
		info.ConnStart = eventTime(ri, stats.ClientConnStart)
		info.ConnFinish = eventTime(ri, stats.ClientConnFinish)
		info.WriteStart = eventTime(ri, stats.WriteStart)
		info.ReadStart = eventTime(ri, stats.ReadStart)
	}
	return info
}

// SnapshotInstance copies the selected instance into owned value data.
// Returns nil when no instance was selected (e.g. discovery/loadbalance
// failure), which the trace renders as InstanceSelected=false.
func SnapshotInstance(ri rpcinfo.RPCInfo) *InstanceInfo {
	if ri == nil || ri.To() == nil {
		return nil
	}
	re := remoteinfo.AsRemoteInfo(ri.To())
	if re == nil {
		if addr := ri.To().Address(); addr != nil {
			return &InstanceInfo{Network: addr.Network(), Address: addr.String()}
		}
		return nil
	}
	ins := re.GetInstance()
	if ins == nil {
		if addr := re.Address(); addr != nil {
			return &InstanceInfo{Network: addr.Network(), Address: addr.String()}
		}
		return nil
	}
	snap := &InstanceInfo{Weight: ins.Weight()}
	if addr := ins.Address(); addr != nil {
		snap.Network = addr.Network()
		snap.Address = addr.String()
	}
	return snap
}

// ClassifyError maps a real attempt error to the trace completion class.
func ClassifyError(err error) Status {
	switch {
	case err == nil:
		return StatusSucceeded
	case kerrors.IsTimeoutError(err):
		return StatusTimedOut
	case errors.Is(err, context.Canceled), errors.Is(err, kerrors.ErrCanceledByBusiness):
		return StatusCanceled
	case errors.Is(err, kerrors.ErrNoMoreInstance),
		errors.Is(err, kerrors.ErrNoInstance),
		errors.Is(err, kerrors.ErrLoadbalance),
		errors.Is(err, kerrors.ErrServiceDiscovery),
		errors.Is(err, kerrors.ErrRoute):
		return StatusNoInstance
	default:
		return StatusFailed
	}
}

func eventTime(ri rpcinfo.RPCInfo, ev stats.Event) time.Time {
	e := ri.Stats().GetEvent(ev)
	if e == nil || e.IsNil() {
		return time.Time{}
	}
	return e.Time()
}
