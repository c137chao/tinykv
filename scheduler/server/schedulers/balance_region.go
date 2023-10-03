// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here
	allStores := cluster.GetStores()
	stores := make([]*core.StoreInfo, 0)

	// a suitable store should be up and the down time cannot be longer than MaxStoreDownTime
	maxDownTime := cluster.GetMaxStoreDownTime()
	for _, store := range allStores {
		if store.IsUp() && store.DownTime() <= maxDownTime {
			stores = append(stores, store)
		}
	}

	// if #stores less than 2, can't move
	if len(stores) < 2 {
		return nil
	}

	// sort stores by regionSize decrease
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})

	// select one region on some store, select store region size should be as much as possible
	fromIdx, moveRegion := getMoveFromStore(cluster, stores)
	if moveRegion == nil {
		return nil
	}

	// according test, if peer count of one region less than maxReplicas, don't remove
	// or try other region ??
	regionStores := moveRegion.GetStoreIds()
	if len(regionStores) < cluster.GetMaxReplicas() {
		return nil
	}

	// move to storeid should be one store which no any peer on it which in move from region
	toIdxOff := getMoveToStore(cluster, stores[fromIdx+1:], regionStores)
	if toIdxOff == -1 {
		return nil
	}

	fromStore := stores[fromIdx]
	toStore := stores[fromIdx+1+toIdxOff]

	log.Infof("source store %v, size: %v, target store %v, size: %v",
		fromStore.GetID(), fromStore.GetRegionSize(), toStore.GetID(), toStore.GetRegionSize())

	diffSize := fromStore.GetRegionSize() - toStore.GetRegionSize()

	if diffSize < 2*moveRegion.GetApproximateSize() {
		return nil
	}

	// create a peer on move to store, it will replace move from peer later
	newPeer, err := cluster.AllocPeer(toStore.GetID())
	if err != nil {
		return nil
	}

	if moveOp, err := operator.CreateMovePeerOperator(
		"balance-region-describe: balabala",
		cluster,
		moveRegion,
		operator.OpBalance,
		fromStore.GetID(),
		toStore.GetID(),
		newPeer.Id,
	); err == nil {
		return moveOp
	}

	return nil

}

// find the store meet the condition in lab doc which has max regionsize
// in one store, priority level: pending region > follower > leader
// if peers pn one store don't meet any condition above, ignore it and fine next one
func getMoveFromStore(cluster opt.Cluster, stores []*core.StoreInfo) (int, *core.RegionInfo) {
	var regionInfo *core.RegionInfo
	for i, store := range stores {
		// First it will try to select a pending region because pending may mean the disk is overloaded.
		cluster.GetPendingRegionsWithLock(store.GetID(), func(region core.RegionsContainer) {
			regionInfo = region.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			return i, regionInfo
		}

		// If there isn’t a pending region, it will try to find a follower region.
		cluster.GetFollowersWithLock(store.GetID(), func(region core.RegionsContainer) {
			regionInfo = region.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			return i, regionInfo
		}

		// If it still cannot pick out one region, it will try to pick leader regions.
		cluster.GetLeadersWithLock(store.GetID(), func(region core.RegionsContainer) {
			regionInfo = region.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			return i, regionInfo
		}

	}
	return -1, nil
}

func getMoveToStore(cluster opt.Cluster, stores []*core.StoreInfo, excludeStores map[uint64]struct{}) int {
	for i := len(stores) - 1; i >= 0; i-- {
		if _, ok := excludeStores[stores[i].GetID()]; !ok {
			return i
		}
	}
	return -1
}
