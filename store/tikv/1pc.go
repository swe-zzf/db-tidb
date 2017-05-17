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

package tikv

import (
	"sync/atomic"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	goctx "golang.org/x/net/context"
	"fmt"
)

// twoPhaseCommitter executes a two-phase commit protocol.
type onePhaseCommitter struct {
	store     *tikvStore
	txn       *tikvTxn
	commitTS   uint64
	keys      [][]byte
	mutations map[string]*pb.Mutation
}

// newOnePhaseCommitter creates a oneTwoPhaseCommitter.
func newOnePhaseCommitter(txn *tikvTxn) (*onePhaseCommitter, error) {
	var (
		keys    [][]byte
		size    int
	)
	mutations := make(map[string]*pb.Mutation)
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		if len(v) == 0 {
			err := fmt.Errorf("Delete operation should do with 2PC,key:%+v",k)
			log.Error("[1PC]:",err)
			return err
		}

		mutations[string(k)] = &pb.Mutation{
			Op:    pb.Op_Put,
			Key:   k,
			Value: v,
		}
		keys = append(keys, k)
		entrySize := len(k) + len(v)
		if entrySize > kv.TxnEntrySizeLimit {
			return kv.ErrEntryTooLarge
		}
		size += entrySize
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Transactions without Put
	if len(keys) == 0 {
		return nil, nil
	}

	entrylimit := atomic.LoadUint64(&kv.TxnEntryCountLimit)
	if len(keys) > int(entrylimit) || size > kv.TxnTotalSizeLimit {
		return nil, kv.ErrTxnTooLarge
	}
	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if len(keys) > logEntryCount || size > logSize {
		tableID := tablecodec.DecodeTableID(keys[0])
		log.Infof("[BIG_TXN] 1pc with table id:%d size:%d, keys:%d, commitTS:%d",
			tableID, size, len(keys), txn.startTS)
	}

	txnWriteKVCountHistogram.Observe(float64(len(keys)))
	txnWriteSizeHistogram.Observe(float64(size / 1024))

	return &onePhaseCommitter{
		store:     txn.store,
		txn:       txn,
		commitTS:   txn.StartTS(),
		keys:      keys,
		mutations: mutations,
	}, nil
}

func (c *onePhaseCommitter) execute() error {
	ctx := goctx.Background()
	bo := NewBackoffer(importMaxBackoff,ctx)
	return c.commit_keys(bo,c.keys)
}

func (c onePhaseCommitter)commit_keys(bo *Backoffer, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	groups, _, err := c.store.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		return errors.Trace(err)
	}

	var sizeFunc = func(key []byte) int {
		return len(key)
	}
	var batches []batchKeys
	for id, g := range groups {
		batches = appendBatchBySize(batches, id, g, sizeFunc, txnCommitBatchSize)
	}

	err = c.doCommitOnBatches(bo, batches)
	if err == nil {
		c.txn.commitTS = c.commitTS
	}
	return errors.Trace(err)
}

func (c *onePhaseCommitter) importSingleBatch(bo *Backoffer, batch batchKeys) error {
	mutations := make([]*pb.Mutation, len(batch.keys))
	for i, k := range batch.keys {
		mutations[i] = c.mutations[string(k)]
	}

	req := &pb.Request{
		Type: pb.MessageType_CmdImport,
		CmdImportReq: &pb.CmdImportRequest{
			Mutations:           mutations,
			CommitVersion:       c.commitTS,
		},
	}

	resp, err := c.store.SendKVReq(bo, req, batch.region, writeTimeout)
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		err = c.commit_keys(bo, batch.keys)
		return errors.Trace(err)
	}
	respDetail := resp.GetCmdImportResp()
	if respDetail == nil {
		return errors.Trace(errBodyMissing)
	}
	errInfo := respDetail.GetError()
	if errInfo == "" {
		return nil
	}
	return errors.Trace(fmt.Errorf("[1PC] failed with %v",errInfo))

}


func (c onePhaseCommitter)doCommitOnBatches(bo *Backoffer, batches []batchKeys) error {
	backoffer, cancel := bo.Fork()
	// Concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	for _, batch := range batches {
		go func(batch batchKeys) {
			singleBatchBackoffer, singleBatchCancel := backoffer.Fork()
			defer singleBatchCancel()
			ch <- c.importSingleBatch(singleBatchBackoffer, batch)
		}(batch)
	}
	var err error
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			log.Debugf("[1PC] importSingleBatch failed: %v, tid: %d", e, c.commitTS)
			// Cancel other requests and return the first error.
			if cancel != nil {
				cancel()
			}
			if err == nil {
				err = e
			}
		}
	}
	return errors.Trace(err)
}
