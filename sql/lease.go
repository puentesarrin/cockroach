// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

type versionInfo struct {
	desc       TableDescriptor
	expiration time.Time
	refcount   int
}

type tableInfo struct {
	versions []*versionInfo
}

type leaseManager struct {
	db     client.DB
	clock  *hlc.Clock
	mu     sync.Mutex
	tables map[ID]*tableInfo
}

func newLeaseManager(db client.DB) *leaseManager {
	return &leaseManager{
		db:     db,
		tables: make(map[ID]*tableInfo),
	}
}

func (m *leaseManager) acquire(tableID ID, version uint32) (*TableDescriptor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ti := m.tables[tableID]
	if ti == nil {
		ti = &tableInfo{}
		m.tables[tableID] = ti
	}
	if version == 0 && len(ti.versions) > 0 {
		v := ti.versions[len(ti.versions)-1]

	}
	for _, v := range ti.versions {
	}
	return nil, nil
}

func (m *leaseManager) acquireOp(tableID ID, version uint32) (*TableDescriptor, error) {
	key := MakeDescMetadataKey(tableID)
	desc := &TableDescriptor{}

	err := m.db.Txn(func(txn *client.Txn) error {
		if err := txn.GetProto(key, desc); err != nil {
			return err
		}
		if err := desc.Validate(); err != nil {
			return err
		}
		if version != 0 && desc.Version != version {
			return fmt.Errorf("unable to acquire lease on non-current version: %d vs %d",
				version, desc.Version)
		}
		b := &client.Batch{}

		// TODO(pmattis): Add lease

		return txn.CommitInBatch(b)
	})
	return desc, err
}

func (m *leaseManager) release(desc TableDescriptor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ti := m.tables[desc.ID]
	if ti == nil {
		return util.Errorf("table %d not found", desc.ID)
	}
	for _, v := range ti.versions {
		if desc.Version == v.desc.Version {
			return nil
		}
	}
	return util.Errorf("table %d version %d not found", desc.ID, desc.Version)
}

func (m *leaseManager) publish(tableID ID, update func(*TableDescriptor) error) error {
	key := MakeDescMetadataKey(tableID)

	// TODO(pmattis): Need to loop here retrying if the number of versions is
	// greater than 1.
	return m.db.Txn(func(txn *client.Txn) error {
		desc := &TableDescriptor{}
		if err := txn.GetProto(key, desc); err != nil {
			return err
		}
		desc.Version = desc.Version + 1
		desc.ModificationTime = m.clock.Now()

		// Scan for versions

		if err := update(desc); err != nil {
			return err
		}
		b := &client.Batch{}
		b.Put(key, desc)
		return txn.CommitInBatch(b)
	})
}
