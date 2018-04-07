// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
    "fmt"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"github.com/coreos/etcd/raftsnap"
    "github.com/tecbot/gorocksdb"
)

// a key-value store backed by raft
type KvStore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	//kvStore     map[string]string // current committed key-value pairs
    kvStore     *StoreObject
	snapshotter *raftsnap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func NewKVStore(dbPath string, snapshotter *raftsnap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *KvStore {
    db := NewStoreObject(dbPath, nil)
	//s := &kvstore{proposeC: proposeC, kvStore: make(map[string]string), snapshotter: snapshotter}
	s := &KvStore{proposeC: proposeC, kvStore: db, snapshotter: snapshotter}
	// replay log into key-value map
	s.ReadCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go s.ReadCommits(commitC, errorC)
    fmt.Println("return new kv store")
	return s
}

func (s *KvStore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	//v, ok := s.kvStore[key]
    v, err := s.kvStore.Get(key)

    var ok bool
    if err == nil {
        ok = true
    } else {
        ok = false
    }
	s.mu.RUnlock()
	return v, ok
}

func (s *KvStore) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

func (s *KvStore) ReadCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			_, err := s.snapshotter.Load()
			if err == raftsnap.ErrNoSnapshot {
				return
			}
			//if err != nil && err != raftsnap.ErrNoSnapshot {
			//	log.Panic(err)
			//}
			//log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			//if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			//	log.Panic(err)
			//}
			continue
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
        fmt.Println("before lock")
		s.mu.Lock()
        fmt.Println("end lock")
		//s.kvStore[dataKv.Key] = dataKv.Val
        s.kvStore.Put(dataKv.Key, dataKv.Val)
		s.mu.Unlock()
	}
	//if err, ok := <-errorC; ok {
    //    fmt.Println(err)
	//	log.Fatal(err)
	//}

    fmt.Println("return from readcommits")
}

func (s *KvStore) GetSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

    snapShot := s.kvStore.rocksdb.NewSnapshot()
    return json.Marshal(snapShot)

	//return json.Marshal(s.kvStore)
}

//func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
func (s *KvStore) RecoverFromSnapshot(snapshot gorocksdb.Snapshot) error {
	//var store map[string]string
	//if err := json.Unmarshal(snapshot, &store); err != nil {
	//	return err
	//}
	//s.mu.Lock()
	//s.kvStore = store
	//s.mu.Unlock()

    //var snapShot gorocksdb.Snapshot
    //if err := json.Unmarshal(snapshot, &snapShot); err != nil {
    //    return err
    //}

    //s.mu.Lock()
    //s.kvStore.SetSnapshot()
	return nil
}
