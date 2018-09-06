package habolt

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

const (
	retainSnapshotCount = 2
	raftStoreFileName   = "raft.db"
	raftTimeout         = 10 * time.Second
	raftUserEventName   = "us_hastore_apply"
)

type HaStore struct {
	mutex	   sync.Mutex
	
	store	   *Store

	BindAddr   string
	Address    string
	Port       int

	raftServer *raft.Raft
	serfServer *serf.Serf

	serfEvents chan serf.Event
}

func NewHaStore(addr, bindAddr string, port int, opts Options) (*HaStore, error) {
	db, err := NewStore(opts)
	if err != nil {
		return nil, err
	}
	obj := &HaStore{
		store: db,
		BindAddr: bindAddr,
		Address: addr,
		Port: port,
	}
	if err := obj.initSerf(); err != nil {
		return nil, err
	}
	if err := obj.initRaft(); err != nil {
		return nil, err
	}
	return obj, nil
}

func (has *HaStore) LogLevel(level int) {
	has.store.LogLevel(level)
}

func (has *HaStore) advAddr() string {
	if has.BindAddr != "" {
		return has.BindAddr
	}
	return has.Address
}

func (has *HaStore) SerfAddr(adv ...bool) string {
	addr := has.Address
	if len(adv) > 0 && adv[0] {
		addr = has.advAddr()
	}
	return fmt.Sprintf("%s:%d", addr, has.Port)
}

func (has *HaStore) RaftAddr(adv ...bool) string {
	addr := has.Address
	if len(adv) > 0 && adv[0] {
		addr = has.advAddr()
	}
	return fmt.Sprintf("%s:%d", addr, has.Port + 1)
}

func (has *HaStore) Start(peers ...string) error {
	if len(peers) > 0 {
		if _, err := has.serfServer.Join(peers, false); err != nil {
			return err
		}
	} else {
		if err := has.raftBootstrap(peers...); err != nil {
			return err
		}
	}
	
	for {
		select {
		case ev := <-has.serfEvents:
			leader := has.raftServer.VerifyLeader()
			if leader.Error() == nil {
				switch evt := ev.(type) {
				case serf.MemberEvent :		
					if err := has.serfMemberListener(evt); err != nil {
						return err
					}
				case serf.UserEvent :
					if evt.Name == raftUserEventName {
						fut := has.raftServer.Apply(evt.Payload, raftTimeout)
						if err := fut.Error(); err != nil {
							has.store.logger.Printf("[DEBUG] raft: Error apply > %v", err)
						}
					}
				}
			}
		}
	}
}

func (has *HaStore) Members(members *[]string) error {
	cFuture := has.raftServer.GetConfiguration()
	if err := cFuture.Error(); err != nil {
		return err
	}
	config := cFuture.Configuration()
	for _, server := range config.Servers {
		*members = append(*members, string(server.ID))
	}
	// for _, server := range has.serfServer.Memberlist().Members() {
	// 	*members = append(*members, server.Name )
	// }
	return nil
}

func (has *HaStore) List(values interface{}, patterns ...string) error {
	has.mutex.Lock()
	defer has.mutex.Unlock()
	return has.store.List(values, patterns...)
}

func (has *HaStore) Get(key string, value interface{}) error {
	has.mutex.Lock()
	defer has.mutex.Unlock()
	return has.store.Get(key, value)
}

func (has *HaStore) Set(key string, value interface{}) error {
	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
		Addr:  has.RaftAddr(),
	}
	msg, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return has.serfServer.UserEvent(raftUserEventName, msg, false)
}

func (has *HaStore) Delete(key string) error {
	c := &command{
		Op:    "del",
		Key:   key,
		Addr:  has.RaftAddr(),
	}
	msg, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return has.serfServer.UserEvent(raftUserEventName, msg, false)
}